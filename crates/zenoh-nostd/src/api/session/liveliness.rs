// asrun P3: liveliness token / subscriber API(WISP 最小集)。
//
// 線格式對齊 zenoh-pico 1.x(`protocol/codec/declarations.h`):
// `_Z_DECL_TOKEN_MID=6` / `_Z_UNDECL_TOKEN_MID=7`,本端 liveliness 訂閱以一般
// `DeclareSubscriber` 上線(對等端無需感知),token 事件在接收端由 run() 合成
// `LivelinessEvent` 交付獨立回呼表。
//
// 限制(no_alloc 務實):本 crate 不依 id 追蹤 token 狀態,故 `undeclare` 必須
// 攜 wire_expr(本實作一律附上);對等端缺 ke 時跳過離線事件。

use dyn_utils::DynObject;
use zenoh_proto::{exts::QoS, fields::*, msgs::*, *};

#[cfg(feature = "alloc")]
use crate::api::callbacks::AllocCallbacks;

use crate::{
    api::{
        arg::LivelinessArg,
        callbacks::{AsyncCallback, DynCallback, FixedCapacityCallbacks, SyncCallback, ZCallbacks},
        liveliness::LivelinessEvent,
        session::Session,
    },
    config::ZSessionConfig,
    io::transport::ZTransportLinkTx,
};

pub type FixedCapacityLivCallbacks<
    'a,
    const CAPACITY: usize,
    Callback = dyn_utils::storage::RawOrBox<16>,
    Future = dyn_utils::storage::RawOrBox<128>,
> = FixedCapacityCallbacks<'a, LivelinessArg, CAPACITY, Callback, Future>;

#[cfg(feature = "alloc")]
pub type AllocLivCallbacks<
    'a,
    Callback = dyn_utils::storage::RawOrBox<16>,
    Future = dyn_utils::storage::RawOrBox<128>,
> = AllocCallbacks<'a, LivelinessArg, Callback, Future>;

type LivCallbackStorage<'res, Config> =
    <<Config as ZSessionConfig>::LivelinessCallbacks<'res> as ZCallbacks<'res, LivelinessArg>>::Callback;

type LivFutureStorage<'res, Config> =
    <<Config as ZSessionConfig>::LivelinessCallbacks<'res> as ZCallbacks<'res, LivelinessArg>>::Future;

/// 已宣告的 liveliness token;`undeclare` 撤回(發送 `UndeclareToken`)。
pub struct LivelinessToken<'a, 'res, Config>
where
    Config: ZSessionConfig,
{
    id: u32,
    ke: &'static keyexpr,
    session: &'a Session<'res, Config>,
}

impl<'a, 'res, Config> LivelinessToken<'a, 'res, Config>
where
    Config: ZSessionConfig,
{
    pub fn keyexpr(&self) -> &keyexpr {
        self.ke
    }

    /// 撤回 token:發送 `UndeclareToken`(携 wire_expr,讓對等端能合成離線事件)。
    pub async fn undeclare(self) -> core::result::Result<(), SessionError> {
        let msg = Declare {
            body: DeclareBody::UndeclareToken(UndeclareToken {
                id: self.id,
                wire_expr: Some(WireExpr::from(self.ke)),
            }),
            ..Default::default()
        };

        self.session
            .driver
            .tx()
            .await
            .send(core::iter::once(NetworkMessage {
                reliability: Reliability::default(),
                qos: QoS::default(),
                body: NetworkBody::Declare(msg),
            }))
            .await?;

        Ok(())
    }
}

/// liveliness 訂閱者:接收配對 ke 的 token online/offline 事件。
pub struct LivelinessSubscriber<'a, 'res, Config>
where
    Config: ZSessionConfig,
{
    id: u32,
    ke: &'static keyexpr,
    session: &'a Session<'res, Config>,
}

impl<'a, 'res, Config> LivelinessSubscriber<'a, 'res, Config>
where
    Config: ZSessionConfig,
{
    /// 退訂:回呼表移除 + 發送 `UndeclareSubscriber`(與一般 sub 同線式)。
    pub async fn undeclare(self) -> core::result::Result<(), SessionError> {
        let msg = Declare {
            body: DeclareBody::UndeclareSubscriber(UndeclareSubscriber {
                id: self.id,
                ..Default::default()
            }),
            ..Default::default()
        };

        self.session
            .state()
            .await
            .liveliness_callbacks
            .remove(self.id)?;

        self.session
            .driver
            .tx()
            .await
            .send(core::iter::once(NetworkMessage {
                reliability: Reliability::default(),
                qos: QoS::default(),
                body: NetworkBody::Declare(msg),
            }))
            .await?;

        Ok(())
    }

    pub fn keyexpr(&self) -> &keyexpr {
        self.ke
    }
}

pub struct LivelinessSubscriberBuilder<'a, 'res, Config>
where
    Config: ZSessionConfig,
{
    session: &'a Session<'res, Config>,
    ke: &'static keyexpr,
    callback: Option<
        DynCallback<
            'res,
            LivCallbackStorage<'res, Config>,
            LivFutureStorage<'res, Config>,
            LivelinessArg,
        >,
    >,
}

impl<'a, 'res, Config> LivelinessSubscriberBuilder<'a, 'res, Config>
where
    Config: ZSessionConfig,
{
    pub(crate) fn new(session: &'a Session<'res, Config>, ke: &'static keyexpr) -> Self {
        Self {
            session,
            ke,
            callback: None,
        }
    }

    pub fn callback(mut self, callback: impl AsyncFnMut(&LivelinessEvent<'_>) + 'res) -> Self {
        self.callback = Some(DynObject::new(AsyncCallback::new(callback)));
        self
    }

    pub fn callback_sync(mut self, callback: impl FnMut(&LivelinessEvent<'_>) + 'res) -> Self {
        self.callback = Some(DynObject::new(SyncCallback::new(callback)));
        self
    }

    pub async fn finish(self) -> core::result::Result<LivelinessSubscriber<'a, 'res, Config>, SessionError> {
        let id = {
            let mut state = self.session.state().await;
            let id = state.next();
            if let Some(callback) = self.callback {
                state.liveliness_callbacks.insert(id, self.ke, None, callback)?;
            }
            id
        };

        let msg = Declare {
            body: DeclareBody::DeclareSubscriber(DeclareSubscriber {
                id,
                wire_expr: WireExpr::from(self.ke),
            }),
            ..Default::default()
        };

        self.session
            .driver
            .tx()
            .await
            .send(core::iter::once(NetworkMessage {
                reliability: Reliability::default(),
                qos: QoS::default(),
                body: NetworkBody::Declare(msg),
            }))
            .await?;

        Ok(LivelinessSubscriber {
            ke: self.ke,
            id,
            session: self.session,
        })
    }
}

impl<'res, Config> Session<'res, Config>
where
    Config: ZSessionConfig,
{
    /// 宣告 liveliness token(立即發送 `DeclareToken`;撤回首見 [`LivelinessToken::undeclare`])。
    pub async fn declare_liveliness_token<'a>(
        &'a self,
        ke: &'static keyexpr,
    ) -> core::result::Result<LivelinessToken<'a, 'res, Config>, SessionError> {
        let id = self.state().await.next();

        let msg = Declare {
            body: DeclareBody::DeclareToken(DeclareToken {
                id,
                wire_expr: WireExpr::from(ke),
            }),
            ..Default::default()
        };

        self.driver
            .tx()
            .await
            .send(core::iter::once(NetworkMessage {
                reliability: Reliability::default(),
                qos: QoS::default(),
                body: NetworkBody::Declare(msg),
            }))
            .await?;

        Ok(LivelinessToken { id, ke, session: self })
    }

    /// liveliness 訂閱 builder(事件交付獨立回呼表,不混入 data sub)。
    pub fn declare_liveliness_subscriber<'a>(
        &'a self,
        ke: &'static keyexpr,
    ) -> LivelinessSubscriberBuilder<'a, 'res, Config> {
        LivelinessSubscriberBuilder::new(self, ke)
    }
}
