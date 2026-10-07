use zenoh_proto::{exts::Value, fields::Reliability, msgs::*, *};

use crate::{
    api::{
        callbacks::{ZCallbacks, ZDynCallback},
        liveliness::LivelinessEvent,
        query::QueryableQuery,
        scopes::MAX_KE,
        session::Session,
    },
    config::ZSessionConfig,
    io::transport::ZTransportLinkTx,
    session::{GetResponse, Sample},
};

impl<'res, Config> Session<'res, Config>
where
    Config: ZSessionConfig,
{
    pub async fn run(&self) -> core::result::Result<(), SessionError> {
        self.driver
            .run(&self.state, async |_, state, msg, _| {
                match msg.body {
                    NetworkBody::Push(Push {
                        wire_expr,
                        payload: PushBody::Put(Put { payload, .. }),
                        ..
                    }) => {
                        let ke = wire_expr.suffix;
                        let ke = keyexpr::new(ke)?;
                        let sample = Sample::new(ke, payload);

                        for cb in state.sub_callbacks.intersects(ke) {
                            cb.call(&sample).await;
                        }
                    }
                    NetworkBody::Response(Response {
                        rid,
                        wire_expr,
                        payload,
                        ..
                    }) => {
                        let ke = wire_expr.suffix;
                        let ke = keyexpr::new(ke)?;
                        let response = match payload {
                            ResponseBody::Reply(Reply {
                                payload: PushBody::Put(Put { payload, .. }),
                                ..
                            }) => GetResponse::Ok(Sample::new(ke, payload)),
                            ResponseBody::Err(Err { payload, .. }) => {
                                GetResponse::Err(Sample::new(ke, payload))
                            }
                        };

                        if let Some(cb) = state.get_callbacks.get(rid) {
                            cb.call(&response).await;
                        }
                    }
                    NetworkBody::ResponseFinal(ResponseFinal { rid, .. }) => {
                        state.get_callbacks.remove(rid)?;
                        // TODO: also close channels
                    }
                    NetworkBody::Request(Request {
                        id,
                        wire_expr,
                        payload:
                            RequestBody::Query(Query {
                                parameters, body, ..
                            }),
                        ..
                    }) => {
                        let ke = wire_expr.suffix;
                        let ke = keyexpr::new(ke)?;
                        let query = QueryableQuery::new(
                            self,
                            id,
                            ke,
                            if parameters.is_empty() {
                                None
                            } else {
                                Some(parameters)
                            },
                            match body {
                                Some(Value { payload, .. }) => Some(payload),
                                None => None,
                            },
                        );

                        let count = state.queryable_callbacks.intersects(ke).count();
                        if count == 0 {
                            // P1/#23 配套:本 session 無 queryable 命中此 Request → 立即 ResponseFinal,
                            // 不佔 counter slot、不留請求方乾等 timeout(rid 為本 session 範圍,語意正確)。
                            self.driver
                                .tx()
                                .await
                                .send(core::iter::once(NetworkMessage {
                                    reliability: Reliability::default(),
                                    qos: exts::QoS::default(),
                                    body: NetworkBody::ResponseFinal(ResponseFinal {
                                        rid: id,
                                        ..Default::default()
                                    }),
                                }))
                                .await?;
                        } else {
                            state.queryable_callbacks.set_counter(id, count)?;
                            for cb in state.queryable_callbacks.intersects(ke) {
                                cb.call(&query).await;
                            }
                            // P1b:auto-finalize — callback 為 inline 派發(&query 不可 mut,
                            // upstream 無處觸發 finalize → 請求方只能乾等 timeout)。
                            // asrun profile 不支持 channel queryable 之「延後回覆」(WIT 同步語意),
                            // dispatch 全部完成即按命中數強制遞減收斂,歸零發 ResponseFinal。
                            let mut finalized = false;
                            for _ in 0..count {
                                finalized |= state.queryable_callbacks.decrease(id);
                            }
                            if finalized {
                                self.driver
                                    .tx()
                                    .await
                                    .send(core::iter::once(NetworkMessage {
                                        reliability: Reliability::default(),
                                        qos: exts::QoS::default(),
                                        body: NetworkBody::ResponseFinal(ResponseFinal {
                                            rid: id,
                                            ..Default::default()
                                        }),
                                    }))
                                    .await?;
                            }
                        }
                    }
                    // asrun P3 liveliness:token 宣告/撤回 → 合成 online/offline 事件交付
                    // liveliness subscribers。
                    // asrun P4:對齊 zenoh(rust) router 轉發形態——先 `DeclareKeyExpr{id,ke}`
                    // 註冊資源、再 `DeclareToken{id,scope:id,suffix:""}` 引用(同 id);
                    // 直接 keyexpr::new("") 會炸 EmptyChunk → 須經 scopes 表解析。
                    NetworkBody::Declare(Declare { body, .. }) => match body {
                        DeclareBody::DeclareKeyExpr(DeclareKeyExpr { id, wire_expr }) => {
                            if !wire_expr.suffix.is_empty() {
                                state.scopes.insert(id as u32, wire_expr.suffix);
                            }
                        }
                        DeclareBody::DeclareToken(DeclareToken { id, wire_expr }) => {
                            // inline token:先記 id 供後續 UndeclareToken 引用
                            // (置於 resolve 前:避免持 state.scopes 共享借用再变更)
                            if wire_expr.scope == 0 && !wire_expr.suffix.is_empty() {
                                state.scopes.insert(id, wire_expr.suffix);
                            }
                            let mut join = heapless::String::<MAX_KE>::new();
                            if let Some(ke_str) = state
                                .scopes
                                .resolve(wire_expr.scope as u32, wire_expr.suffix, &mut join)
                            {
                                if let Ok(ke) = keyexpr::new(ke_str) {
                                    let event = LivelinessEvent::new(ke, true);
                                    for cb in state.liveliness_callbacks.intersects(ke) {
                                        cb.call(&event).await;
                                    }
                                }
                            }
                        }
                        DeclareBody::UndeclareToken(UndeclareToken { id, wire_expr }) => {
                            let mut join = heapless::String::<MAX_KE>::new();
                            {
                                // router 轉發形態(zenohd 1.10.1 trace 實錘):
                                // `UndeclareToken{id, ext_wire_expr={scope:0,suffix:""}}`
                                // —— ext 在但內容空,ke 須由 id 經 scopes 表還原
                                // (id 於 DeclareKeyExpr/inline token 註冊時已入表)。
                                let ke_str: Option<&str> = wire_expr
                                    .as_ref()
                                    .and_then(|we| {
                                        state.scopes.resolve(we.scope as u32, we.suffix, &mut join)
                                    })
                                    .filter(|s| !s.is_empty())
                                    .or_else(|| state.scopes.get(id));
                                if let Some(ke_str) = ke_str {
                                    if let Ok(ke) = keyexpr::new(ke_str) {
                                        let event = LivelinessEvent::new(ke, false);
                                        for cb in state.liveliness_callbacks.intersects(ke) {
                                            cb.call(&event).await;
                                        }
                                    }
                                }
                            }
                            state.scopes.remove(id);
                        }
                        _ => {}
                    },
                    _ => {}
                }

                Ok::<(), SessionError>(())
            })
            .await
            .map_err(|e| e.flatten_map())
    }
}
