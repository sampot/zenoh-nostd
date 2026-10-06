// asrun P5: session 優雅關閉(原 upstream `TODO: send Close msg` 落地)。
//
// `close()` 向對端發 `Close(behaviour=Session)`;對端 run() 經 sansio rx
// 翻轉 closed/graceful → `Ok(()))` 收口。本端後續 I/O 依 sansio tx
// state(Closed)自然拒絕(呼叫端視為 session 終止,上層重開)。

use zenoh_proto::SessionError;

use crate::{api::session::Session, config::ZSessionConfig, io::transport::ZTransportLinkTx};

impl<'res, Config> Session<'res, Config>
where
    Config: ZSessionConfig,
{
    /// 優雅關閉:發送 `Close` 並等待寫入完成。不回收 `'res` 資源
    /// (nostd 靜態模型:整槽 reuse 由呼叫端控制)。
    pub async fn close(&self) -> core::result::Result<(), SessionError> {
        self.driver.tx().await.close().await.map_err(Into::into)
    }
}
