use zenoh_proto::{
    TransportLinkError,
    msgs::{Close, CloseBehaviour, NetworkMessage, NetworkMessageRef, TransportMessageRef},
};
use zenoh_sansio::{ZTransportRx, ZTransportTx};

// zenoh-pico close reasons(`definitions/transport.h`:_Z_CLOSE_GENERIC=0 … _Z_CLOSE_EXPIRED=5)
const Z_CLOSE_GENERIC: u8 = 0x00;
const Z_CLOSE_EXPIRED: u8 = 0x05;

use super::{ZLinkInfo, ZLinkRx, ZLinkTx};

pub trait ZTransportLinkTx {
    fn tx(&mut self) -> (&mut impl ZLinkTx, &mut impl ZTransportTx);

    fn send<'a>(
        &mut self,
        msgs: impl Iterator<Item = NetworkMessage<'a>>,
    ) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        transport.encode(msgs);

        async move {
            if let Some(bytes) = transport.flush(link.is_streamed()) {
                link.write_all(bytes).await.map_err(|e| e.into())
            } else {
                Ok(())
            }
        }
    }

    #[allow(dead_code)]
    fn send_optimized_ref<'a>(
        &mut self,
        msgs: impl Iterator<Item = (NetworkMessageRef<'a>, &'a [u8])>,
    ) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        transport.encode_optimized_ref(msgs);

        async move {
            if let Some(bytes) = transport.flush(link.is_streamed()) {
                link.write_all(bytes).await.map_err(|e| e.into())
            } else {
                Ok(())
            }
        }
    }

    fn keepalive(
        &mut self,
    ) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        transport.keepalive();

        async move {
            if let Some(bytes) = transport.flush(link.is_streamed()) {
                link.write_all(bytes).await.map_err(|e| e.into())
            } else {
                Ok(())
            }
        }
    }

    // P5/asrun:優雅關閉 — 顯式構造 `Close{behaviour=Session}`(S=1,byte `0x23`)。
    // zenoh-pico `_z_t_msg_make_close(reason, link_only=false)` 一律 session close
    // (transport/unicast/transport.c:324;lease 逾時走 _Z_CLOSE_EXPIRED)。
    // upstream sansio `tx.close()` 發 `Close::default()`=Link(S=0)→ 位元級不對齊,
    // 故在此顯式構造,不觸碰 upstream sansio。reason=Generic(0x00)。
    fn close(&mut self) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        transport.transport_ref(TransportMessageRef::Close(&Close {
            reason: Z_CLOSE_GENERIC,
            behaviour: CloseBehaviour::Session,
        }));

        async move {
            if let Some(bytes) = transport.flush(link.is_streamed()) {
                link.write_all(bytes).await.map_err(|e| e.into())
            } else {
                Ok(())
            }
        }
    }

    // P5/asrun:租約逾時關閉 — Close{reason=Expired, behaviour=Session}(byte `0x23 0x05`),
    // 對齊 pico lease.c `_z_unicast_transport_close(ztu, _Z_CLOSE_EXPIRED)`。
    fn close_expired(
        &mut self,
    ) -> impl Future<Output = core::result::Result<(), zenoh_proto::TransportLinkError>> {
        let (link, transport) = self.tx();
        transport.transport_ref(TransportMessageRef::Close(&Close {
            reason: Z_CLOSE_EXPIRED,
            behaviour: CloseBehaviour::Session,
        }));

        async move {
            if let Some(bytes) = transport.flush(link.is_streamed()) {
                link.write_all(bytes).await.map_err(|e| e.into())
            } else {
                Ok(())
            }
        }
    }
}

pub trait ZTransportLinkRx {
    fn rx(&mut self) -> (&mut impl ZLinkRx, &mut impl ZTransportRx);

    fn recv(
        &mut self,
    ) -> impl core::future::Future<
        Output = core::result::Result<
            impl Iterator<Item = (NetworkMessage<'_>, &'_ [u8])>,
            zenoh_proto::TransportLinkError,
        >,
    > {
        let (link, transport) = self.rx();
        let streamed = link.is_streamed();

        async move {
            transport
                .decode_with_async(
                    async |bytes| {
                        if streamed {
                            link.read_exact(bytes).await.map(|_| bytes.len())
                        } else {
                            link.read(bytes).await
                        }
                    },
                    streamed,
                )
                .await
                .map_err(|e| e.flatten_map::<TransportLinkError>())?;

            Ok(transport.flush())
        }
    }
}
