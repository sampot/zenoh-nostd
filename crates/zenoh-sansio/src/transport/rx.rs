use core::fmt::Display;
use core::time::Duration;

use zenoh_proto::{
    EitherError, TransportError, ZBodyDecode, ZReadable, fields::Resolution, msgs::*,
};

use crate::{ZTransportRx, transport::TransportTx};

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum State {
    Opened,
    Used,
    Synchronized { last_received: Duration },
    Closed,
}

#[derive(Debug)]
pub struct TransportRx<Buff> {
    buff: Buff,

    cursor: usize,
    batch_size: usize,

    sn: u32,
    resolution: Resolution,
    lease: Duration,

    state: State,

    ignore_invalid_sn: bool,

    // P5/asrun:標記「收到對端 Close 幀」(graceful shutdown);Cell 便於惰性 flush 中記錄
    close_received: core::cell::Cell<bool>,
}

impl<Buff> TransportRx<Buff> {
    pub(crate) fn new(
        buff: Buff,

        batch_size: usize,
        sn: u32,
        resolution: Resolution,
        lease: Duration,
    ) -> Self {
        Self {
            buff,

            cursor: 0,
            batch_size,

            sn,
            resolution,
            lease,

            state: State::Opened,
            ignore_invalid_sn: false,
            close_received: core::cell::Cell::new(false),
        }
    }

    /// P5/asrun:本端是否在 flush 中收到過對端 Close 幀(graceful shutdown)。
    pub fn close_received(&self) -> bool {
        self.close_received.get()
    }

    pub(crate) fn into_inner(self) -> Buff {
        self.buff
    }

    pub(crate) fn flush_transport(&mut self) -> impl Iterator<Item = (TransportMessage<'_>, &[u8])>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        let size = core::cmp::min(
            self.buff.as_ref().len(),
            core::cmp::min(self.batch_size, self.cursor),
        );
        self.cursor = 0;
        let mut reader = &self.buff.as_ref()[..size];
        let mut last_frame = None;
        let sn = &mut self.sn;
        let resolution = self.resolution;
        let ignore = self.ignore_invalid_sn;

        core::iter::from_fn(move || {
            Self::decode(&mut reader, &mut last_frame, sn, resolution, ignore)
        })
        .filter_map(|m| match m.0 {
            Message::Transport(msg) => Some((msg, m.1)),
            _ => None,
        })
    }

    pub fn ignore_invalid_sn(&mut self) {
        self.ignore_invalid_sn = true;
    }

    pub fn sync(&mut self, tx: Option<&TransportTx<Buff>>, now: Duration) {
        if let Some(tx) = tx
            && tx.closed()
        {
            self.state = State::Closed;
            return;
        }

        if self.should_close(now) {
            self.state = State::Closed;
        }

        if matches!(self.state, State::Used | State::Opened) {
            self.state = State::Synchronized { last_received: now };
        };
    }

    pub fn next_timeout(&self) -> Duration {
        match self.state {
            State::Opened | State::Closed | State::Used => Duration::from_secs(0),
            State::Synchronized { last_received } => last_received + self.lease,
        }
    }

    pub fn should_close(&self, now: Duration) -> bool {
        match self.state {
            State::Opened | State::Closed | State::Used => false,
            State::Synchronized { last_received } => now > last_received + self.lease,
        }
    }

    pub fn closed(&self) -> bool {
        matches!(self.state, State::Closed)
    }

    pub(crate) fn decode<'a>(
        reader: &mut &'a [u8],
        last_frame: &mut Option<FrameHeader>,
        sn: &mut u32,
        resolution: Resolution,
        ignore: bool,
    ) -> Option<(Message<'a>, &'a [u8])>
    where
        Buff: AsRef<[u8]>,
    {
        if !reader.can_read() {
            return None;
        }

        let (data, start) = (reader.as_ptr(), reader.len());

        let header = reader
            .read_u8()
            .expect("reader should not be empty at this stage");

        macro_rules! decode {
            ($ty:ty) => {
                match <$ty as ZBodyDecode>::z_body_decode(reader, header) {
                    Ok(msg) => msg,
                    Err(e) => {
                        zenoh_proto::error!(
                            "Failed to decode message of type {}: {}. Skipping the rest of the message - {}",
                            core::any::type_name::<$ty>(),
                            e,
                            zenoh_proto::zctx!()
                        );

                        return None;
                    }
                }
            };

            (@Transport $ty:ident) => {{
                last_frame.take();
                Message::Transport(TransportMessage:: $ty (decode!($ty)))
            }};

            (@Network $ty:ident) => {
                Message::Network(NetworkMessage {
                    reliability: last_frame.as_ref().expect("Should be in frame").reliability,
                    qos: last_frame.as_ref().expect("Should be in frame").qos,
                    body: NetworkBody:: $ty (decode!($ty))
                })
            };
        }

        let ack = header & 0b0010_0000 != 0;
        let net = last_frame.is_some();
        let ifinal = header & 0b0110_0000 == 0;
        let id = header & 0b0001_1111;

        let body = match id {
            FrameHeader::ID => {
                let header = decode!(FrameHeader);

                *sn = sn.wrapping_add(1);

                if !ignore {
                    // Check for missed messages regarding resolution
                    let _ = resolution;

                    if header.sn < *sn - 1 {
                        zenoh_proto::error!(
                            "Inconsistent `SN` value {}, expected higher than {}",
                            header.sn,
                            *sn - 1
                        );
                        return None;
                    } else if header.sn != *sn - 1 {
                        zenoh_proto::debug!("Transport missed {} messages", header.sn - *sn + 1);
                    }
                }

                last_frame.replace(header);

                return Self::decode(reader, last_frame, sn, resolution, ignore);
            }
            InitAck::ID if ack => decode!(@Transport InitAck),
            InitSyn::ID => decode!(@Transport InitSyn),
            OpenAck::ID if ack => decode!(@Transport OpenAck),
            OpenSyn::ID => decode!(@Transport OpenSyn),
            Close::ID => decode!(@Transport Close),
            KeepAlive::ID => decode!(@Transport KeepAlive),
            Push::ID if net => decode!(@Network Push),
            Request::ID if net => decode!(@Network Request),
            Response::ID if net => decode!(@Network Response),
            ResponseFinal::ID if net => decode!(@Network ResponseFinal),
            InterestFinal::ID if net && ifinal => decode!(@Network InterestFinal),
            Interest::ID if net => decode!(@Network Interest),
            Declare::ID if net => decode!(@Network Declare),
            _ => {
                zenoh_proto::error!(
                    "Unrecognized message header: {:08b}. Skipping the rest of the message - {}",
                    header,
                    zenoh_proto::zctx!()
                );
                return None;
            }
        };

        let len = start - reader.len();
        Some((body, unsafe { core::slice::from_raw_parts(data, len) }))
    }
}

impl<Buff> ZTransportRx for TransportRx<Buff>
where
    Buff: AsMut<[u8]> + AsRef<[u8]>,
{
    fn decode_prefixed(&mut self, mut read: &[u8]) -> core::result::Result<(), TransportError>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        if read.is_empty() || self.state == State::Closed {
            return Ok(());
        }

        self.decode_prefixed_with(|data| {
            let size = data.len().min(read.len());
            let (ret, remain) = read.split_at(size);
            data[..size].copy_from_slice(ret);
            read = remain;
            Ok::<_, TransportError>(size)
        })
        .map_err(|e| e.flatten())
    }

    fn decode_raw(&mut self, read: &[u8]) -> core::result::Result<(), TransportError>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        if read.is_empty() || self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        let len = read.len();
        if buff.len() < read.len() {
            zenoh_proto::zbail!(@log TransportError::TransportRxFull);
        }

        buff[..len].copy_from_slice(read);

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    fn decode_prefixed_with<E>(
        &mut self,
        mut read: impl FnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        E: Display,
    {
        if self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        if 2 > buff.len() {
            zenoh_proto::zbail!(@log TransportError::TransportRxFull);
        }

        let mut len = [0u8; 2];
        let l = read(&mut len).map_err(EitherError::B)?;

        if l == 0 {
            return Ok(());
        } else if l != 2 {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute)
        }

        let len = u16::from_le_bytes(len) as usize;
        if len > u16::MAX as usize || len > buff.len() {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute);
        }

        if read(&mut buff[..len]).map_err(EitherError::B)? != len {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute)
        }

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    fn decode_raw_with<E>(
        &mut self,
        mut read: impl FnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        E: Display,
    {
        if self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        let len = read(buff).map_err(EitherError::B)?;

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    async fn decode_prefixed_with_async<E>(
        &mut self,
        mut read: impl AsyncFnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        E: Display,
    {
        if self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        if 2 > buff.len() {
            zenoh_proto::zbail!(@log TransportError::TransportRxFull);
        }

        let mut len = [0u8; 2];
        let l = read(&mut len).await.map_err(EitherError::B)?;

        if l == 0 {
            return Ok(());
        } else if l != 2 {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute)
        }

        let len = u16::from_le_bytes(len) as usize;
        if len > u16::MAX as usize || len > buff.len() {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute);
        }

        if read(&mut buff[..len]).await.map_err(EitherError::B)? != len {
            zenoh_proto::zbail!(@log TransportError::InvalidAttribute)
        }

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    async fn decode_raw_with_async<E>(
        &mut self,
        mut read: impl AsyncFnMut(&mut [u8]) -> core::result::Result<usize, E>,
    ) -> core::result::Result<(), EitherError<TransportError, E>>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
        E: Display,
    {
        if self.state == State::Closed {
            return Ok(());
        }

        let max = core::cmp::min(self.buff.as_ref().len(), self.batch_size);
        let buff = &mut self.buff.as_mut()[self.cursor..max];

        let len = read(buff).await.map_err(EitherError::B)?;

        if len > 0 {
            self.state = State::Used;
        }

        self.cursor += len;

        Ok(())
    }

    fn flush(&mut self) -> impl Iterator<Item = (NetworkMessage<'_>, &'_ [u8])>
    where
        Buff: AsMut<[u8]> + AsRef<[u8]>,
    {
        // P5/asrun:欄位級 borrow-split,使 Close 幀可在惰性迭代中翻轉
        // State::Closed(graceful),而非無聲吞掉。
        let Self {
            cursor,
            batch_size,
            sn,
            resolution,
            ignore_invalid_sn: ignore,
            state,
            close_received,
            ..
        } = self;

        // buff 走 self 不可變讀取(避免 destructuring 後的 local reborrow lifetime 問題)
        let size = core::cmp::min(
            self.buff.as_ref().len(),
            core::cmp::min(*batch_size, *cursor),
        );
        *cursor = 0;

        FlushIter::<Buff> {
            reader: &self.buff.as_ref()[..size],
            state,
            close_received,
            last_frame: None,
            sn,
            resolution: *resolution,
            ignore: *ignore,
            _pd: core::marker::PhantomData,
        }
    }

    fn clear(&mut self) {
        self.cursor = 0;
    }
}

/// P5/asrun:惰性 flush 迭代器——欄位級借用,Close 幀在解出即時翻轉
/// `State::Closed` + flag(graceful),解決閉包無法同時可變 borrow 的問題。
struct FlushIter<'a, Buff: AsRef<[u8]>> {
    reader: &'a [u8],
    state: &'a mut State,
    close_received: &'a core::cell::Cell<bool>,
    last_frame: Option<FrameHeader>,
    sn: &'a mut u32,
    resolution: Resolution,
    ignore: bool,
    _pd: core::marker::PhantomData<Buff>,
}

impl<'a, Buff: AsRef<[u8]>> Iterator for FlushIter<'a, Buff> {
    type Item = (NetworkMessage<'a>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.reader.is_empty() {
                return None;
            }
            let m = TransportRx::<Buff>::decode(
                &mut self.reader,
                &mut self.last_frame,
                self.sn,
                self.resolution,
                self.ignore,
            )?;
            match m.0 {
                Message::Network(msg) => return Some((msg, m.1)),
                Message::Transport(TransportMessage::Close(_)) => {
                    *self.state = State::Closed;
                    self.close_received.set(true);
                    continue;
                }
                _ => continue,
            }
        }
    }
}
