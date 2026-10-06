// asrun P3: liveliness 事件型別 — token online/offline,交付 liveliness subscribers。
//
// 線格式對齊 zenoh-pico 1.x:token 以 `Declare{DeclareToken/UndeclareToken}`
// (MID 6/7,見 zenoh-pico `protocol/codec/declarations.h`)交換,接收端合成
// 狀態事件 — 對應 zenoh「liveliness token 如瞬时 Put/Delete」語意,但本 crate
// 停在事件層(不拷貝 Sample,no_alloc 友善)。

use zenoh_proto::keyexpr;

/// 一次 liveliness 狀態轉換:`online == true` 為 token 宣告,`false` 為撤回。
pub struct LivelinessEvent<'a> {
    keyexpr: &'a keyexpr,
    online: bool,
}

impl<'a> LivelinessEvent<'a> {
    pub(crate) fn new(keyexpr: &'a keyexpr, online: bool) -> Self {
        Self { keyexpr, online }
    }

    pub fn keyexpr(&self) -> &'a keyexpr {
        self.keyexpr
    }

    pub fn online(&self) -> bool {
        self.online
    }
}
