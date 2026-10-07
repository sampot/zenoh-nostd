//! asrun P4:入站 **resource 映射**(router 轉發 declaration 用 rid 引用 ke)。
//!
//! zenoh(rust) router 向南面 client 轉發 liveliness declaration 的權威流程
//! (zenohd 1.10.1 trace 實錘):先 `DeclareKeyExpr{id, wire_expr=inline ke}`
//! 註冊資源,再以 `DeclareToken{id, wire_expr={scope:id, suffix:""}}` 引用;
//! 直接 `keyexpr::new("")` 會炸 EmptyChunk。nostd/sansio 無內建資源表,
//! 故 session 層掛一張 no_alloc 小表(FIFO 淘汰)。
//!
//! WISP 邊界:`N` 槽、ke 上限 `MAX_KE` bytes;註冊滿時淘汰最舊,無法
//! 解析的後續引用優雅跳過(不投遞、不退 session)。

use heapless::{String, Vec};

/// 單條 ke 位元組上限(router 轉發的 token/scope ke;超長跳過)。
pub const MAX_KE: usize = 96;

/// rid(declare id,u16)→ full ke。同一張表覆蓋 DeclareKeyExpr 與
/// DeclareToken/UndeclareToken 的 id(router 對同一 ke 用同一 id)。
pub struct ScopeMap<const N: usize> {
    // heapless Vec 保序 → push/remove(0) 即 FIFO。id 取 u32 公分宽
    // (DeclareKeyExpr.id=u16 / DeclareToken.id=u32,face 內同一計數空間)。
    slots: Vec<(u32, String<MAX_KE>), N>,
}

impl<const N: usize> Default for ScopeMap<N> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
        }
    }
}

impl<const N: usize> ScopeMap<N> {
    pub fn new() -> Self {
        Self::default()
    }

    /// 註冊 rid→ke;同 id 覆寫,滿則淘汰最舊。超長 ke 跳過(return false)。
    pub fn insert(&mut self, id: u32, ke: &str) -> bool {
        if let Some(slot) = self.slots.iter_mut().find(|(i, _)| *i == id) {
            // 覆寫:截斷風險同上,超長即整筆退回(保留原值比半截 ke 安全)。
            if ke.len() > MAX_KE {
                return false;
            }
            slot.1.clear();
            if slot.1.push_str(ke).is_err() {
                return false;
            }
            return true;
        }
        if ke.len() > MAX_KE {
            return false;
        }
        let mut s = String::new();
        if s.push_str(ke).is_err() {
            return false;
        }
        if self.slots.is_full() {
            self.slots.remove(0); // FIFO 淘汰最舊
        }
        self.slots.push((id, s)).is_ok()
    }

    pub fn get(&self, id: u32) -> Option<&str> {
        self.slots.iter().find(|(i, _)| *i == id).map(|(_, k)| k.as_str())
    }

    /// token id 撤回時移除(undeaclre 後同一 rid 可能被 router 復用)。
    pub fn remove(&mut self, id: u32) {
        if let Some(pos) = self.slots.iter().position(|(i, _)| *i == id) {
            self.slots.remove(pos);
        }
    }

    /// 把可能帶 scope 引用的 wire_expr 解析為完整 ke 字串(借用本表)。
    /// scope==0 → inline suffix;scope!=0 → 表查 + "/" + suffix。
    /// `out` 僅在需要 join 時使用;回傳 None 表示無法解析(跳過)。
    pub fn resolve<'a>(&'a self, scope: u32, suffix: &'a str, out: &'a mut String<MAX_KE>) -> Option<&'a str> {
        if scope == 0 {
            return Some(suffix);
        }
        let base = self.get(scope)?;
        if suffix.is_empty() {
            out.clear();
            out.push_str(base).ok()?;
            return Some(out.as_str());
        }
        out.clear();
        out.push_str(base).ok()?;
        if !base.ends_with('/') && !suffix.starts_with('/') {
            out.push('/').ok()?;
        }
        out.push_str(suffix).ok()?;
        Some(out.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_fifo() {
        let mut m: ScopeMap<2> = ScopeMap::new();
        assert!(m.insert(1, "a/b"));
        assert!(m.insert(2, "c/d"));
        assert_eq!(m.get(1), Some("a/b"));
        assert!(m.insert(3, "e/f")); // 滿 → 淘汰 id=1
        assert_eq!(m.get(1), None);
        assert_eq!(m.get(2), Some("c/d"));
        assert_eq!(m.get(3), Some("e/f"));
    }

    #[test]
    fn oversize_ke_rejected() {
        let mut m: ScopeMap<2> = ScopeMap::new();
        let long = [b'k'; MAX_KE + 1];
        let long = core::str::from_utf8(&long).unwrap();
        assert!(!m.insert(1, long));
        assert_eq!(m.get(1), None);
    }

    #[test]
    fn resolve_scoped_and_inline() {
        let mut m: ScopeMap<2> = ScopeMap::new();
        assert!(m.insert(7, "asrun/interop/live/devA"));
        let mut out = String::<MAX_KE>::new();
        // 空 suffix(scope 引用本體,router token 轉發形態)
        assert_eq!(m.resolve(7, "", &mut out), Some("asrun/interop/live/devA"));
        // 非空 suffix → join
        assert_eq!(m.resolve(7, "sub", &mut out), Some("asrun/interop/live/devA/sub"));
        // 未知 rid → None(優雅跳過)
        assert_eq!(m.resolve(8, "x", &mut out), None);
    }
}
