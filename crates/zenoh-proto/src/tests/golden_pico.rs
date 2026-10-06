//! T4.1 golden frames — vendored codec 輸出對齊 zenoh-pico 的权威位元组。
//!
//! 每個 golden 常量均由 **zenoh-pico 源碼定義**推得(逐條附 `pico:` 引用),
//! 而非以 vendored 編碼器自產自證:若兩側對這些 byte 有分歧即視為回歸。
//! real-pico 運行期對比(CMake 構 `libzenohpico`)留 M4;此處先釘住
//! 規格層 byte,守住 close 語義(P5 S-bit)與基礎 transport 幀不退化。
//!
//! no_std 友好:固定長度 array,不依賴 alloc;同時校驗 `z_len` 與實際寫入長一致。

use crate::{ZEncode, ZLen, msgs::*};

macro_rules! assert_frame {
    ($v:expr, [$($byte:literal),* $(,)?]) => {{
        let v = $v;
        let expect: &[u8] = &[$($byte),*];
        let len = v.z_len();
        assert_eq!(len, expect.len(), "z_len drift vs golden");
        let mut buf = [0u8; 8];
        if len > buf.len() { panic!("golden frame exceeds buffer"); }
        ZEncode::z_encode(&v, &mut &mut buf[..]).unwrap();
        assert_eq!(&buf[..len], expect);
    }};
}

// —— Close: pico `definitions/transport.h` `_Z_MID_T_CLOSE=0x03`,S=`0x20`(bit5) ——
// 幀:`|Z|X|S| CLOSE(0x03)|` + `[reason]`(`codec/transport.c:_z_close_encode`)。

#[test]
fn golden_close_session_generic() {
    // P5 優雅關閉(nostd io close()):Session→S=1 → header 0x23;reason Generic=0。
    // pico:正常 close 走 `_z_t_msg_make_close(reason, link_only=false)`(unicast/transport.c:324)。
    assert_frame!(
        Close { reason: 0x00, behaviour: CloseBehaviour::Session },
        [0x23, 0x00]
    );
}

#[test]
fn golden_close_session_expired() {
    // 租約逾時(close_expired()):reason Expired=0x05。
    // pico:`lease.c` `_z_unicast_transport_close(ztu, _Z_CLOSE_EXPIRED)` → S=1。
    assert_frame!(
        Close { reason: 0x05, behaviour: CloseBehaviour::Session },
        [0x23, 0x05]
    );
}

#[test]
fn golden_close_link_generic_upstream_default() {
    // 對照:upstream sansio `tx.close()` 用 `Close::default()`=Link(S=0) → header 0x03。
    // 此即 P5 修正前誤發的字節;保留以顯式標記「Link≠pico 常規 close」。
    let v = Close::default();
    assert_eq!(v.behaviour, CloseBehaviour::Link);
    assert_frame!(v, [0x03, 0x00]);
}

// —— KeepAlive: pico `_Z_MID_T_KEEP_ALIVE=0x04`,無 body ——

#[test]
fn golden_keepalive() {
    // 幀:`|_|_| KEEP_ALIVE(0x04)|`,無 body(`codec/transport.c:_z_keep_alive_encode` no-op)。
    assert_frame!(KeepAlive, [0x04]);
}
