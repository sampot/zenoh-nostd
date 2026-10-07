//! T4.1 golden frames — vendored codec 輸出對齊 **zenoh(rust) 权威位元组**。
//!
//! 與 `golden_pico`(平行實作參照)不同,此處 golden 由**官方 rust
//! `zenoh-codec`/`zenoh-protocol` =1.10.1**(`Zenoh080::write`,即跑在
//! zenohd 上的编码器)對同一訊息的實際輸出捕得(scratch cargo project
//! 交叉運行,非自產自證)。Interest 為 P4 liveliness 跨-router 路由的
//! 關鍵幀;`Final` 變體為**已知分歧**——nostd 永不發出(見測試註解)。

use crate::{ZEncode, fields::*, keyexpr, msgs::*};

macro_rules! assert_wire {
    ($v:expr, [$($byte:literal),* $(,)?]) => {{
        let v = $v;
        let expect: &[u8] = &[$($byte),*];
        let mut arr = [0u8; 128];
        let remaining;
        {
            let mut w: &mut [u8] = &mut arr[..];
            ZEncode::z_encode(&v, &mut w).unwrap();
            remaining = w.len();
        }
        let written = arr.len() - remaining;
        assert_eq!(&arr[..written], expect, "wire drift vs rust zenoh-codec 1.10.1");
    }};
}

fn ke() -> &'static keyexpr {
    keyexpr::new("asrun/interop/live/**").unwrap()
}

fn interest(mode: InterestMode) -> NetworkBody<'static> {
    NetworkBody::Interest(Interest {
        id: 1,
        mode,
        inner: InterestInner {
            options: InterestOptions::KEYEXPRS.options | InterestOptions::TOKENS.options,
            wire_expr: Some(WireExpr::from(ke())),
        },
        ..Default::default()
    })
}

// —— rust:`|Z|MODE:2|INTEREST(0x19)| id K? options | WireExpr…|`——
// header=0x59(Future)|0x79(CurrentFuture);options=0x79(KEYEXPRS|TOKENS|R|N|M,
// R/N/M 由 wire_expr 推得,rust `interest.rs:options()`同式)。

#[test]
fn golden_rust_interest_future() {
    // rust 客户端 liveliness sub declare(history=false)所發幀,逐位元一致。
    assert_wire!(
        interest(InterestMode::Future),
        [
            0x59, 0x01, 0x79, 0x00, 0x15, b'a', b's', b'r', b'u', b'n', b'/', b'i', b'n',
            b't', b'e', b'r', b'o', b'p', b'/', b'l', b'i', b'v', b'e', b'/', b'*', b'*'
        ]
    );
}

#[test]
fn golden_rust_interest_current_future() {
    // history=true 變體(rust `InterestMode::CurrentFuture`),同 body。
    assert_wire!(
        interest(InterestMode::CurrentFuture),
        [
            0x79, 0x01, 0x79, 0x00, 0x15, b'a', b's', b'r', b'u', b'n', b'/', b'i', b'n',
            b't', b'e', b'r', b'o', b'p', b'/', b'l', b'i', b'v', b'e', b'/', b'*', b'*'
        ]
    );
}

#[test]
fn golden_rust_interest_final_divergence_pinned() {
    // **已知分歧釘定**:rust Final = `19 01`(mode==Final 不寫 options byte);
    // vendored macro 固定 FULL options → `19 01 09`。nostd **永不發 Final**
    // (liveliness sub undeclare 為 local-only,api/session/liveliness.rs),
    // 故此分歧不上線;若有人讓它上牌,此釘與 interop 測必爆。
    let v = NetworkBody::Interest(Interest {
        id: 1,
        mode: InterestMode::Final,
        ..Default::default()
    });
    assert_wire!(v, [0x19, 0x01, 0x00]);
}
