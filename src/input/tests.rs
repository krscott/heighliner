use super::*;

fn session() -> (Arc<Device>, Session) {
    let device = Arc::new(Device::recorded());
    (device.clone(), Session::new(device))
}

fn parse(json: &str) -> Message {
    serde_json::from_str(json).unwrap()
}

use Event::{Move, Press, Release};

const SHIFT: KeyCode = KeyCode::KEY_LEFTSHIFT;
const CTRL: KeyCode = KeyCode::KEY_LEFTCTRL;

#[test]
fn messages_are_read_from_json() {
    assert_eq!(
        parse(r#"{"type":"move","x":-3,"y":4}"#),
        Message::Move { x: -3, y: 4 }
    );
    assert_eq!(
        parse(r#"{"type":"keys","keys":["ctrl","volume_up"]}"#),
        Message::Keys {
            keys: vec![Key::Ctrl, Key::VolumeUp]
        }
    );
    assert_eq!(
        parse(r#"{"type":"click","button":"right"}"#),
        Message::Click {
            button: Button::Right,
            modifiers: vec![]
        }
    );
    assert_eq!(
        parse(r#"{"type":"text","text":"a","modifiers":["shift"]}"#),
        Message::Text {
            text: "a".into(),
            modifiers: vec![Key::Shift]
        }
    );
    // Out of range, or no such key.
    for json in [
        r#"{"type":"move","x":40000,"y":0}"#,
        r#"{"type":"keys","keys":["power"]}"#,
    ] {
        assert!(serde_json::from_str::<Message>(json).is_err(), "{json}");
    }
}

#[tokio::test]
async fn text_is_typed_through_a_us_layout() {
    let (device, mut session) = session();
    session
        .apply(Message::Text {
            text: "aZ!\n".into(),
            modifiers: vec![],
        })
        .await
        .unwrap();
    let a = KeyCode::KEY_A;
    let z = KeyCode::KEY_Z;
    let one = KeyCode::KEY_1;
    let enter = KeyCode::KEY_ENTER;
    assert_eq!(
        device.take(),
        [
            vec![Press(a)],
            vec![Release(a)],
            vec![Press(SHIFT)],
            vec![Press(z)],
            vec![Release(z)],
            vec![Release(SHIFT)],
            vec![Press(SHIFT)],
            vec![Press(one)],
            vec![Release(one)],
            vec![Release(SHIFT)],
            vec![Press(enter)],
            vec![Release(enter)],
        ]
    );
}

#[test]
fn every_printable_ascii_character_can_be_typed() {
    for c in ' '..='~' {
        assert!(us_key(c).is_some(), "{c:?}");
    }
    assert_eq!(us_key('"'), Some((KeyCode::KEY_APOSTROPHE, true)));
    assert_eq!(us_key('0'), Some((KeyCode::KEY_0, false)));
    assert_eq!(us_key(')'), Some((KeyCode::KEY_0, true)));
}

#[tokio::test]
async fn text_with_a_character_the_layout_lacks_types_nothing() {
    let (device, mut session) = session();
    let refused = session
        .apply(Message::Text {
            text: "café".into(),
            modifiers: vec![Key::Ctrl],
        })
        .await;
    assert!(matches!(refused, Err(Refused::Untypeable('é'))));
    assert!(device.take().is_empty());

    let refused = session
        .apply(Message::Text {
            text: "a".repeat(MAX_TEXT + 1),
            modifiers: vec![],
        })
        .await;
    assert!(matches!(refused, Err(Refused::TooLong)));
    assert!(device.take().is_empty());
}

#[tokio::test]
async fn modifiers_are_held_around_text_keys_and_clicks() {
    let (device, mut session) = session();
    session
        .apply(Message::Text {
            text: "A".into(),
            modifiers: vec![Key::Ctrl, Key::Shift],
        })
        .await
        .unwrap();
    let a = KeyCode::KEY_A;
    // Shift is already held, so it isn't pressed again for the capital.
    assert_eq!(
        device.take(),
        [
            vec![Press(CTRL)],
            vec![Press(SHIFT)],
            vec![Press(a)],
            vec![Release(a)],
            vec![Release(SHIFT)],
            vec![Release(CTRL)],
        ]
    );
    // Each character is a chord of its own, so no modifier stays down
    // between them.
    session
        .apply(Message::Text {
            text: "ab".into(),
            modifiers: vec![Key::Alt],
        })
        .await
        .unwrap();
    let (alt, b) = (KeyCode::KEY_LEFTALT, KeyCode::KEY_B);
    assert_eq!(
        device.take(),
        [
            vec![Press(alt)],
            vec![Press(a)],
            vec![Release(a)],
            vec![Release(alt)],
            vec![Press(alt)],
            vec![Press(b)],
            vec![Release(b)],
            vec![Release(alt)],
        ]
    );

    session
        .apply(Message::Keys {
            keys: vec![Key::Alt, Key::Tab],
        })
        .await
        .unwrap();
    let (alt, tab) = (KeyCode::KEY_LEFTALT, KeyCode::KEY_TAB);
    assert_eq!(
        device.take(),
        [
            vec![Press(alt)],
            vec![Press(tab)],
            vec![Release(tab)],
            vec![Release(alt)]
        ]
    );

    session
        .apply(Message::Click {
            button: Button::Left,
            modifiers: vec![Key::Ctrl],
        })
        .await
        .unwrap();
    let left = KeyCode::BTN_LEFT;
    assert_eq!(
        device.take(),
        [
            vec![Press(CTRL)],
            vec![Press(left)],
            vec![Release(left)],
            vec![Release(CTRL)]
        ]
    );

    let refused = session
        .apply(Message::Keys {
            keys: vec![Key::Tab; MAX_KEYS + 1],
        })
        .await;
    assert!(matches!(refused, Err(Refused::TooManyKeys)));
    assert!(device.take().is_empty());
}

#[tokio::test]
async fn a_double_click_leaves_the_button_up_long_enough_to_count() {
    let (_, mut session) = session();
    let click = || Message::Click {
        button: Button::Left,
        modifiers: vec![],
    };
    session.apply(click()).await.unwrap();
    let released = Instant::now();
    session.apply(click()).await.unwrap();
    assert!(released.elapsed() >= BUTTON_GAP);
}

#[tokio::test]
async fn moves_skip_still_axes() {
    let (device, mut session) = session();
    session.apply(Message::Move { x: 5, y: 0 }).await.unwrap();
    session.apply(Message::Move { x: 0, y: 0 }).await.unwrap();
    assert_eq!(device.take(), [vec![Move(RelativeAxisCode::REL_X, 5)]]);
}

#[tokio::test]
async fn scrolling_sends_whole_notches_as_they_add_up() {
    const REL_HWHEEL_HI_RES: RelativeAxisCode = RelativeAxisCode::REL_HWHEEL_HI_RES;
    const REL_WHEEL: RelativeAxisCode = RelativeAxisCode::REL_WHEEL;
    const REL_WHEEL_HI_RES: RelativeAxisCode = RelativeAxisCode::REL_WHEEL_HI_RES;
    let (device, mut session) = session();
    // Down: the wheel's axis points up.
    for (x, y) in [(0, 100), (0, 30), (-7, 0)] {
        session.apply(Message::Scroll { x, y }).await.unwrap();
    }
    assert_eq!(
        device.take(),
        [
            vec![Move(REL_WHEEL_HI_RES, -100)],
            vec![Move(REL_WHEEL_HI_RES, -30), Move(REL_WHEEL, -1)],
            vec![Move(REL_HWHEEL_HI_RES, -7)],
        ]
    );
}

#[tokio::test]
async fn dropping_a_session_releases_the_buttons_it_held() {
    let (device, mut session) = session();
    for (button, down) in [
        (Button::Left, true),
        (Button::Right, true),
        (Button::Right, false),
    ] {
        session
            .apply(Message::Button { button, down })
            .await
            .unwrap();
    }
    device.take();
    drop(session);
    assert_eq!(device.take(), [vec![Release(KeyCode::BTN_LEFT)]]);
}
