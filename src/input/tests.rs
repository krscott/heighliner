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

#[test]
fn messages_are_read_from_json() {
    assert_eq!(
        parse(r#"{"type":"move","x":-3,"y":4}"#),
        Message::Move { x: -3, y: 4 }
    );
    assert_eq!(
        parse(r#"{"type":"key","key":"volume_up","down":true}"#),
        Message::Key {
            key: Key::VolumeUp,
            down: true
        }
    );
    assert_eq!(
        parse(r#"{"type":"button","button":"right","down":false}"#),
        Message::Button {
            button: Button::Right,
            down: false
        }
    );
    // Out of range, or no such key.
    for json in [
        r#"{"type":"move","x":40000,"y":0}"#,
        r#"{"type":"key","key":"power","down":true}"#,
    ] {
        assert!(serde_json::from_str::<Message>(json).is_err(), "{json}");
    }
}

#[test]
fn text_is_typed_through_a_us_layout() {
    let (device, mut session) = session();
    session
        .apply(Message::Text {
            text: "aZ!\n".into(),
        })
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

#[test]
fn text_with_a_character_the_layout_lacks_types_nothing() {
    let (device, mut session) = session();
    let refused = session.apply(Message::Text {
        text: "café".into(),
    });
    assert!(matches!(refused, Err(Refused::Untypeable('é'))));
    assert!(device.take().is_empty());

    let refused = session.apply(Message::Text {
        text: "a".repeat(MAX_TEXT + 1),
    });
    assert!(matches!(refused, Err(Refused::TooLong)));
    assert!(device.take().is_empty());
}

#[test]
fn a_held_shift_is_left_alone() {
    let (device, mut session) = session();
    session
        .apply(Message::Key {
            key: Key::Shift,
            down: true,
        })
        .unwrap();
    session.apply(Message::Text { text: "A".into() }).unwrap();
    let a = KeyCode::KEY_A;
    assert_eq!(
        device.take(),
        [vec![Press(SHIFT)], vec![Press(a)], vec![Release(a)]]
    );
}

#[test]
fn moves_skip_still_axes() {
    let (device, mut session) = session();
    session.apply(Message::Move { x: 5, y: 0 }).unwrap();
    session.apply(Message::Move { x: 0, y: 0 }).unwrap();
    assert_eq!(device.take(), [vec![Move(RelativeAxisCode::REL_X, 5)]]);
}

#[test]
fn scrolling_sends_whole_notches_as_they_add_up() {
    const REL_HWHEEL_HI_RES: RelativeAxisCode = RelativeAxisCode::REL_HWHEEL_HI_RES;
    const REL_WHEEL: RelativeAxisCode = RelativeAxisCode::REL_WHEEL;
    const REL_WHEEL_HI_RES: RelativeAxisCode = RelativeAxisCode::REL_WHEEL_HI_RES;
    let (device, mut session) = session();
    // Down: the wheel's axis points up.
    session.apply(Message::Scroll { x: 0, y: 100 }).unwrap();
    session.apply(Message::Scroll { x: 0, y: 30 }).unwrap();
    session.apply(Message::Scroll { x: -7, y: 0 }).unwrap();
    assert_eq!(
        device.take(),
        [
            vec![Move(REL_WHEEL_HI_RES, -100)],
            vec![Move(REL_WHEEL_HI_RES, -30), Move(REL_WHEEL, -1)],
            vec![Move(REL_HWHEEL_HI_RES, -7)],
        ]
    );
}

#[test]
fn dropping_a_session_releases_what_it_held() {
    let (device, mut session) = session();
    for message in [
        Message::Button {
            button: Button::Left,
            down: true,
        },
        Message::Key {
            key: Key::Ctrl,
            down: true,
        },
        Message::Key {
            key: Key::Alt,
            down: true,
        },
        Message::Key {
            key: Key::Alt,
            down: false,
        },
    ] {
        session.apply(message).unwrap();
    }
    device.take();
    drop(session);
    assert_eq!(
        device.take(),
        [
            vec![Release(KeyCode::KEY_LEFTCTRL)],
            vec![Release(KeyCode::BTN_LEFT)],
        ]
    );
}
