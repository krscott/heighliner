//! Remote Input: a virtual keyboard and pointer that the daemon creates with
//! the kernel's uinput, which the desktop sees as devices plugged into this
//! Node, and the messages the web UI sends to them.
//!
//! Text is typed through a US keyboard layout, whatever layout the desktop
//! has, so only the characters that layout types without dead keys are
//! accepted.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use evdev::uinput::VirtualDevice;
use evdev::{AttributeSet, BusType, EventType, InputEvent, InputId, KeyCode, RelativeAxisCode};
use serde::Deserialize;

/// The name the desktop shows for the virtual devices.
const NAME: &str = "heighliner Remote Input";

/// The longest text one message may type.
pub const MAX_TEXT: usize = 1024;

/// How far one wheel notch is, in the high-resolution units scrolling is
/// sent in.
const NOTCH: i32 = 120;

/// Remote Input on this Node.
#[derive(Clone)]
pub enum RemoteInput {
    /// Not enabled on this Node.
    Off,
    /// Enabled, but the virtual keyboard and pointer couldn't be created.
    Failed(Arc<str>),
    Ready(Arc<Device>),
}

impl RemoteInput {
    /// Creates the virtual keyboard and pointer if `enabled`.
    pub fn open(enabled: bool) -> Self {
        if !enabled {
            return Self::Off;
        }
        match Device::create() {
            Ok(device) => Self::Ready(Arc::new(device)),
            Err(error) => {
                let error = format!(
                    "can't create the virtual keyboard and pointer through /dev/uinput: {error}"
                );
                tracing::warn!("{error}");
                Self::Failed(error.into())
            }
        }
    }
}

/// A key the page has a button for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Key {
    Escape,
    Tab,
    Enter,
    Backspace,
    Left,
    Right,
    Up,
    Down,
    Super,
    Ctrl,
    Alt,
    Shift,
    F11,
    VolumeDown,
    VolumeUp,
    Mute,
}

impl Key {
    const ALL: [Self; 16] = [
        Self::Escape,
        Self::Tab,
        Self::Enter,
        Self::Backspace,
        Self::Left,
        Self::Right,
        Self::Up,
        Self::Down,
        Self::Super,
        Self::Ctrl,
        Self::Alt,
        Self::Shift,
        Self::F11,
        Self::VolumeDown,
        Self::VolumeUp,
        Self::Mute,
    ];

    fn code(self) -> KeyCode {
        match self {
            Self::Escape => KeyCode::KEY_ESC,
            Self::Tab => KeyCode::KEY_TAB,
            Self::Enter => KeyCode::KEY_ENTER,
            Self::Backspace => KeyCode::KEY_BACKSPACE,
            Self::Left => KeyCode::KEY_LEFT,
            Self::Right => KeyCode::KEY_RIGHT,
            Self::Up => KeyCode::KEY_UP,
            Self::Down => KeyCode::KEY_DOWN,
            Self::Super => KeyCode::KEY_LEFTMETA,
            Self::Ctrl => KeyCode::KEY_LEFTCTRL,
            Self::Alt => KeyCode::KEY_LEFTALT,
            Self::Shift => KeyCode::KEY_LEFTSHIFT,
            Self::F11 => KeyCode::KEY_F11,
            Self::VolumeDown => KeyCode::KEY_VOLUMEDOWN,
            Self::VolumeUp => KeyCode::KEY_VOLUMEUP,
            Self::Mute => KeyCode::KEY_MUTE,
        }
    }
}

/// A pointer button.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Button {
    Left,
    Middle,
    Right,
}

impl Button {
    const ALL: [Self; 3] = [Self::Left, Self::Middle, Self::Right];

    fn code(self) -> KeyCode {
        match self {
            Self::Left => KeyCode::BTN_LEFT,
            Self::Middle => KeyCode::BTN_MIDDLE,
            Self::Right => KeyCode::BTN_RIGHT,
        }
    }
}

/// What the Remote Input page sends, one JSON object per WebSocket message.
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    /// Moves the pointer right by `x` and down by `y`.
    Move {
        x: i16,
        y: i16,
    },
    /// Scrolls the view right by `x` and down by `y`, in 120ths of a wheel
    /// notch.
    Scroll {
        x: i16,
        y: i16,
    },
    Button {
        button: Button,
        down: bool,
    },
    Key {
        key: Key,
        down: bool,
    },
    /// Types `text`, pressing and releasing a key for each character.
    Text {
        text: String,
    },
}

/// Why a message wasn't carried out.
#[derive(Debug)]
pub enum Refused {
    /// The US layout can't type this character; nothing was typed.
    Untypeable(char),
    TooLong,
    /// Writing to the virtual device failed.
    Device(io::Error),
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Untypeable(c) => write!(
                f,
                "can't type {c:?}: Remote Input types through a US keyboard layout"
            ),
            Self::TooLong => write!(f, "can't type more than {MAX_TEXT} characters at once"),
            Self::Device(error) => write!(f, "can't send input to the virtual devices: {error}"),
        }
    }
}

/// An input event, before it is written to the virtual device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Event {
    Press(KeyCode),
    Release(KeyCode),
    Move(RelativeAxisCode, i32),
}

impl From<Event> for InputEvent {
    fn from(event: Event) -> Self {
        match event {
            Event::Press(key) => InputEvent::new(EventType::KEY.0, key.code(), 1),
            Event::Release(key) => InputEvent::new(EventType::KEY.0, key.code(), 0),
            Event::Move(axis, value) => InputEvent::new(EventType::RELATIVE.0, axis.0, value),
        }
    }
}

/// The virtual keyboard and pointer: one uinput device that has both.
pub struct Device(Mutex<Sink>);

enum Sink {
    Uinput(VirtualDevice),
    /// Keeps each report sent, for tests.
    #[cfg(test)]
    Recorded(Vec<Vec<Event>>),
}

impl Device {
    fn create() -> io::Result<Self> {
        let mut keys = AttributeSet::<KeyCode>::new();
        for key in Key::ALL {
            keys.insert(key.code());
        }
        for button in Button::ALL {
            keys.insert(button.code());
        }
        for c in ' '..='~' {
            if let Some((key, _)) = us_key(c) {
                keys.insert(key);
            }
        }
        let mut axes = AttributeSet::<RelativeAxisCode>::new();
        for axis in [
            RelativeAxisCode::REL_X,
            RelativeAxisCode::REL_Y,
            RelativeAxisCode::REL_WHEEL,
            RelativeAxisCode::REL_HWHEEL,
            RelativeAxisCode::REL_WHEEL_HI_RES,
            RelativeAxisCode::REL_HWHEEL_HI_RES,
        ] {
            axes.insert(axis);
        }
        let device = VirtualDevice::builder()?
            .name(NAME)
            .input_id(InputId::new(BusType::BUS_VIRTUAL, 0, 0, 1))
            .with_keys(&keys)?
            .with_relative_axes(&axes)?
            .build()?;
        Ok(Self(Mutex::new(Sink::Uinput(device))))
    }

    #[cfg(test)]
    pub(crate) fn recorded() -> Self {
        Self(Mutex::new(Sink::Recorded(Vec::new())))
    }

    /// Takes the reports sent so far.
    #[cfg(test)]
    pub(crate) fn take(&self) -> Vec<Vec<Event>> {
        match &mut *self.0.lock().unwrap() {
            Sink::Recorded(reports) => std::mem::take(reports),
            Sink::Uinput(_) => unreachable!("only a recorded device is read"),
        }
    }

    /// Sends each of `reports`, the events in one at once.
    fn send(&self, reports: &[&[Event]]) -> io::Result<()> {
        let mut sink = self.0.lock().unwrap();
        for report in reports {
            match &mut *sink {
                Sink::Uinput(device) => {
                    let events: Vec<InputEvent> = report.iter().map(|&e| e.into()).collect();
                    device.emit(&events)?;
                }
                #[cfg(test)]
                Sink::Recorded(reports) => reports.push(report.to_vec()),
            }
        }
        Ok(())
    }
}

/// The key that types `c` on a US layout, and whether it needs Shift.
fn us_key(c: char) -> Option<(KeyCode, bool)> {
    const LETTERS: [KeyCode; 26] = [
        KeyCode::KEY_A,
        KeyCode::KEY_B,
        KeyCode::KEY_C,
        KeyCode::KEY_D,
        KeyCode::KEY_E,
        KeyCode::KEY_F,
        KeyCode::KEY_G,
        KeyCode::KEY_H,
        KeyCode::KEY_I,
        KeyCode::KEY_J,
        KeyCode::KEY_K,
        KeyCode::KEY_L,
        KeyCode::KEY_M,
        KeyCode::KEY_N,
        KeyCode::KEY_O,
        KeyCode::KEY_P,
        KeyCode::KEY_Q,
        KeyCode::KEY_R,
        KeyCode::KEY_S,
        KeyCode::KEY_T,
        KeyCode::KEY_U,
        KeyCode::KEY_V,
        KeyCode::KEY_W,
        KeyCode::KEY_X,
        KeyCode::KEY_Y,
        KeyCode::KEY_Z,
    ];
    // The digit row, from 1 to 0, and what it types with Shift.
    const DIGITS: [KeyCode; 10] = [
        KeyCode::KEY_1,
        KeyCode::KEY_2,
        KeyCode::KEY_3,
        KeyCode::KEY_4,
        KeyCode::KEY_5,
        KeyCode::KEY_6,
        KeyCode::KEY_7,
        KeyCode::KEY_8,
        KeyCode::KEY_9,
        KeyCode::KEY_0,
    ];
    const SHIFTED_DIGITS: &str = "!@#$%^&*()";
    // The other keys that type a character, then what they type with Shift.
    const SYMBOLS: [(char, char, KeyCode); 11] = [
        ('`', '~', KeyCode::KEY_GRAVE),
        ('-', '_', KeyCode::KEY_MINUS),
        ('=', '+', KeyCode::KEY_EQUAL),
        ('[', '{', KeyCode::KEY_LEFTBRACE),
        (']', '}', KeyCode::KEY_RIGHTBRACE),
        ('\\', '|', KeyCode::KEY_BACKSLASH),
        (';', ':', KeyCode::KEY_SEMICOLON),
        ('\'', '"', KeyCode::KEY_APOSTROPHE),
        (',', '<', KeyCode::KEY_COMMA),
        ('.', '>', KeyCode::KEY_DOT),
        ('/', '?', KeyCode::KEY_SLASH),
    ];
    let index = |base: u8| usize::from(c as u8 - base);
    Some(match c {
        'a'..='z' => (LETTERS[index(b'a')], false),
        'A'..='Z' => (LETTERS[index(b'A')], true),
        '1'..='9' => (DIGITS[index(b'1')], false),
        '0' => (KeyCode::KEY_0, false),
        ' ' => (KeyCode::KEY_SPACE, false),
        '\n' => (KeyCode::KEY_ENTER, false),
        '\t' => (KeyCode::KEY_TAB, false),
        _ => {
            if let Some(i) = SHIFTED_DIGITS.chars().position(|d| d == c) {
                return Some((DIGITS[i], true));
            }
            let &(plain, _, key) = SYMBOLS
                .iter()
                .find(|&&(plain, shifted, _)| c == plain || c == shifted)?;
            (key, c != plain)
        }
    })
}

/// One page's connection to the virtual devices. It remembers the keys and
/// buttons it holds down, and releases them when dropped, so a page that
/// loses its connection mid-drag leaves nothing held.
pub struct Session {
    device: Arc<Device>,
    held: BTreeSet<KeyCode>,
    /// Scrolling not yet sent as a whole wheel notch, right and down.
    unsent: (i32, i32),
}

impl Session {
    pub fn new(device: Arc<Device>) -> Self {
        Self {
            device,
            held: BTreeSet::new(),
            unsent: (0, 0),
        }
    }

    /// Carries out `message`.
    pub fn apply(&mut self, message: Message) -> Result<(), Refused> {
        let sent = match message {
            Message::Text { text } => return self.type_text(&text),
            Message::Move { x, y } => {
                let report: Vec<Event> =
                    [(RelativeAxisCode::REL_X, x), (RelativeAxisCode::REL_Y, y)]
                        .into_iter()
                        .filter(|&(_, value)| value != 0)
                        .map(|(axis, value)| Event::Move(axis, value.into()))
                        .collect();
                if report.is_empty() {
                    return Ok(());
                }
                self.device.send(&[&report])
            }
            Message::Scroll { x, y } => self.scroll(x.into(), y.into()),
            Message::Button { button, down } => self.press(button.code(), down),
            Message::Key { key, down } => self.press(key.code(), down),
        };
        sent.map_err(Refused::Device)
    }

    fn press(&mut self, key: KeyCode, down: bool) -> io::Result<()> {
        let event = if down {
            self.held.insert(key);
            Event::Press(key)
        } else {
            self.held.remove(&key);
            Event::Release(key)
        };
        self.device.send(&[&[event]])
    }

    /// Scrolls in high-resolution units, sending a whole notch too each time
    /// they add up to one, for programs that read only whole notches. The
    /// wheel's axes point right and up.
    fn scroll(&mut self, x: i32, y: i32) -> io::Result<()> {
        let (unsent_x, unsent_y) = (self.unsent.0 + x, self.unsent.1 + y);
        let (notches_x, notches_y) = (unsent_x / NOTCH, unsent_y / NOTCH);
        self.unsent = (unsent_x % NOTCH, unsent_y % NOTCH);
        let report: Vec<Event> = [
            (RelativeAxisCode::REL_HWHEEL_HI_RES, x),
            (RelativeAxisCode::REL_WHEEL_HI_RES, -y),
            (RelativeAxisCode::REL_HWHEEL, notches_x),
            (RelativeAxisCode::REL_WHEEL, -notches_y),
        ]
        .into_iter()
        .filter(|&(_, value)| value != 0)
        .map(|(axis, value)| Event::Move(axis, value))
        .collect();
        if report.is_empty() {
            return Ok(());
        }
        self.device.send(&[&report])
    }

    /// Types `text` if every character in it can be typed, and nothing
    /// otherwise. A character that needs Shift is typed with it, unless
    /// Shift is already held.
    fn type_text(&mut self, text: &str) -> Result<(), Refused> {
        if text.chars().count() > MAX_TEXT {
            return Err(Refused::TooLong);
        }
        let shift = KeyCode::KEY_LEFTSHIFT;
        let mut reports = Vec::new();
        for c in text.chars() {
            let (key, shifted) = us_key(c).ok_or(Refused::Untypeable(c))?;
            let add_shift = shifted && !self.held.contains(&shift);
            if add_shift {
                reports.push([Event::Press(shift)]);
            }
            reports.push([Event::Press(key)]);
            reports.push([Event::Release(key)]);
            if add_shift {
                reports.push([Event::Release(shift)]);
            }
        }
        let reports: Vec<&[Event]> = reports.iter().map(|report| report.as_slice()).collect();
        self.device.send(&reports).map_err(Refused::Device)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let released: Vec<[Event; 1]> =
            self.held.iter().map(|&key| [Event::Release(key)]).collect();
        let released: Vec<&[Event]> = released.iter().map(|report| report.as_slice()).collect();
        if let Err(error) = self.device.send(&released) {
            tracing::warn!(%error, "can't release the keys and buttons a Remote Input page held");
        }
    }
}

#[cfg(test)]
mod tests;
