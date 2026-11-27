//! Remote Input: a virtual keyboard and pointer that the daemon creates with
//! the kernel's uinput, which the desktop sees as devices plugged into this
//! Node, and the messages the web UI sends to them.
//!
//! Text is typed through a US keyboard layout, whatever layout the desktop
//! has, so only the characters that layout types without dead keys are
//! accepted. Keys are only ever pressed and released within one message;
//! only pointer buttons stay down between messages, for dragging.

use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use evdev::uinput::VirtualDevice;
use evdev::{AttributeSet, BusType, EventType, InputEvent, InputId, KeyCode, RelativeAxisCode};
use serde::Deserialize;
use tokio::time::Instant;

/// The name the desktop shows for the virtual devices.
const NAME: &str = "heighliner Remote Input";

/// The longest text one message may type.
pub const MAX_TEXT: usize = 1024;

/// The most keys one message may press at once.
const MAX_KEYS: usize = 8;

/// How far one wheel notch is, in the high-resolution units scrolling is
/// sent in.
const NOTCH: i32 = 120;

/// The pause after each keystroke or click. The kernel keeps only a few
/// dozen events for each reader of a device and drops the rest, so a
/// compositor that is busy for a moment would otherwise lose keystrokes.
const STROKE_GAP: Duration = Duration::from_millis(8);

/// How long a released button stays up before it is pressed again.
/// libinput takes a release and press closer together than its debounce
/// time for a bouncing switch and drops both, which would make a double
/// click one long click.
const BUTTON_GAP: Duration = Duration::from_millis(30);

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
    /// Creates the virtual keyboard and pointer.
    pub fn open() -> Self {
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

/// A key the page has a button for, or a modifier.
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Button {
    Left,
    Middle,
    Right,
}

impl Button {
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
    Move { x: i16, y: i16 },
    /// Scrolls the view right by `x` and down by `y`, in 120ths of a wheel
    /// notch.
    Scroll { x: i16, y: i16 },
    /// Presses `keys` in order, then releases them in reverse.
    Keys { keys: Vec<Key> },
    /// Clicks `button` with `modifiers` held.
    Click {
        button: Button,
        #[serde(default)]
        modifiers: Vec<Key>,
    },
    /// Presses `button` and leaves it down, or releases it.
    Button { button: Button, down: bool },
    /// Types `text`, a keystroke for each character, with `modifiers` held.
    Text {
        text: String,
        #[serde(default)]
        modifiers: Vec<Key>,
    },
}

/// Why a message wasn't carried out.
#[derive(Debug)]
pub enum Refused {
    /// The US layout can't type this character; nothing was typed.
    Untypeable(char),
    TooLong,
    TooManyKeys,
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
            Self::TooManyKeys => write!(f, "can't press more than {MAX_KEYS} keys at once"),
            Self::Device(error) => write!(f, "can't send input to the virtual devices: {error}"),
        }
    }
}

impl From<io::Error> for Refused {
    fn from(error: io::Error) -> Self {
        Self::Device(error)
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
        // Every keyboard key, which saves keeping a list in step with `Key`
        // and the layout, and the pointer's buttons.
        let mut keys = AttributeSet::<KeyCode>::new();
        for code in KeyCode::KEY_ESC.0..=KeyCode::KEY_MICMUTE.0 {
            keys.insert(KeyCode(code));
        }
        for button in [Button::Left, Button::Middle, Button::Right] {
            keys.insert(button.code());
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
    fn send(&self, reports: &[Vec<Event>]) -> io::Result<()> {
        let mut sink = self.0.lock().unwrap();
        for report in reports.iter().filter(|report| !report.is_empty()) {
            match &mut *sink {
                Sink::Uinput(device) => {
                    let events: Vec<InputEvent> = report.iter().map(|&e| e.into()).collect();
                    device.emit(&events)?;
                }
                #[cfg(test)]
                Sink::Recorded(reports) => reports.push(report.clone()),
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

/// Pressing `keys` in order, then releasing them in reverse, one report
/// each.
fn chord(keys: &[KeyCode]) -> Vec<Vec<Event>> {
    let presses = keys.iter().map(|&key| vec![Event::Press(key)]);
    let releases = keys.iter().rev().map(|&key| vec![Event::Release(key)]);
    presses.chain(releases).collect()
}

/// One page's connection to the virtual devices. It remembers the buttons
/// it holds down, and releases them when dropped, so a page that loses its
/// connection mid-drag leaves nothing held.
pub struct Session {
    device: Arc<Device>,
    held: BTreeSet<Button>,
    /// Scrolling not yet sent as a whole wheel notch, right and down.
    unsent: (i32, i32),
    /// When the next keystroke or click may be sent.
    next_stroke: Instant,
    /// When a button may next be pressed.
    next_press: Instant,
}

impl Session {
    pub fn new(device: Arc<Device>) -> Self {
        let now = Instant::now();
        Self {
            device,
            held: BTreeSet::new(),
            unsent: (0, 0),
            next_stroke: now,
            next_press: now,
        }
    }

    /// Carries out `message`, pausing as keystrokes and clicks need.
    pub async fn apply(&mut self, message: Message) -> Result<(), Refused> {
        match message {
            Message::Move { x, y } => {
                let report = [(RelativeAxisCode::REL_X, x), (RelativeAxisCode::REL_Y, y)]
                    .into_iter()
                    .filter(|&(_, value)| value != 0)
                    .map(|(axis, value)| Event::Move(axis, value.into()))
                    .collect();
                self.device.send(&[report])?;
            }
            Message::Scroll { x, y } => self.scroll(x.into(), y.into())?,
            Message::Keys { keys } => {
                let codes = codes(&keys)?;
                self.stroke(chord(&codes)).await?;
            }
            Message::Click { button, modifiers } => {
                let mut keys = codes(&modifiers)?;
                keys.push(button.code());
                tokio::time::sleep_until(self.next_press).await;
                self.stroke(chord(&keys)).await?;
                self.next_press = Instant::now() + BUTTON_GAP;
            }
            Message::Button { button, down } => {
                if down {
                    tokio::time::sleep_until(self.next_press).await;
                    self.held.insert(button);
                    self.device.send(&[vec![Event::Press(button.code())]])?;
                } else {
                    self.held.remove(&button);
                    self.device.send(&[vec![Event::Release(button.code())]])?;
                    self.next_press = Instant::now() + BUTTON_GAP;
                }
            }
            Message::Text { text, modifiers } => self.type_text(&text, &modifiers).await?,
        }
        Ok(())
    }

    /// Sends `reports` once the last keystroke or click has had its pause.
    async fn stroke(&mut self, reports: Vec<Vec<Event>>) -> io::Result<()> {
        tokio::time::sleep_until(self.next_stroke).await;
        self.device.send(&reports)?;
        self.next_stroke = Instant::now() + STROKE_GAP;
        Ok(())
    }

    /// Scrolls in high-resolution units, sending a whole notch too each time
    /// they add up to one, for programs that read only whole notches. The
    /// wheel's axes point right and up.
    fn scroll(&mut self, x: i32, y: i32) -> io::Result<()> {
        let (unsent_x, unsent_y) = (self.unsent.0 + x, self.unsent.1 + y);
        let (notches_x, notches_y) = (unsent_x / NOTCH, unsent_y / NOTCH);
        self.unsent = (unsent_x % NOTCH, unsent_y % NOTCH);
        let report = [
            (RelativeAxisCode::REL_HWHEEL_HI_RES, x),
            (RelativeAxisCode::REL_WHEEL_HI_RES, -y),
            (RelativeAxisCode::REL_HWHEEL, notches_x),
            (RelativeAxisCode::REL_WHEEL, -notches_y),
        ]
        .into_iter()
        .filter(|&(_, value)| value != 0)
        .map(|(axis, value)| Event::Move(axis, value))
        .collect();
        self.device.send(&[report])
    }

    /// Types `text`, each character a chord with `modifiers`, if every
    /// character in it can be typed, and nothing otherwise. A character that
    /// needs Shift is typed with it, unless Shift is one of the modifiers.
    async fn type_text(&mut self, text: &str, modifiers: &[Key]) -> Result<(), Refused> {
        if text.chars().count() > MAX_TEXT {
            return Err(Refused::TooLong);
        }
        let modifiers = codes(modifiers)?;
        let strokes = text
            .chars()
            .map(|c| {
                let (key, shifted) = us_key(c).ok_or(Refused::Untypeable(c))?;
                let mut keys = modifiers.clone();
                if shifted && !keys.contains(&KeyCode::KEY_LEFTSHIFT) {
                    keys.push(KeyCode::KEY_LEFTSHIFT);
                }
                keys.push(key);
                Ok(chord(&keys))
            })
            .collect::<Result<Vec<_>, Refused>>()?;
        for stroke in strokes {
            self.stroke(stroke).await?;
        }
        Ok(())
    }
}

/// The codes of `keys`, if there aren't too many to press at once.
fn codes(keys: &[Key]) -> Result<Vec<KeyCode>, Refused> {
    if keys.len() > MAX_KEYS {
        return Err(Refused::TooManyKeys);
    }
    Ok(keys.iter().map(|key| key.code()).collect())
}

impl Drop for Session {
    fn drop(&mut self) {
        let released: Vec<Vec<Event>> = self
            .held
            .iter()
            .map(|button| vec![Event::Release(button.code())])
            .collect();
        if let Err(error) = self.device.send(&released) {
            tracing::warn!(%error, "can't release the buttons a Remote Input page held");
        }
    }
}

#[cfg(test)]
mod tests;
