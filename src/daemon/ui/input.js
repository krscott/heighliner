// The Remote Input page: turns touches on the touchpad, the buttons and
// typing into messages for this Node's virtual keyboard and pointer, sent
// over a WebSocket. Keys are pressed and released within one message; the
// daemon releases any button a page held when its connection closes.
"use strict";
(() => {
  const remote = document.querySelector(".remote[data-socket]");
  if (!remote) {
    return;
  }
  const pad = remote.querySelector(".touchpad");
  const typing = remote.querySelector(".typing");
  const state = remote.querySelector(".remote-state");
  const note = remote.querySelector(".remote-note");

  // Pointer pixels per CSS pixel a finger moves.
  const SPEED = 2;
  // Scrolling, in 120ths of a wheel notch, per CSS pixel two fingers move.
  const SCROLL = 4;
  // A touch shorter than this that moves less than SLOP CSS pixels is a tap.
  const TAP_MS = 250;
  const SLOP = 8;
  // How long after a tap a touch counts as the second half of a double tap.
  const DOUBLE_MS = 300;
  // How long a connection waits before trying again.
  const RETRY_MS = 1000;
  // How long a notice stays.
  const NOTICE_MS = 5000;
  // The most text one message may type, as the daemon allows.
  const MAX_TEXT = Number(remote.dataset.maxText);

  let socket = null;
  let retry = null;
  // A tap's click, waiting to see whether a second touch follows.
  let waiting = null;
  // The touch on the touchpad, from its first finger landing to its last
  // lifting.
  let gesture = null;

  function show(text, kind) {
    state.textContent = text;
    state.className = `badge remote-state ${kind}`;
  }

  let noticed = null;

  function notice(text) {
    note.textContent = text;
    clearTimeout(noticed);
    noticed = setTimeout(() => {
      note.textContent = "";
    }, NOTICE_MS);
  }

  function connect() {
    clearTimeout(retry);
    if (socket || document.hidden) {
      return;
    }
    show("connecting", "unknown");
    const url = new URL(remote.dataset.socket, location.href);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    const opened = new WebSocket(url);
    socket = opened;
    opened.addEventListener("open", () => {
      // What was typed while disconnected never arrived, and the daemon
      // released what the last connection held.
      forget();
      held.clear();
      show("connected", "good");
    });
    opened.addEventListener("message", (event) => {
      try {
        const answer = JSON.parse(event.data);
        if (answer.refused) {
          forget();
          notice(`Refused: ${answer.refused}`);
        }
      } catch {
        // Not an answer this page knows.
      }
    });
    opened.addEventListener("close", () => {
      if (socket !== opened) {
        return;
      }
      socket = null;
      show("disconnected", "bad");
      retry = setTimeout(connect, RETRY_MS);
    });
  }

  // A hidden page closes its connection, so nothing stays held while the
  // phone sleeps, and connects again when it is shown.
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      const closing = socket;
      socket = null;
      closing?.close();
    } else {
      connect();
    }
  });

  function send(message) {
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify(message));
    }
  }

  // Modifiers chosen for the next key, character or click.
  const latched = new Set();

  function setLatched(key, on) {
    if (on) {
      latched.add(key);
    } else {
      latched.delete(key);
    }
    const button = remote.querySelector(`[data-modifier="${key}"]`);
    button?.setAttribute("aria-pressed", String(on));
  }

  // The latched modifiers, which are then no longer latched.
  function takeModifiers() {
    const taken = [...latched];
    for (const key of taken) {
      setLatched(key, false);
    }
    return taken;
  }

  // Movement not sent yet, sent once per frame, or before anything that
  // must come after it.
  const pending = { moveX: 0, moveY: 0, scrollX: 0, scrollY: 0 };
  let frame = null;

  function flush() {
    frame = null;
    const take = (key) => {
      const whole = Math.trunc(pending[key]);
      pending[key] -= whole;
      return whole;
    };
    const [x, y] = [take("moveX"), take("moveY")];
    if (x || y) {
      send({ type: "move", x, y });
    }
    const [sx, sy] = [take("scrollX"), take("scrollY")];
    if (sx || sy) {
      send({ type: "scroll", x: sx, y: sy });
    }
  }

  function queue(key, amount) {
    pending[key] += amount;
    frame ??= requestAnimationFrame(flush);
  }

  // The buttons the page holds down, which a click would release. Each
  // control and the touchpad's drag press and release them alike, so
  // whichever lets go first releases a button.
  const held = new Set();

  function sendClick(button) {
    if (!held.has(button)) {
      flush();
      send({ type: "click", button, modifiers: takeModifiers() });
    }
  }

  // Sends a tap's click that is waiting to see whether a second tap
  // follows, so that it comes before whatever is sent next. A touch that
  // started meanwhile can then no longer become a drag. Queuing the tap
  // emptied the typing field already.
  function settle() {
    if (waiting !== null) {
      clearTimeout(waiting);
      waiting = null;
      sendClick("left");
    } else if (gesture?.owed) {
      gesture.owed = false;
      sendClick("left");
      queue("moveX", gesture.early.x);
      queue("moveY", gesture.early.y);
    }
  }

  function click(button) {
    settle();
    forget();
    sendClick(button);
  }

  function pressKeys(keys) {
    settle();
    forget();
    send({ type: "keys", keys: [...takeModifiers(), ...keys] });
  }

  function press(button) {
    settle();
    forget();
    if (!held.has(button)) {
      held.add(button);
      flush();
      send({ type: "button", button, down: true });
    }
  }

  function release(button) {
    if (held.delete(button)) {
      flush();
      send({ type: "button", button, down: false });
    }
  }

  // Pressing a key or modifier leaves the focus where it was, so the
  // phone's keyboard stays open while typing.
  for (const button of remote.querySelectorAll("[data-keys], [data-modifier]")) {
    button.addEventListener("pointerdown", (event) => event.preventDefault());
    button.addEventListener("mousedown", (event) => event.preventDefault());
  }

  for (const button of remote.querySelectorAll("[data-keys]")) {
    button.addEventListener("click", () => pressKeys(button.dataset.keys.split(" ")));
  }

  for (const button of remote.querySelectorAll("[data-modifier]")) {
    const key = button.dataset.modifier;
    button.addEventListener("click", () => {
      // A tap before the modifier doesn't get it, and what the field holds
      // no longer matters.
      settle();
      forget();
      setLatched(key, !latched.has(key));
    });
  }

  // Mouse buttons are held from a finger landing on them until one lifts,
  // so one can be held while another finger moves the pointer. With a
  // modifier latched, one clicks instead.
  for (const button of remote.querySelectorAll("[data-button]")) {
    const name = button.dataset.button;
    button.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      button.setPointerCapture(event.pointerId);
      // A tap before this may take the latched modifiers first.
      settle();
      if (latched.size > 0) {
        click(name);
      } else {
        press(name);
      }
    });
    button.addEventListener("pointerup", () => release(name));
    button.addEventListener("pointercancel", () => release(name));
    button.addEventListener("contextmenu", (event) => event.preventDefault());
  }

  // The touchpad. One finger moves the pointer and two scroll, moving the
  // view with the fingers, as on a phone. A tap clicks, once it is clear it
  // isn't the first half of a double tap; a touch that starts just after a
  // tap and moves drags.
  function centre(touches) {
    let x = 0;
    let y = 0;
    for (const touch of touches) {
      x += touch.clientX;
      y += touch.clientY;
    }
    return { x: x / touches.length, y: y / touches.length };
  }

  pad.addEventListener("touchstart", (event) => {
    event.preventDefault();
    const touches = event.targetTouches;
    if (!gesture) {
      const afterTap = waiting !== null;
      clearTimeout(waiting);
      waiting = null;
      gesture = {
        start: performance.now(),
        fingers: 0,
        travelled: 0,
        // A click is owed for the tap this touch followed.
        owed: afterTap,
        dragging: false,
        // Movement held back until it is clear whether this is a drag, so
        // that the press comes before it.
        early: { x: 0, y: 0 },
      };
    }
    gesture.fingers = Math.max(gesture.fingers, touches.length);
    gesture.at = centre(touches);
    gesture.count = touches.length;
    // A second finger makes it a scroll or a right click, not a drag.
    if (gesture.fingers > 1) {
      settle();
    }
  });

  pad.addEventListener("touchmove", (event) => {
    event.preventDefault();
    if (!gesture) {
      return;
    }
    const touches = event.targetTouches;
    const at = centre(touches);
    // A finger lifting or landing moves the centre; that isn't movement.
    if (touches.length !== gesture.count) {
      gesture.at = at;
      gesture.count = touches.length;
      return;
    }
    const dx = at.x - gesture.at.x;
    const dy = at.y - gesture.at.y;
    gesture.at = at;
    gesture.travelled += Math.abs(dx) + Math.abs(dy);
    if (touches.length > 1) {
      queue("scrollX", -dx * SCROLL);
      queue("scrollY", -dy * SCROLL);
    } else if (gesture.owed && !gesture.dragging) {
      gesture.early.x += dx * SPEED;
      gesture.early.y += dy * SPEED;
      if (gesture.travelled > SLOP) {
        gesture.owed = false;
        gesture.dragging = true;
        press("left");
        queue("moveX", gesture.early.x);
        queue("moveY", gesture.early.y);
      }
    } else {
      queue("moveX", dx * SPEED);
      queue("moveY", dy * SPEED);
    }
  });

  pad.addEventListener("touchend", (event) => {
    event.preventDefault();
    if (!gesture) {
      return;
    }
    if (event.targetTouches.length > 0) {
      gesture.at = centre(event.targetTouches);
      gesture.count = event.targetTouches.length;
      return;
    }
    const ended = gesture;
    gesture = null;
    if (ended.dragging) {
      release("left");
      return;
    }
    const isTap = performance.now() - ended.start < TAP_MS && ended.travelled < SLOP;
    if (ended.owed) {
      click("left");
    }
    if (!isTap) {
      return;
    }
    if (ended.fingers > 1) {
      click("right");
    } else if (ended.owed) {
      // The second tap of a double tap.
      click("left");
    } else {
      // The click will move the text cursor on the Node.
      forget();
      waiting = setTimeout(() => {
        waiting = null;
        click("left");
      }, DOUBLE_MS);
    }
  });

  // A touch the browser took away, for example for a system gesture,
  // clicks nothing, though the tap before it still does; a drag it was
  // making ends.
  pad.addEventListener("touchcancel", () => {
    const cancelled = gesture;
    gesture = null;
    if (cancelled?.dragging) {
      release("left");
    } else if (cancelled?.owed) {
      click("left");
    }
  });

  // Typing. While the user only types, what the field holds is compared
  // with what it held, so that whatever the phone's keyboard does, such as
  // replacing a word it corrected, is sent as the backspaces and characters
  // that do the same. Anything else sent can move the text cursor on the
  // Node, so it empties the field first. The US layout the daemon types
  // with has only printable ASCII; the punctuation phones put in for it is
  // typed as what it replaced.
  const plain = {
    "‘": "'",
    "’": "'",
    "“": '"',
    "”": '"',
    "–": "-",
    "—": "--",
    "…": "...",
  };
  const typeable = (c) => c === "\n" || c === "\t" || (c >= " " && c <= "~");
  let typed = [];

  function forget() {
    typing.value = "";
    typed = [];
  }

  typing.addEventListener("input", () => {
    // A tap before this may take the latched modifiers first; it emptied
    // the field already.
    settle();
    const chars = Array.from(typing.value).flatMap((c) => Array.from(plain[c] ?? c));
    const kept = chars.filter(typeable);
    if (kept.length !== chars.length) {
      const dropped = [...new Set(chars.filter((c) => !typeable(c)))].join(" ");
      notice(`Can't type ${dropped}: Remote Input types through a US keyboard layout.`);
    }
    if (kept.join("") !== typing.value) {
      typing.value = kept.join("");
    }
    let same = 0;
    while (same < kept.length && same < typed.length && kept[same] === typed[same]) {
      same += 1;
    }
    const erase = typed.length - same;
    const text = kept.slice(same);
    typed = kept;
    if (text.length > MAX_TEXT) {
      notice(`Can't type more than ${MAX_TEXT} characters at once.`);
      forget();
      return;
    }
    // With a modifier latched, one character, even one that replaced a
    // selection in the field, or one Backspace is a shortcut; anything
    // more, such as a word the keyboard suggested, isn't sent.
    if (latched.size > 0) {
      forget();
      if (text.length === 1) {
        send({ type: "text", text: text[0], modifiers: takeModifiers() });
      } else if (text.length === 0 && erase === 1) {
        pressKeys(["backspace"]);
      } else if (text.length > 0 || erase > 0) {
        notice("With a modifier, type one character.");
      }
      return;
    }
    for (let i = 0; i < erase; i += 1) {
      send({ type: "keys", keys: ["backspace"] });
    }
    if (text.length > 0) {
      send({ type: "text", text: text.join(""), modifiers: [] });
    }
    // A finished line starts a new one, which keeps the field short.
    if (text.at(-1) === "\n") {
      forget();
    }
  });

  // An empty field has nothing to delete, so Backspace makes no input.
  typing.addEventListener("keydown", (event) => {
    if (event.key === "Backspace" && typing.value === "") {
      event.preventDefault();
      pressKeys(["backspace"]);
    }
  });

  const fullscreen = remote.querySelector(".fullscreen");
  if (document.fullscreenEnabled) {
    fullscreen.hidden = false;
    fullscreen.addEventListener("click", () => {
      if (document.fullscreenElement) {
        document.exitFullscreen();
      } else {
        document.documentElement.requestFullscreen();
      }
    });
  }

  connect();
})();
