// The Remote Input page: turns touches on the touchpad, the buttons and
// typing into messages for this Node's virtual keyboard and pointer, sent
// over a WebSocket. The daemon releases whatever a page held when its
// connection closes.
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
  // A touch shorter than this that moves less than SLOP pixels is a tap.
  const TAP_MS = 250;
  const SLOP = 8;
  // How long after a tap a touch counts as the second half of a double tap.
  const DOUBLE_MS = 300;
  // How long a connection waits before trying again.
  const RETRY_MS = 1000;
  // How long a notice stays.
  const NOTICE_MS = 5000;

  let socket = null;
  let retry = null;

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
    opened.addEventListener("open", () => show("connected", "good"));
    opened.addEventListener("message", (event) => {
      try {
        const answer = JSON.parse(event.data);
        if (answer.refused) {
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

  // Runs `act` with the latched modifiers held, then releases them.
  function withModifiers(act) {
    const held = [...latched];
    for (const key of held) {
      send({ type: "key", key, down: true });
    }
    act();
    for (const key of held.reverse()) {
      send({ type: "key", key, down: false });
      setLatched(key, false);
    }
  }

  // Presses `keys` in order, then releases them in reverse.
  function tap(keys) {
    for (const key of keys) {
      send({ type: "key", key, down: true });
    }
    for (const key of [...keys].reverse()) {
      send({ type: "key", key, down: false });
    }
  }

  function click(button) {
    send({ type: "button", button, down: true });
    send({ type: "button", button, down: false });
  }

  // Pressing a key or modifier leaves the focus where it was, so the
  // phone's keyboard stays open while typing.
  for (const button of remote.querySelectorAll("[data-keys], [data-modifier]")) {
    button.addEventListener("pointerdown", (event) => event.preventDefault());
  }

  for (const button of remote.querySelectorAll("[data-keys]")) {
    button.addEventListener("click", () =>
      withModifiers(() => tap(button.dataset.keys.split(" "))),
    );
  }

  for (const button of remote.querySelectorAll("[data-modifier]")) {
    const key = button.dataset.modifier;
    button.addEventListener("click", () => setLatched(key, !latched.has(key)));
  }

  // Mouse buttons are held for as long as they are pressed, so one can be
  // held while another finger moves the pointer.
  for (const button of remote.querySelectorAll("[data-button]")) {
    const name = button.dataset.button;
    let down = false;
    const release = () => {
      if (down) {
        down = false;
        send({ type: "button", button: name, down: false });
      }
    };
    button.addEventListener("pointerdown", (event) => {
      event.preventDefault();
      button.setPointerCapture(event.pointerId);
      if (latched.size > 0) {
        withModifiers(() => click(name));
        return;
      }
      down = true;
      send({ type: "button", button: name, down: true });
    });
    button.addEventListener("pointerup", release);
    button.addEventListener("pointercancel", release);
    button.addEventListener("contextmenu", (event) => event.preventDefault());
  }

  // Movement not sent yet, sent once per frame.
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

  // The touchpad. One finger moves the pointer and two scroll, moving the
  // view with the fingers, as on a phone. A tap clicks, once it is clear it
  // isn't the first half of a double tap; a touch that starts just after a
  // tap and moves drags.
  let gesture = null;
  // A tap's click, waiting to see whether a second touch follows.
  let waiting = null;

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
      };
    }
    gesture.fingers = Math.max(gesture.fingers, touches.length);
    gesture.at = centre(touches);
    gesture.count = touches.length;
    if (gesture.fingers > 1 && gesture.owed) {
      gesture.owed = false;
      withModifiers(() => click("left"));
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
    if (touches.length === 1) {
      if (gesture.owed && !gesture.dragging && gesture.travelled > SLOP) {
        gesture.owed = false;
        gesture.dragging = true;
        send({ type: "button", button: "left", down: true });
      }
      queue("moveX", dx * SPEED);
      queue("moveY", dy * SPEED);
    } else {
      queue("scrollX", -dx * SCROLL);
      queue("scrollY", -dy * SCROLL);
    }
  });

  function touchEnd(event) {
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
      send({ type: "button", button: "left", down: false });
      return;
    }
    const isTap = performance.now() - ended.start < TAP_MS && ended.travelled < SLOP;
    if (ended.owed) {
      withModifiers(() => click("left"));
    }
    if (!isTap) {
      return;
    }
    if (ended.fingers > 1) {
      withModifiers(() => click("right"));
    } else if (ended.owed) {
      // The second tap of a double tap.
      withModifiers(() => click("left"));
    } else {
      waiting = setTimeout(() => {
        waiting = null;
        withModifiers(() => click("left"));
      }, DOUBLE_MS);
    }
  }

  pad.addEventListener("touchend", touchEnd);
  pad.addEventListener("touchcancel", touchEnd);

  // Typing. What the field holds is compared with what it held, so that
  // whatever the phone's keyboard does, such as replacing a word it
  // corrected, is sent as the backspaces and characters that do the same.
  // The US layout the daemon types with has only printable ASCII.
  const typeable = (c) => c === "\n" || c === "\t" || (c >= " " && c <= "~");
  let typed = [];

  typing.addEventListener("input", () => {
    const chars = Array.from(typing.value);
    const kept = chars.filter(typeable);
    if (kept.length !== chars.length) {
      const dropped = [...new Set(chars.filter((c) => !typeable(c)))].join(" ");
      notice(`Can't type ${dropped}: Remote Input types through a US keyboard layout.`);
      typing.value = kept.join("");
    }
    let same = 0;
    while (same < kept.length && same < typed.length && kept[same] === typed[same]) {
      same += 1;
    }
    const erase = typed.length - same;
    const text = kept.slice(same).join("");
    if (erase > 0 || text) {
      withModifiers(() => {
        for (let i = 0; i < erase; i += 1) {
          tap(["backspace"]);
        }
        if (text) {
          send({ type: "text", text });
        }
      });
    }
    typed = kept;
    // A finished line starts a new one, which keeps the field short.
    if (text.endsWith("\n")) {
      typing.value = "";
      typed = [];
    }
  });

  // An empty field has nothing to delete, so Backspace makes no input.
  typing.addEventListener("keydown", (event) => {
    if (event.key === "Backspace" && typing.value === "") {
      event.preventDefault();
      withModifiers(() => tap(["backspace"]));
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
