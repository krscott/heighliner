// Keeps a page current: whenever the daemon says something changed, the
// page is fetched again and its main content swapped in. Without this
// script, pages refresh on a timer instead.
"use strict";
(() => {
  let fetching = false;
  let again = false;

  // Whether the user is selecting or typing in the page, which a swap would
  // undo; the page is updated at the next change after.
  function inUse(main) {
    const selection = document.getSelection();
    const selecting =
      selection && !selection.isCollapsed && main.contains(selection.anchorNode);
    return selecting || main.contains(document.activeElement);
  }

  async function update() {
    if (fetching) {
      again = true;
      return;
    }
    fetching = true;
    let shown = false;
    try {
      // A page that is gone answers with one saying so, which is shown too.
      const response = await fetch(location.href);
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      const next = page.querySelector("main");
      const main = document.querySelector("main");
      if (next && main) {
        shown = true;
        if (next.innerHTML !== main.innerHTML && !inUse(main)) {
          main.replaceWith(next);
          document.title = page.title;
        }
      }
    } catch {
      // Shown as lost below; the next change, or reconnecting, tries again.
    } finally {
      fetching = false;
    }
    document.body.classList.toggle("disconnected", !shown);
    if (again) {
      again = false;
      update();
    }
  }

  const events = new EventSource("/ui/events");
  let opened = false;
  events.addEventListener("changed", update);
  events.addEventListener("open", () => {
    // Catch up on what changed while disconnected.
    if (opened) {
      update();
    } else {
      document.body.classList.remove("disconnected");
    }
    opened = true;
  });
  events.addEventListener("error", () => document.body.classList.add("disconnected"));
})();
