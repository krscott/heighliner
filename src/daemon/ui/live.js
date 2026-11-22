// Keeps a page current: whenever the daemon says something changed, the
// page is fetched again and its main content swapped in. Without this
// script, pages refresh on a timer instead.
"use strict";
(() => {
  let fetching = false;
  let again = false;

  async function update() {
    if (fetching) {
      again = true;
      return;
    }
    fetching = true;
    try {
      // A page that is gone answers with one saying so, which is shown too.
      const response = await fetch(location.href);
      const page = new DOMParser().parseFromString(await response.text(), "text/html");
      const next = page.querySelector("main");
      const main = document.querySelector("main");
      if (next && main && next.innerHTML !== main.innerHTML) {
        main.replaceWith(next);
        document.title = page.title;
      }
    } catch {
      // The next change, or reconnecting, tries again.
    } finally {
      fetching = false;
    }
    if (again) {
      again = false;
      update();
    }
  }

  const events = new EventSource("/ui/events");
  let opened = false;
  events.addEventListener("changed", update);
  events.addEventListener("open", () => {
    document.body.classList.remove("disconnected");
    // Catch up on what changed while disconnected.
    if (opened) {
      update();
    }
    opened = true;
  });
  events.addEventListener("error", () => document.body.classList.add("disconnected"));
})();
