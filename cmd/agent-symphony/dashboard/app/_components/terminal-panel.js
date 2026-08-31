"use client";

import { useEffect, useRef, useState } from "react";

export default function TerminalPanel({ config, onClose }) {
  const container = useRef(null);
  const closeButton = useRef(null);
  const panel = useRef(null);
  const opener = useRef(null);
  const [connection, setConnection] = useState("Connecting…");

  useEffect(() => {
    let disposed = false;
    let socket;
    let terminal;
    let resizeObserver;
    let input;

    async function connect() {
      const [{ Terminal }, { FitAddon }] = await Promise.all([
        import("@xterm/xterm"),
        import("@xterm/addon-fit"),
      ]);
      if (disposed || !container.current) return;
      terminal = new Terminal({
        convertEol: true,
        cursorBlink: !config.readOnly,
        disableStdin: Boolean(config.readOnly),
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
        fontSize: 14,
        screenReaderMode: true,
        theme: { background: "#101719", foreground: "#e8efed", cursor: "#69c4b2" },
      });
      const fit = new FitAddon();
      terminal.loadAddon(fit);
      terminal.open(container.current);
      fit.fit();
      if (!config.readOnly) terminal.focus();

      const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
      socket = new WebSocket(`${scheme}//${window.location.host}${config.endpoint}`);
      socket.binaryType = "arraybuffer";
      const sendSize = () => {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify({ type: "resize", cols: terminal.cols, rows: terminal.rows }));
        }
      };
      socket.addEventListener("open", () => {
        setConnection(config.readOnly ? "Connected · read-only" : "Connected");
        sendSize();
      });
      socket.addEventListener("message", (event) => terminal.write(new Uint8Array(event.data)));
      socket.addEventListener("close", (event) => {
        if (disposed) return;
        const message = event.reason || "Terminal disconnected.";
        setConnection(message);
        terminal.writeln(`\r\n\x1b[33m${message}\x1b[0m`);
      });
      if (!config.readOnly) {
        input = terminal.onData((data) => {
          if (socket.readyState === WebSocket.OPEN) socket.send(new TextEncoder().encode(data));
        });
      }
      resizeObserver = new ResizeObserver(() => {
        fit.fit();
        sendSize();
      });
      resizeObserver.observe(container.current);
    }

    connect().catch(() => {
      setConnection("Terminal failed to load.");
      if (container.current) container.current.textContent = "Terminal failed to load.";
    });
    opener.current = document.activeElement;
    closeButton.current?.focus();
    const keyboard = (event) => {
      if (event.key === "Escape") {
        if (container.current?.contains(event.target)) return;
        event.preventDefault();
        event.stopPropagation();
        onClose();
        return;
      }
      if (event.key !== "Tab" || !panel.current) return;
      const focusable = [...panel.current.querySelectorAll("button, [href], input, textarea, [tabindex]:not([tabindex='-1'])")]
        .filter((element) => !element.disabled && element.getAttribute("aria-hidden") !== "true");
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (!panel.current.contains(document.activeElement)) {
        event.preventDefault();
        first.focus();
      } else if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    window.addEventListener("keydown", keyboard, true);
    document.body.classList.add("terminalOpen");
    return () => {
      disposed = true;
      window.removeEventListener("keydown", keyboard, true);
      document.body.classList.remove("terminalOpen");
      resizeObserver?.disconnect();
      input?.dispose();
      socket?.close();
      terminal?.dispose();
      opener.current?.focus();
    };
  }, [config.endpoint, config.readOnly, onClose]);

  return (
    <div className="terminalBackdrop" role="dialog" aria-modal="true" aria-labelledby="terminalTitle" aria-describedby="terminalConnection">
      <section className="terminalPanel" ref={panel}>
        <header>
          <div>
            <p className="eyebrow">{config.eyebrow}</p>
            <h2 id="terminalTitle">{config.title}</h2>
            <p className="terminalConnection" id="terminalConnection" role="status" aria-live="polite">{connection}</p>
          </div>
          <button ref={closeButton} type="button" onClick={onClose}>Close</button>
        </header>
        <div className="terminal" ref={container} aria-label={`${config.readOnly ? "Read-only terminal" : "Terminal"} for ${config.title}`} />
      </section>
    </div>
  );
}
