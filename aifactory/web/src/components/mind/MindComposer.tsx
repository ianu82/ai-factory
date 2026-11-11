import { forwardRef, useImperativeHandle, useLayoutEffect, useRef, useState } from "react";
import { ArrowUp, Square } from "lucide-react";

export type ComposerHandle = { focus: () => void; fill: (text: string) => void };

/**
 * Where the person talks to Mind. Grows with the text up to a limit, Enter sends and Shift+Enter adds a line.
 * While a reply is running the send button becomes Stop. A disabled composer says why, instead of failing on send.
 */
export const MindComposer = forwardRef<ComposerHandle, {
  onSend: (text: string) => void; onStop?: () => void; running?: boolean; disabled?: boolean; disabledReason?: string;
  placeholder: string; large?: boolean; autoFocus?: boolean;
}>(function MindComposer({ onSend, onStop, running = false, disabled = false, disabledReason, placeholder, large = false, autoFocus = false }, ref) {
  const [text, setText] = useState("");
  const area = useRef<HTMLTextAreaElement>(null);
  useImperativeHandle(ref, () => ({ focus: () => area.current?.focus(), fill: (value: string) => { setText(value); requestAnimationFrame(() => area.current?.focus()); } }), []);
  useLayoutEffect(() => {
    const el = area.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, large ? 260 : 200)}px`;
  }, [text, large]);
  const canSend = !disabled && !running && text.trim().length > 0;
  const send = () => { if (!canSend) return; onSend(text.trim()); setText(""); };
  return <form className={`mind-composer${large ? " large" : ""}${disabled ? " disabled" : ""}`} onSubmit={(e) => { e.preventDefault(); send(); }}>
    <textarea ref={area} value={text} rows={large ? 3 : 1} placeholder={placeholder} aria-label="Message Mind" autoFocus={autoFocus}
      maxLength={8000} disabled={disabled} onChange={(e) => setText(e.target.value)}
      onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) { e.preventDefault(); send(); } }} />
    <div className="mind-composer-bar">
      <span className="mind-composer-hint">{disabled && disabledReason ? disabledReason : running ? "Mind is working…" : "Enter to send · Shift+Enter for a new line"}</span>
      {running && onStop
        ? <button type="button" className="mind-send stop" onClick={onStop} aria-label="Stop"><Square /></button>
        : <button type="submit" className="mind-send" disabled={!canSend} aria-label="Send"><ArrowUp /></button>}
    </div>
  </form>;
});
