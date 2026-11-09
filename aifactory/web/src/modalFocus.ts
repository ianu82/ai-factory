/** Enter the dialog on mount, contain keyboard focus, then restore its trigger. */
export function containModalFocus(dialog: HTMLElement, onClose: () => void, isActive: () => boolean = () => true): () => void {
  const doc = dialog.ownerDocument;
  const previous = doc.activeElement as HTMLElement | null;
  const controls = () => Array.from(dialog.querySelectorAll<HTMLElement>(
    'button:not(:disabled), input:not(:disabled), textarea:not(:disabled), select:not(:disabled), a[href], [tabindex]:not([tabindex="-1"])',
  )).filter((element) => element.tabIndex >= 0 && !element.closest('[hidden], [aria-hidden="true"]'));
  const first = () => controls()[0] || dialog;
  // Child forms may already have selected their preferred initial control.
  if (isActive() && !dialog.contains(doc.activeElement)) first().focus();
  const onKey = (event: KeyboardEvent) => {
    // A persistent sheet must yield keyboard and focus ownership to other modals.
    if (event.defaultPrevented || !isActive()) return;
    if (event.key === "Escape") { event.preventDefault(); event.stopPropagation(); onClose(); }
    if (event.key !== "Tab") return;
    const items = controls();
    const active = doc.activeElement;
    if (!items.length) { event.preventDefault(); dialog.focus(); return; }
    if (!dialog.contains(active) || active === dialog || !items.includes(active as HTMLElement)) {
      event.preventDefault();
      (event.shiftKey ? items[items.length - 1] : items[0]).focus();
    } else if (event.shiftKey && active === items[0]) {
      event.preventDefault(); items[items.length - 1].focus();
    } else if (!event.shiftKey && active === items[items.length - 1]) {
      event.preventDefault(); items[0].focus();
    }
  };
  const onFocus = (event: FocusEvent) => {
    if (isActive() && !dialog.contains(event.target as Node)) first().focus();
  };
  doc.addEventListener("keydown", onKey);
  doc.addEventListener("focusin", onFocus);
  return () => {
    doc.removeEventListener("keydown", onKey);
    doc.removeEventListener("focusin", onFocus);
    if (isActive() && previous?.isConnected) previous.focus();
  };
}
