import { Fragment, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { mindLink } from "./mind";

/**
 * A small, safe Markdown renderer for Mind replies: React elements only, never HTML strings, so model text can
 * not inject markup. Covers what a delivery lead writes: paragraphs, headings, bullet and numbered lists (one
 * level of nesting), quotes, fenced code, tables, bold, italic, inline code and links. Links become router links
 * only for trusted in-app paths; other http(s) links open in a new tab; anything else stays text.
 */
export function Markdown({ text, className = "" }: { text: string; className?: string }) {
  return <div className={`md ${className}`}>{blocks(text.replace(/\r\n?/g, "\n"))}</div>;
}

type ListItem = { text: string; children: ListItem[] };

function blocks(source: string): ReactNode[] {
  const lines = source.split("\n");
  const out: ReactNode[] = [];
  let i = 0;
  const key = () => `b${out.length}`;
  while (i < lines.length) {
    const line = lines[i];
    if (!line.trim()) { i++; continue; }
    const fence = line.match(/^\s*(```|~~~)\s*([\w+-]*)\s*$/);
    if (fence) {
      const body: string[] = [];
      i++;
      while (i < lines.length && !lines[i].trim().startsWith(fence[1])) body.push(lines[i++]);
      i++;
      out.push(<pre key={key()} className="md-code" data-lang={fence[2] || undefined}><code>{body.join("\n")}</code></pre>);
      continue;
    }
    const heading = line.match(/^(#{1,4})\s+(.*)$/);
    if (heading) {
      const level = Math.min(heading[1].length + 2, 6);
      const Tag = `h${level}` as "h3" | "h4" | "h5" | "h6";
      out.push(<Tag key={key()}>{inline(heading[2])}</Tag>);
      i++;
      continue;
    }
    if (/^\s*([-*_])(\s*\1){2,}\s*$/.test(line)) { out.push(<hr key={key()} />); i++; continue; }
    if (/^\s*>/.test(line)) {
      const quoted: string[] = [];
      while (i < lines.length && /^\s*>/.test(lines[i])) quoted.push(lines[i++].replace(/^\s*>\s?/, ""));
      out.push(<blockquote key={key()}>{blocks(quoted.join("\n"))}</blockquote>);
      continue;
    }
    if (isTableStart(lines, i)) {
      const rows: string[][] = [];
      const header = cells(lines[i]);
      i += 2;
      while (i < lines.length && /^\s*\|.*\|\s*$/.test(lines[i])) rows.push(cells(lines[i++]));
      out.push(
        <div key={key()} className="md-table"><table>
          <thead><tr>{header.map((c, n) => <th key={n}>{inline(c)}</th>)}</tr></thead>
          <tbody>{rows.map((r, n) => <tr key={n}>{header.map((_, m) => <td key={m}>{inline(r[m] ?? "")}</td>)}</tr>)}</tbody>
        </table></div>,
      );
      continue;
    }
    const bullet = /^(\s*)([-*+•]|\d+[.)])\s+(.*)$/;
    if (bullet.test(line)) {
      const ordered = /\d/.test(line.match(bullet)![2]);
      const items: ListItem[] = [];
      const baseIndent = line.match(bullet)![1].length;
      while (i < lines.length) {
        const m = lines[i].match(bullet);
        if (m) {
          const item = { text: m[3], children: [] as ListItem[] };
          if (m[1].length > baseIndent && items.length) items[items.length - 1].children.push(item);
          else items.push(item);
          i++;
        } else if (lines[i].trim() && /^\s{2,}\S/.test(lines[i]) && items.length) {
          const last = items[items.length - 1];
          const target = last.children.length ? last.children[last.children.length - 1] : last;
          target.text += " " + lines[i].trim();
          i++;
        } else break;
      }
      out.push(list(items, ordered, key()));
      continue;
    }
    const para: string[] = [];
    while (i < lines.length && lines[i].trim() && !startsBlock(lines, i)) para.push(lines[i++]);
    if (!para.length) para.push(lines[i++]);
    out.push(<p key={key()}>{para.map((p, n) => <Fragment key={n}>{n > 0 && <br />}{inline(p)}</Fragment>)}</p>);
  }
  return out;
}

function list(items: ListItem[], ordered: boolean, key: string): ReactNode {
  const Tag = ordered ? "ol" : "ul";
  return <Tag key={key}>{items.map((item, n) => (
    <li key={n}>{inline(item.text)}{item.children.length > 0 && list(item.children, false, `${key}-${n}`)}</li>
  ))}</Tag>;
}

function startsBlock(lines: string[], i: number): boolean {
  const line = lines[i];
  return /^\s*(```|~~~)/.test(line) || /^#{1,4}\s/.test(line) || /^\s*>/.test(line)
    || /^(\s*)([-*+•]|\d+[.)])\s+/.test(line) || isTableStart(lines, i);
}

function isTableStart(lines: string[], i: number): boolean {
  return /^\s*\|.*\|\s*$/.test(lines[i] || "") && /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$/.test(lines[i + 1] || "");
}

function cells(row: string): string[] {
  return row.trim().replace(/^\|/, "").replace(/\|$/, "").split("|").map((c) => c.trim());
}

const INLINE = /(`[^`\n]+`)|(\*\*[^*\n]+\*\*|__[^_\n]+__)|(\*[^*\s][^*\n]*\*|_[^_\s][^_\n]*_)|(\[[^\]\n]+\]\([^)\s]+\))/;

/** Inline spans, matched left to right; React escapes every text node. */
export function inline(text: string): ReactNode[] {
  const out: ReactNode[] = [];
  let rest = text;
  while (rest) {
    const m = rest.match(INLINE);
    if (!m || m.index === undefined) { out.push(rest); break; }
    if (m.index > 0) out.push(rest.slice(0, m.index));
    const token = m[0];
    const k = out.length;
    if (m[1]) out.push(<code key={k}>{token.slice(1, -1)}</code>);
    else if (m[2]) out.push(<strong key={k}>{inline(token.slice(2, -2))}</strong>);
    else if (m[3]) out.push(<em key={k}>{inline(token.slice(1, -1))}</em>);
    else {
      const [, label, href] = token.match(/^\[([^\]]+)\]\(([^)]+)\)$/)!;
      const kind = mindLink(href);
      out.push(kind === "internal" ? <Link key={k} to={href}>{inline(label)}</Link>
        : kind === "external" ? <a key={k} href={href} target="_blank" rel="noopener noreferrer">{inline(label)}</a>
        : <Fragment key={k}>{label}</Fragment>);
    }
    rest = rest.slice(m.index + token.length);
  }
  return out;
}
