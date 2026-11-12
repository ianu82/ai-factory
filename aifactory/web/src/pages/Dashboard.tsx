import { useEffect, useRef, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate, useSearchParams } from "react-router-dom";
import { api } from "../api";
import { useWs } from "../App";
import { money } from "../format";
import { Empty, Skeleton } from "../ui";
import {
  asPeriod, bucketLabel, change, hours, niceMax, noPerson, perDay, percent, PERIODS, phaseShares, ratio, roles,
  sparkline, tickIndexes, type Bucket, type Direction, type Figures, type Period, type Series,
} from "../dashboard";

// Categorical slots in their validated order; gray is "unknown or other", never a category of its own.
const SLOT = ["var(--viz-1)", "var(--viz-2)", "var(--viz-3)", "var(--viz-4)", "var(--viz-5)", "var(--viz-6)"];
const OTHER = "var(--viz-other)";
// Text set inside a filled block: white, except near-black on yellow.
const INK_ON = ["#fff", "#fff", "#fff", "#0b0b0b", "#fff", "#fff"];
// One color per party across the page: the Factory's agents, people, the Director.
const AGENTS = SLOT[0], PEOPLE = SLOT[1], DIRECTOR = SLOT[2];
const NOUN: Record<Period, string> = { day: "day", week: "week", month: "month" };

export function DashboardPage() {
  const { ws } = useWs();
  const [params, setParams] = useSearchParams();
  const period = asPeriod(params.get("period"));
  const q = useQuery({
    queryKey: ["dashboard", ws.id, period],
    queryFn: () => api<Series>(`/api/workspaces/${ws.id}/dashboard?period=${period}`),
    staleTime: 60_000, refetchInterval: 300_000,
  });
  const d = q.data;
  return (
    <>
      <div className="page-head">
        <h1>Dashboard</h1>
        <span className="muted hide-sm" style={{ fontSize: 12.5 }}>Is the Factory delivering better and faster?</span>
        <span className="spacer" />
        <div className="seg" role="radiogroup" aria-label="Period">
          {PERIODS.map((p) => (
            <button key={p.value} role="radio" aria-checked={p.value === period} className={p.value === period ? "on" : ""}
              onClick={() => setParams((prev) => { const next = new URLSearchParams(prev); next.set("period", p.value); return next; }, { replace: true })}>
              {p.label}
            </button>
          ))}
        </div>
      </div>
      <div className="page-body">
        {q.isLoading ? <Skeleton /> : q.isError || !d ? (
          <Empty title="Could not load the dashboard">{(q.error as Error)?.message || "Try again in a moment."}</Empty>
        ) : <Board data={d} />}
      </div>
    </>
  );
}

function Board({ data }: { data: Series }) {
  const { period, buckets, range, definitions: def } = data;
  const n = buckets.length;
  const full = buckets[n - 1]?.partial ? buckets.slice(0, -1) : buckets;
  const last = full[full.length - 1];
  const before = full[full.length - 2];
  const now = buckets[n - 1]?.partial ? buckets[n - 1] : undefined;
  const latest = last ? bucketLabel(last.start, period, true) : "";
  const card = (value: (b: Figures) => number | null) => ({
    values: buckets.map(value), now: last ? value(last) : null, before: before ? value(before) : null,
    soFar: now ? value(now) : null,
  });
  const delivered = card((b) => b.tasks_delivered);
  const deploys = card((b) => perDay(b.merges, b.days));
  const cycle = card((b) => b.cycle_time.median_hours);
  const unit = card((b) => b.cost_per_delivered_task.usd);
  const alone = card((b) => noPerson(b.autonomy).share);
  const failure = card((b) => ratio(b.change_failure.failed, b.change_failure.settled));
  const costKnown = (b?: Figures) => b?.cost_per_delivered_task.known ?? true;
  const usd = (v: number | null, known = true) => (v === null ? "—" : money(v, known));
  const undated = Object.entries(data.undated).filter(([, v]) => v > 0);
  return (
    <div className="dash">
      <p className="dash-note">
        Latest full {NOUN[period]}: <b>{latest || "none yet"}</b>, compared with the {NOUN[period]} before.
        {now && <> The current {NOUN[period]} is still running and shows as “so far”.</>} Times are UTC.
      </p>
      <div className="dash-cards">
        <Stat label="Tasks delivered" title={def.tasks_delivered} m={delivered} better="up" fmt={(v) => String(Math.round(v))}
          sub={last && `${last.prs_opened} PRs opened · ${last.prs_opened_merged} merged since`} period={period} />
        <Stat label="Deploy frequency" title={def.merges} m={deploys} better="up" fmt={(v) => `${v.toFixed(v < 10 ? 1 : 0)}/day`}
          sub={last && `${last.merges} merges`} period={period} />
        <Stat label="Cycle time" title={def.cycle_time} m={cycle} better="down" fmt={hours}
          sub={last && `task start → merge · ${last.cycle_time.sample_count} merged`} period={period} />
        <Stat label="Cost per delivered task" title={def.cost_per_delivered_task} m={unit} better="down"
          fmt={(v) => money(v, true)} show={(v) => usd(v, costKnown(last))} showSoFar={(v) => usd(v, costKnown(now))}
          sub={last && `${money(last.cost.usd, last.cost.known)} spent`} period={period} />
        <Stat label="No person needed" title={def.autonomy} m={alone} better="up" fmt={percent} show={percent}
          sub={last && `agents alone ${percent(noPerson(last.autonomy).alone)} · Director stood in on ${last.autonomy.director}`}
          period={period} />
        <Stat label="Change failure rate" title={def.change_failure} m={failure} better="down" fmt={percent} show={percent}
          sub={last && `${last.change_failure.failed} of ${last.change_failure.settled} settled merges${last.change_failure.pending ? ` · ${last.change_failure.pending} pending` : ""}`}
          period={period} />
      </div>

      <Panel title="Cycle time, phase by phase" note={def.stages}
        aside={<span><b className="dash-big">{hours(range.cycle_time.median_hours)}</b> median, task start to merge · {range.cycle_time.sample_count} merged this range</span>}>
        <Phases figures={range} />
      </Panel>

      <div className="dash-grid">
        <Panel title="Pull requests opened" note={def.prs_opened}>
          <Columns period={period} buckets={buckets} series={[
            { label: "Merged since", color: AGENTS, values: buckets.map((b) => b.prs_opened_merged) },
            { label: "Not merged", color: OTHER, values: buckets.map((b) => b.prs_opened - b.prs_opened_merged) },
          ]} extra={(b) => [`${b.merges} merges · ${b.tasks_delivered} tasks delivered`]} />
        </Panel>
        <Panel title="Who delivered" note={def.autonomy}>
          <Columns period={period} buckets={buckets} series={[
            { label: "Agents alone", color: AGENTS, values: buckets.map((b) => b.autonomy.unattended) },
            { label: "Director stood in", color: DIRECTOR, values: buckets.map((b) => b.autonomy.director) },
            { label: "A person helped", color: PEOPLE, values: buckets.map((b) => b.autonomy.assisted) },
            { label: "Unverified", color: OTHER, values: buckets.map((b) => b.autonomy.unverified) },
          ]} unit="outcomes" />
        </Panel>
        <Panel title="Decisions, by who resolved them" note={def.decisions}>
          <Columns period={period} buckets={buckets} series={[
            { label: "Lead", color: AGENTS, values: buckets.map((b) => b.decisions.lead) },
            { label: "Director", color: DIRECTOR, values: buckets.map((b) => b.decisions.director) },
            { label: "People", color: PEOPLE, values: buckets.map((b) => b.decisions.person) },
            { label: "Operator & other", color: OTHER, values: buckets.map((b) => b.decisions.operator + b.decisions.factory) },
          ]} extra={(b) => [`${b.decisions.asked} asked`, `Guidance: ${b.guidance.person} from people (interventions) · ${b.guidance.director} Director · ${b.guidance.lead} Lead`]} />
        </Panel>
        <Panel title="Cost per delivered task" note={`${def.cost_per_delivered_task} ≥ marks a lower bound: some usage has no known cost.`}>
          <Columns period={period} buckets={buckets} money series={[
            { label: "Cost per delivered task", color: AGENTS, values: buckets.map((b) => b.cost_per_delivered_task.usd) },
          ]} lowerBound={buckets.map((b) => !b.cost_per_delivered_task.known && b.cost_per_delivered_task.usd !== null)}
            extra={(b) => [`${money(b.cost.usd, b.cost.known)} spent · ${b.tasks_delivered} delivered`,
              `Median outcome: ${usd(b.outcome_cost.median_usd, b.outcome_cost.known)} (${b.outcome_cost.sample_count} completed)`]} />
        </Panel>
        <Panel title="Leading indicators" note="Over the whole range, with each period's trend.">
          <Indicators buckets={buckets} range={range} def={def} />
        </Panel>
        <Panel title="Costliest outcomes" note={`Completed in this range, by total cost. ${def.outcome_cost}`}>
          <Costliest data={data} />
        </Panel>
        <Panel title="Runs by role" note="Agent sessions started in each period.">
          <Roles buckets={buckets} range={range} />
        </Panel>
      </div>

      {undated.length > 0 && (
        <p className="dash-note">Not in any period because the history has no date for them: {undated.map(([k, v]) => `${v} ${k.replace(/_/g, " ")}`).join(", ")}.</p>
      )}
      <details className="dash-table">
        <summary>Every figure, by {NOUN[period]}</summary>
        <FiguresTable period={period} buckets={buckets} />
      </details>
    </div>
  );
}

type Measure = { values: (number | null)[]; now: number | null; before: number | null; soFar: number | null };

function Stat({ label, title, m, better, fmt, show, showSoFar, sub, period }: {
  label: string; title?: string; m: Measure; better: Direction; fmt: (v: number) => string;
  show?: (v: number | null) => string; showSoFar?: (v: number | null) => string; sub?: ReactNode; period: Period;
}) {
  const display = show || ((v: number | null) => (v === null ? "—" : fmt(v)));
  const delta = change(m.now, m.before, better, fmt);
  const path = sparkline(m.values, 120, 28);
  return (
    <div className="dash-card" title={title}>
      <div className="dash-card-label">{label}</div>
      <div className="dash-card-value">{display(m.now)}</div>
      <div className="dash-card-foot">
        {delta && <span className={`dash-delta ${delta.tone}`}>{delta.text}</span>}
        {path && <svg width="120" height="28" viewBox="0 0 120 28" aria-hidden className="dash-spark"><path d={path} /></svg>}
      </div>
      {sub && <div className="dash-card-sub">{sub}</div>}
      {m.soFar !== null && <div className="dash-card-sub">This {NOUN[period]} so far: {(showSoFar || display)(m.soFar)}</div>}
    </div>
  );
}

function Panel({ title, note, aside, children }: { title: string; note?: string; aside?: ReactNode; children: ReactNode }) {
  return (
    <section className="dash-panel">
      <header>
        <h2 title={note}>{title}</h2>
        {aside && <div className="dash-aside">{aside}</div>}
      </header>
      {children}
      {note && <p className="dash-def">{note}</p>}
    </section>
  );
}

function Phases({ figures }: { figures: Figures }) {
  const parts = phaseShares(figures.stages);
  const width = useWidth();
  if (!parts.some((p) => p.share > 0)) return <p className="muted" style={{ fontSize: 12.5 }}>No merged task in this range has a dated stage yet.</p>;
  return (
    <div>
      <div className="dash-phases" ref={width.ref} role="img"
        aria-label={parts.map((p) => `${p.label} ${hours(p.hours)}`).join(", ")}>
        {parts.filter((p) => p.share > 0).map((p) => {
          const color = SLOT[parts.indexOf(p)];
          const room = width.px * p.share;
          return (
            <div key={p.key} className="dash-phase" style={{ flexGrow: p.share, background: color, color: INK_ON[parts.indexOf(p)] }}
              title={`${p.label}: ${hours(p.hours)} median (${figures.stages[p.key].sample_count} tasks)`}>
              {room > p.label.length * 7 + 40 && <span>{p.label} · {hours(p.hours)}</span>}
            </div>
          );
        })}
      </div>
      <div className="dash-phase-legend">
        {parts.map((p, i) => (
          <div key={p.key}>
            <span className="dash-key" style={{ background: SLOT[i] }} />
            <span className="muted">{p.label}</span>
            <b>{hours(p.hours)}</b>
            <span className="faint">{figures.stages[p.key].sample_count} tasks</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function useWidth() {
  const ref = useRef<HTMLDivElement | null>(null);
  const [px, setPx] = useState(600);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new ResizeObserver(([entry]) => setPx(entry.contentRect.width));
    observer.observe(el);
    setPx(el.getBoundingClientRect().width);
    return () => observer.disconnect();
  }, []);
  return { ref, px };
}

// A null value is undefined for that period (a cost per task with no task delivered): it draws nothing and reads "—".
type ColumnSeries = { label: string; color: string; values: (number | null)[] };

function Columns({ period, buckets, series, extra, unit, money: isMoney, lowerBound }: {
  period: Period; buckets: Bucket[]; series: ColumnSeries[]; extra?: (b: Bucket) => string[]; unit?: string;
  money?: boolean; lowerBound?: boolean[];
}) {
  const box = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const H = 150, top = 12, bottom = 20, left = 40, right = 6;
  const W = Math.max(240, box.px);
  const plotW = W - left - right, plotH = H - top - bottom;
  const totals = buckets.map((_, i) => series.reduce((s, x) => s + (x.values[i] || 0), 0));
  const max = niceMax(Math.max(...totals, 0));
  const band = plotW / Math.max(1, buckets.length);
  const barW = Math.max(3, Math.min(24, band * 0.62));
  const y = (v: number) => top + plotH - (v / max) * plotH;
  const fmt = (v: number) => (isMoney ? money(v) : Number.isInteger(v) ? String(v) : v.toFixed(1));
  const show = (s: ColumnSeries, i: number) => {
    const v = s.values[i] ?? null;
    return v === null ? "—" : `${lowerBound?.[i] ? "≥" : ""}${fmt(v)}`;
  };
  const ticks = tickIndexes(buckets.length, Math.max(2, Math.floor(plotW / 64)));
  const tip = hover === null ? null : buckets[hover];
  return (
    <div className="dash-chart" ref={box.ref}>
      {series.length > 1 && (
        <div className="dash-legend">
          {series.map((s) => <span key={s.label}><i style={{ background: s.color }} />{s.label}</span>)}
        </div>
      )}
      <svg width={W} height={H} role="img" aria-label={`${series.map((s) => s.label).join(", ")} by ${NOUN[period]}`}>
        {[0, max / 2, max].map((v) => (
          <g key={v}>
            <line x1={left} x2={W - right} y1={y(v)} y2={y(v)} className={v === 0 ? "dash-base" : "dash-grid-line"} />
            <text x={left - 6} y={y(v) + 3.5} textAnchor="end" className="dash-tick">{fmt(v)}</text>
          </g>
        ))}
        {buckets.map((b, i) => {
          const cx = left + band * i + band / 2;
          let base = top + plotH;
          const visible = series.map((s) => s.values[i] || 0);
          const topIndex = visible.reduce((t, v, k) => (v > 0 ? k : t), -1);
          return (
            <g key={b.start} opacity={b.partial ? 0.55 : 1}>
              {series.map((s, k) => {
                const v = visible[k];
                if (v <= 0) return null;
                const h = (v / max) * plotH;
                const gap = k === topIndex ? 0 : 2;
                const yTop = base - h;
                const shape = k === topIndex
                  ? <path key={s.label} d={column(cx - barW / 2, yTop, barW, h, Math.min(4, h, barW / 2))} fill={s.color} />
                  : <rect key={s.label} x={cx - barW / 2} y={yTop + gap} width={barW} height={Math.max(0, h - gap)} fill={s.color} />;
                base = yTop;
                return shape;
              })}
              {lowerBound?.[i] && totals[i] > 0 && <text x={cx} y={y(totals[i]) - 3} textAnchor="middle" className="dash-tick">≥</text>}
              {ticks.includes(i) && <text x={cx} y={H - 5} textAnchor="middle" className="dash-tick">{bucketLabel(b.start, period)}</text>}
              <rect x={left + band * i} y={top} width={band} height={plotH} fill="transparent" tabIndex={0}
                aria-label={`${bucketLabel(b.start, period, true)}: ${series.map((s) => `${s.label} ${show(s, i)}`).join(", ")}`}
                onMouseEnter={() => setHover(i)} onMouseLeave={() => setHover(null)} onFocus={() => setHover(i)} onBlur={() => setHover(null)} />
            </g>
          );
        })}
        {hover !== null && <rect x={left + band * hover} y={top} width={band} height={plotH} className="dash-hover" pointerEvents="none" />}
      </svg>
      {tip && hover !== null && (
        <div className="dash-tip" style={{ left: Math.min(W - 230, Math.max(0, left + band * hover + band / 2 - 110)) }}>
          <div className="dash-tip-head">{bucketLabel(tip.start, period, true)}{tip.partial ? " · so far" : ""}</div>
          {series.map((s) => (
            <div key={s.label} className="dash-tip-row">
              <i style={{ background: s.color }} /><b>{show(s, hover)}</b><span>{s.label}{unit ? ` ${unit}` : ""}</span>
            </div>
          ))}
          {extra?.(tip).map((line) => <div key={line} className="dash-tip-extra">{line}</div>)}
        </div>
      )}
    </div>
  );
}

/** A column with rounded top corners and a square foot on the baseline. */
function column(x: number, y: number, w: number, h: number, r: number): string {
  return `M${x} ${y + h}V${y + r}Q${x} ${y} ${x + r} ${y}H${x + w - r}Q${x + w} ${y} ${x + w} ${y + r}V${y + h}Z`;
}

function Indicators({ buckets, range, def }: { buckets: Bucket[]; range: Figures; def: Record<string, string> }) {
  const rows: { label: string; title?: string; value: string; values: (number | null)[] }[] = [
    { label: "Merged on first attempt", title: def.first_attempt,
      value: percent(ratio(range.first_attempt.passed, range.first_attempt.merged)),
      values: buckets.map((b) => ratio(b.first_attempt.passed, b.first_attempt.merged)) },
    { label: "People's decisions per delivered task", title: def.decisions,
      value: fixed(ratio(range.decisions.person, range.tasks_delivered)),
      values: buckets.map((b) => ratio(b.decisions.person, b.tasks_delivered)) },
    { label: "Interventions per delivered task", title: def.guidance,
      value: fixed(ratio(range.guidance.person, range.tasks_delivered)),
      values: buckets.map((b) => ratio(b.guidance.person, b.tasks_delivered)) },
    { label: "Outcome lead time", title: def.lead_time, value: hours(range.lead_time.median_hours),
      values: buckets.map((b) => b.lead_time.median_hours) },
    { label: "Median cost per outcome", title: def.outcome_cost,
      value: range.outcome_cost.median_usd === null ? "—" : money(range.outcome_cost.median_usd, range.outcome_cost.known),
      values: buckets.map((b) => b.outcome_cost.median_usd) },
  ];
  return <Rows rows={rows} />;
}

function fixed(v: number | null): string {
  return v === null ? "—" : v.toFixed(v < 10 ? 1 : 0);
}

function Rows({ rows }: { rows: { label: string; title?: string; value: string; values: (number | null)[] }[] }) {
  return (
    <div className="dash-rows">
      {rows.map((r) => (
        <div key={r.label} className="dash-row" title={r.title}>
          <span>{r.label}</span>
          <svg width="96" height="22" viewBox="0 0 96 22" aria-hidden className="dash-spark"><path d={sparkline(r.values, 96, 22)} /></svg>
          <b>{r.value}</b>
        </div>
      ))}
    </div>
  );
}

function Roles({ buckets, range }: { buckets: Bucket[]; range: Figures }) {
  const names = roles(range.runs.by_role);
  if (!names.length) return <p className="muted" style={{ fontSize: 12.5 }}>No runs started in this range.</p>;
  return <Rows rows={names.map((role) => ({
    label: role.replace(/_/g, " "), value: String(range.runs.by_role[role]),
    values: buckets.map((b) => b.runs.by_role[role] || 0),
  }))} />;
}

function Costliest({ data }: { data: Series }) {
  const { ws } = useWs();
  const navigate = useNavigate();
  if (!data.expensive_outcomes.length) return <p className="muted" style={{ fontSize: 12.5 }}>No outcome completed in this range.</p>;
  return (
    <table className="table dash-costly">
      <tbody>
        {data.expensive_outcomes.map((o) => (
          <tr key={o.id} onClick={() => navigate(`/w/${ws.id}/outcomes/${o.id}`)} style={{ cursor: "pointer" }}>
            <td style={{ maxWidth: 260, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              <span className="ref">#{o.number}</span> {o.title}
            </td>
            <td className="r num muted">{o.tasks_delivered} {o.tasks_delivered === 1 ? "task" : "tasks"}</td>
            <td className="r num"><b>{money(o.cost_usd, o.cost_known)}</b></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function FiguresTable({ period, buckets }: { period: Period; buckets: Bucket[] }) {
  const head = ["Period", "Delivered", "PRs opened", "Merged since", "Merges", "Cycle time", "Cost", "Per task",
    "Agents alone", "Director", "Person", "Unverified", "Failed / settled", "First attempt", "People's decisions",
    "Director's decisions", "Interventions"];
  return (
    <div style={{ overflowX: "auto" }}>
      <table className="table">
        <thead><tr>{head.map((h, i) => <th key={h} className={i ? "r" : ""}>{h}</th>)}</tr></thead>
        <tbody>
          {[...buckets].reverse().map((b) => (
            <tr key={b.start}>
              <td>{bucketLabel(b.start, period, true)}{b.partial ? " (so far)" : ""}</td>
              <td className="r num">{b.tasks_delivered}</td>
              <td className="r num">{b.prs_opened}</td>
              <td className="r num">{b.prs_opened_merged}</td>
              <td className="r num">{b.merges}</td>
              <td className="r num">{hours(b.cycle_time.median_hours)}</td>
              <td className="r num">{money(b.cost.usd, b.cost.known)}</td>
              <td className="r num">{b.cost_per_delivered_task.usd === null ? "—" : money(b.cost_per_delivered_task.usd, b.cost_per_delivered_task.known)}</td>
              <td className="r num">{b.autonomy.unattended}</td>
              <td className="r num">{b.autonomy.director}</td>
              <td className="r num">{b.autonomy.assisted}</td>
              <td className="r num">{b.autonomy.unverified}</td>
              <td className="r num">{b.change_failure.failed} / {b.change_failure.settled}</td>
              <td className="r num">{b.first_attempt.passed} / {b.first_attempt.merged}</td>
              <td className="r num">{b.decisions.person}</td>
              <td className="r num">{b.decisions.director}</td>
              <td className="r num">{b.guidance.person}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
