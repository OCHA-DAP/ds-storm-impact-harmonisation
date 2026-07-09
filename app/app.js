/* Storm exposure comparison — static app, no framework.
 * Data: app/data/* produced by export_app_data.py.
 * Forecast value = fcastonly + obsv accrued up to the issued time. */

"use strict";

const COL = {
  fc: "#1565C0", ob: "#2E7D32", gdacs: "#BF360C", adam: "#8E24AA",
  ws: { 34: "#F9A825", 50: "#E65100", 64: "#C62828" },
  grid: "#e6e6e6", axis: "#cfcfcf", ink2: "#555", ink3: "#888",
  pale: "#9e9e9e",
};
const WS_LEVELS = [34, 50, 64];
const PALE = 0.25;
const TARGET_RP = 4;

const S = {
  core: null, years: 0,
  iso3: "HTI", ws: 64, trigSrc: "chd", thrMem: {}, topN: 25,
  showG: true, showA: true,
  storm: null, issuedIdx: null,
};
const SRC_LABEL = { chd: "CHD forecast", gdacs: "GDACS", adam: "ADAM" };
const IDX = { series: new Map(), obsv: new Map(), ext: new Map(), finals: new Map(), byCountry: new Map() };
const cacheTracks = {}, cacheBuffers = {};
let trackMap = null, bufferMap = null, trackLayers = [], bufferLayerGroup = null;

const $ = (id) => document.getElementById(id);
const fmt = (n) => (n == null || Number.isNaN(n)) ? "—" : Math.round(n).toLocaleString("en-US");
const fmtT = (t) => t ? t.slice(0, 10) + " " + t.slice(11, 13) + "Z" : "—";
const key = (...p) => p.join("|");

/* ---------------------------------------------------------- data access */

function buildIndexes(core) {
  for (const [a, i, w, t, fo, ob, tot] of core.series) {
    const k = key(a, i, w);
    if (!IDX.series.has(k)) IDX.series.set(k, []);
    IDX.series.get(k).push({ t, fo, ob, tot });
  }
  for (const [a, i, w, t, pop] of core.obsv) {
    const k = key(a, i, w);
    if (!IDX.obsv.has(k)) IDX.obsv.set(k, []);
    IDX.obsv.get(k).push({ t, pop });
  }
  for (const [src, a, i, w, t, pop] of core.ext) {
    const k = key(src, a, i, w);
    if (!IDX.ext.has(k)) IDX.ext.set(k, []);
    IDX.ext.get(k).push({ t, pop });
  }
  for (const [a, i, w, obsv, fmax, gdacs, adam] of core.finals) {
    IDX.finals.set(key(a, i, w), { obsv, fmax, gdacs, adam });
    if (!IDX.byCountry.has(i)) IDX.byCountry.set(i, new Map());
    const c = IDX.byCountry.get(i);
    if (!c.has(a)) c.set(a, {});
    c.get(a)[w] = { obsv, fmax, gdacs, adam };
  }
}

const country = () => S.core.countries.find((c) => c.iso3 === S.iso3);
const stormMeta = (a) => S.core.storms[a] || { name: a, season: null, sid: null };
const label = (a) => {
  const m = stormMeta(a);
  return m.season ? `${m.name} ${m.season}` : m.name;
};
const totalPop = () => {
  const c = country();
  if (c && c.pop) return c.pop;
  let mx = 0;
  for (const r of rankedRows(Infinity)) mx = Math.max(mx, r.obsv || 0, r.fmax || 0);
  return Math.max(mx, 1e6);
};

function rankedRows(limit) {
  const c = IDX.byCountry.get(S.iso3) || new Map();
  const rows = [];
  for (const [a, byWs] of c) {
    const f = byWs[S.ws];
    if (!f) continue; // no data at this wind level
    rows.push({ atcf: a, obsv: f.obsv ?? null, fmax: f.fmax ?? null, gdacs: f.gdacs ?? null, adam: f.adam ?? null });
  }
  rows.sort((x, y) =>
    (y.obsv ?? -1) - (x.obsv ?? -1) || (y.fmax ?? -1) - (x.fmax ?? -1));
  for (const r of rows) {
    r.trigT = triggerTime(r.atcf);
    r.label = label(r.atcf);
  }
  return rows.slice(0, limit);
}

/* Time series the trigger is evaluated on: CHD forecast value per issued
 * time, or the GDACS/ADAM estimate per valid time. */
function trigSeries(atcf, ws) {
  if (S.trigSrc === "chd") {
    return (IDX.series.get(key(atcf, S.iso3, ws)) || [])
      .map((p) => ({ t: p.t, v: p.tot }));
  }
  return (IDX.ext.get(key(S.trigSrc, atcf, S.iso3, ws)) || [])
    .map((p) => ({ t: p.t, v: p.pop }));
}

function triggerTime(atcf, ws = S.ws, thr = curThr()) {
  for (const p of trigSeries(atcf, ws)) if (p.v != null && p.v >= thr) return p.t;
  return null;
}

function stormMaxTotals(ws) {
  const c = IDX.byCountry.get(S.iso3) || new Map();
  const out = [];
  for (const a of c.keys()) {
    let mx = 0;
    for (const p of trigSeries(a, ws)) mx = Math.max(mx, p.v || 0);
    if (mx > 0) out.push(mx);
  }
  return out.sort((a, b) => b - a);
}

function defaultThreshold(ws) {
  const vals = stormMaxTotals(ws);
  if (!vals.length) return 100000;
  // Weibull plotting position (team standard, methods/return-periods.md):
  // RP = (n_years + 1) / n_activations
  const k = Math.min(Math.max(Math.floor((S.years + 1) / TARGET_RP + 0.5 - 1e-9), 1), vals.length);
  const v = vals[k - 1];
  const mag = Math.pow(10, Math.max(Math.floor(Math.log10(v)) - 1, 0));
  return Math.max(Math.floor(v / mag) * mag, 1000);
}

/* Slider spans 0 → just above the largest historical exposure at this wind
 * level; the total population is shown after a visible scale break and is
 * not itself selectable. */
function thresholdOptions(ws) {
  const pop = totalPop();
  const c = IDX.byCountry.get(S.iso3) || new Map();
  let topHist = 0;
  for (const [, byWs] of c) {
    const f = byWs[ws] || {};
    topHist = Math.max(topHist, S.trigSrc === "chd"
      ? Math.max(f.obsv || 0, f.fmax || 0)
      : f[S.trigSrc] || 0);
  }
  if (!topHist) topHist = pop / 2;
  topHist = Math.min(topHist, pop);
  const raw = Math.max(topHist / 120, 1);
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = Math.ceil(raw / mag) * mag;
  const top = Math.min((Math.ceil(topHist / step) + 1) * step, pop);
  const opts = [];
  for (let v = 0; v <= top; v += step) opts.push(Math.round(v));
  return opts;
}

function curThr() {
  const k = key(S.iso3, S.ws, S.trigSrc);
  if (S.thrMem[k] != null) return S.thrMem[k];
  const opts = thresholdOptions(S.ws);
  const d = Math.min(defaultThreshold(S.ws), totalPop());
  return opts.reduce((best, v) => Math.abs(v - d) < Math.abs(best - d) ? v : best, opts[0]);
}

/* ------------------------------------------------------------- tooltip */

const tipEl = $("tooltip");
function tipShow(x, y, title, rows) {
  tipEl.replaceChildren();
  const t = document.createElement("div");
  t.className = "t-title"; t.textContent = title;
  tipEl.appendChild(t);
  for (const [color, lbl, val] of rows) {
    const r = document.createElement("div"); r.className = "t-row";
    const k = document.createElement("span"); k.className = "t-key"; k.style.borderTopColor = color;
    const v = document.createElement("span"); v.className = "t-val"; v.textContent = val;
    const l = document.createElement("span"); l.className = "t-lbl"; l.textContent = lbl;
    r.append(k, v, l); tipEl.appendChild(r);
  }
  tipEl.style.display = "block";
  const bw = tipEl.offsetWidth, bh = tipEl.offsetHeight;
  tipEl.style.left = Math.min(x + 14, window.innerWidth - bw - 8) + "px";
  tipEl.style.top = Math.min(y + 14, window.innerHeight - bh - 8) + "px";
}
const tipHide = () => { tipEl.style.display = "none"; };

/* ----------------------------------------------------------- svg utils */

function svgEl(tag, attrs = {}, parent = null) {
  const e = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k2, v] of Object.entries(attrs)) e.setAttribute(k2, v);
  if (parent) parent.appendChild(e);
  return e;
}

function niceTicks(max, n = 4) {
  if (max <= 0) return [0, 1];
  const raw = max / n;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw);
  const ticks = [];
  for (let v = 0; v <= max + 1e-9; v += step) ticks.push(v);
  return ticks;
}
const fmtTick = (v) => v >= 1e6 ? (v / 1e6).toLocaleString("en-US", { maximumFractionDigits: 1 }) + "M"
  : v >= 1e3 ? (v / 1e3).toLocaleString("en-US", { maximumFractionDigits: 0 }) + "k"
  : String(v);

function yAxis(svg, x0, x1, yScale, ticks) {
  for (const v of ticks) {
    const y = yScale(v);
    svgEl("line", { x1: x0, x2: x1, y1: y, y2: y, stroke: COL.grid, "stroke-width": 1 }, svg);
    svgEl("text", { x: x0 - 8, y: y + 3.5, "text-anchor": "end", class: "tick" }, svg).textContent = fmtTick(v);
  }
}

function refLine(svg, x0, x1, y, text) {
  svgEl("line", { x1: x0, x2: x1, y1: y, y2: y, stroke: "#616161", "stroke-width": 1.5, "stroke-dasharray": "6 4" }, svg);
  if (text) svgEl("text", { x: x1 - 4, y: y - 5, "text-anchor": "end", class: "refline-label" }, svg).textContent = text;
}

function roundTopBar(svg, x, yTop, w, yBase, fill, opacity, r = 4) {
  const h = Math.max(yBase - yTop, 0);
  const rr = Math.min(r, w / 2, h);
  const d = h <= 0 ? "" :
    `M${x},${yBase} L${x},${yTop + rr} Q${x},${yTop} ${x + rr},${yTop} L${x + w - rr},${yTop} Q${x + w},${yTop} ${x + w},${yTop + rr} L${x + w},${yBase} Z`;
  return svgEl("path", { d, fill, opacity }, svg);
}

function marker(svg, cx, cy, kind, color, size = 5.5, opacity = 1) {
  const g = svgEl("g", { opacity }, svg);
  if (kind === "diamond") {
    svgEl("path", {
      d: `M${cx},${cy - size} L${cx + size},${cy} L${cx},${cy + size} L${cx - size},${cy} Z`,
      fill: color, stroke: "#fff", "stroke-width": 1.5,
    }, g);
  } else if (kind === "x") {
    const s = size * 0.8;
    svgEl("path", {
      d: `M${cx - s},${cy - s} L${cx + s},${cy + s} M${cx - s},${cy + s} L${cx + s},${cy - s}`,
      stroke: "#fff", "stroke-width": 4.5, fill: "none", "stroke-linecap": "round",
    }, g);
    svgEl("path", {
      d: `M${cx - s},${cy - s} L${cx + s},${cy + s} M${cx - s},${cy + s} L${cx + s},${cy - s}`,
      stroke: color, "stroke-width": 2.5, fill: "none", "stroke-linecap": "round",
    }, g);
  }
  return g;
}

function legend(el, items) {
  el.replaceChildren();
  for (const [color, text, type] of items) {
    const k = document.createElement("span"); k.className = "key";
    const sw = document.createElement("span");
    sw.className = type === "line" ? "linekey" : "swatch";
    if (type === "line") sw.style.borderTopColor = color; else sw.style.background = color;
    const t = document.createElement("span"); t.textContent = text;
    k.append(sw, t); el.appendChild(k);
  }
}

/* ------------------------------------------------------- ranked chart */

function renderRanked() {
  const rows = rankedRows(S.topN);
  const el = $("ranked");
  el.replaceChildren();
  legend($("rankedLegend"), [
    [COL.ob, "CHD observed (final)"],
    [COL.fc, "CHD forecast, max (fcastonly+obsv)"],
    ...(S.showG ? [[COL.gdacs, "GDACS (final)"]] : []),
    ...(S.showA ? [[COL.adam, "ADAM (final)"]] : []),
  ]);
  const thr = curThr();
  const W = Math.max(el.clientWidth || 1100, 700), H = 380;
  const m = { l: 64, r: 20, t: 12, b: 58 };
  const svg = svgEl("svg", { width: W, height: H, viewBox: `0 0 ${W} ${H}` }, el);
  if (!rows.length) {
    svgEl("text", { x: W / 2, y: H / 2, class: "nodata", "text-anchor": "middle" }, svg)
      .textContent = "No exposure recorded for this country.";
    return;
  }
  let max = thr;
  for (const r of rows) {
    max = Math.max(max, r.obsv || 0, r.fmax || 0,
      S.showG ? r.gdacs || 0 : 0, S.showA ? r.adam || 0 : 0);
  }
  const yMax = max * 1.08;
  const y = (v) => m.t + (H - m.t - m.b) * (1 - v / yMax);
  const x0 = m.l, x1 = W - m.r;
  yAxis(svg, x0, x1, y, niceTicks(yMax));
  svgEl("text", {
    x: 14, y: (m.t + H - m.b) / 2, class: "axis-title",
    transform: `rotate(-90 14 ${(m.t + H - m.b) / 2})`, "text-anchor": "middle",
  }, svg).textContent = `Population exposed (${S.ws} kt)`;

  const band = (x1 - x0) / rows.length;
  const barW = Math.min(24, band * 0.28);
  const yBase = y(0);
  rows.forEach((r, i) => {
    const cx = x0 + band * (i + 0.5);
    const op = r.trigT ? 1 : PALE;
    if (r.obsv != null) roundTopBar(svg, cx - barW - 1, y(r.obsv), barW, yBase, COL.ob, op);
    if (r.fmax != null) roundTopBar(svg, cx + 1, y(r.fmax), barW, yBase, COL.fc, op);
    if (S.showG && r.gdacs != null) marker(svg, cx - barW / 2 - 1, y(r.gdacs), "diamond", COL.gdacs, 5.5, op);
    if (S.showA && r.adam != null) marker(svg, cx + barW / 2 + 1, y(r.adam), "x", COL.adam, 5.5, op);
    const lbl = svgEl("text", { x: cx, y: H - m.b + 18, "text-anchor": "middle", class: "tick" }, svg);
    lbl.textContent = r.label.length > 14 && rows.length > 15 ? r.label.slice(0, 13) + "…" : r.label;
    // hit target: whole band
    const hit = svgEl("rect", { x: cx - band / 2, y: m.t, width: band, height: H - m.t - m.b, fill: "transparent" }, svg);
    hit.addEventListener("pointermove", (ev) => {
      tipShow(ev.clientX, ev.clientY, r.label, [
        [COL.ob, "observed (final)", fmt(r.obsv)],
        [COL.fc, "forecast (max)", fmt(r.fmax)],
        ...(S.showG ? [[COL.gdacs, "GDACS", fmt(r.gdacs)]] : []),
        ...(S.showA ? [[COL.adam, "ADAM", fmt(r.adam)]] : []),
        ["#616161", r.trigT ? `triggered ${fmtT(r.trigT)}` : "did not trigger", ""],
      ]);
    });
    hit.addEventListener("pointerleave", tipHide);
  });
  svgEl("line", { x1: x0, x2: x1, y1: yBase, y2: yBase, stroke: COL.axis, "stroke-width": 1 }, svg);
  refLine(svg, x0, x1, y(thr), `threshold ${fmt(thr)}`);
}

/* ------------------------------------------------------- trigger table */

function renderTable() {
  const rows = rankedRows(Infinity);
  const wrap = $("trigTable");
  wrap.replaceChildren();
  const tbl = document.createElement("table");
  const head = tbl.createTHead().insertRow();
  for (const h of ["Storm", "ATCF ID", "Observed (final)", "Forecast (max)", "GDACS (final)", "ADAM (final)", "Triggered", "First trigger (issued)"]) {
    const th = document.createElement("th"); th.textContent = h; head.appendChild(th);
  }
  const body = tbl.createTBody();
  for (const r of rows) {
    const tr = body.insertRow();
    if (r.trigT) tr.className = "trig";
    const cells = [r.label, r.atcf, fmt(r.obsv), fmt(r.fmax), fmt(r.gdacs), fmt(r.adam),
      r.trigT ? "yes" : "no", r.trigT ? fmtT(r.trigT) : "—"];
    cells.forEach((c, j) => {
      const td = tr.insertCell(); td.textContent = c;
      if (c === "—") td.className = "na";
      if (j === 0 || j === 1) td.style.textAlign = "left";
    });
  }
  wrap.appendChild(tbl);
}

/* ---------------------------------------------------------- track map */

async function renderTrackMap() {
  if (!$("trackDetails").open) return;
  if (!trackMap) {
    trackMap = L.map("trackMap", { scrollWheelZoom: false });
    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      attribution: "&copy; OpenStreetMap &copy; CARTO", subdomains: "abcd",
    }).addTo(trackMap);
  }
  trackMap.invalidateSize();
  if (!cacheTracks[S.iso3]) {
    cacheTracks[S.iso3] = fetch(`data/tracks/${S.iso3}.json`).then((r) => r.ok ? r.json() : {});
  }
  const tracks = await cacheTracks[S.iso3];
  for (const l of trackLayers) trackMap.removeLayer(l);
  trackLayers = [];
  const rows = rankedRows(S.topN);
  const shown = new Set(rows.map((r) => r.atcf));
  const trig = new Map(rows.map((r) => [r.atcf, !!r.trigT]));
  const sorted = Object.entries(tracks).filter(([a]) => shown.has(a))
    .sort(([a], [b]) => (trig.get(a) ? 1 : 0) - (trig.get(b) ? 1 : 0));
  for (const [a, line] of sorted) {
    const t = trig.get(a);
    const pl = L.polyline(line, {
      color: t ? COL.fc : COL.pale, weight: t ? 2.5 : 1.5, opacity: t ? 0.9 : 0.35,
    }).addTo(trackMap);
    pl.bindTooltip(label(a), { sticky: true });
    trackLayers.push(pl);
  }
  const c = country();
  if (c && c.bbox) {
    trackMap.fitBounds([[c.bbox[1], c.bbox[0]], [c.bbox[3], c.bbox[2]]], { padding: [60, 60] });
  } else if (trackLayers.length) {
    trackMap.fitBounds(L.featureGroup(trackLayers).getBounds());
  }
}

/* ------------------------------------------------------ storm section */

function stormOptions() {
  return rankedRows(Infinity).map((r) => r.atcf);
}

function tiles(el, pairs) {
  el.replaceChildren();
  for (const [lbl, val] of pairs) {
    const d = document.createElement("div"); d.className = "tile";
    const l = document.createElement("div"); l.className = "lbl"; l.textContent = lbl;
    const v = document.createElement("div"); v.className = "val"; v.textContent = val;
    d.append(l, v); el.appendChild(d);
  }
}

function renderStorm() {
  const opts = stormOptions();
  if (!opts.includes(S.storm)) S.storm = opts[0] || null;
  const sel = $("storm");
  sel.replaceChildren();
  for (const a of opts) {
    const o = document.createElement("option");
    o.value = a; o.textContent = `${label(a)} (${a})`;
    sel.appendChild(o);
  }
  if (S.storm) sel.value = S.storm;
  const f = (IDX.byCountry.get(S.iso3)?.get(S.storm) || {})[S.ws] || {};
  const trigT = S.storm ? triggerTime(S.storm) : null;
  tiles($("stormTiles"), [
    ["Observed (final)", fmt(f.obsv)],
    ["Forecast (max)", fmt(f.fmax)],
    ["Triggered", trigT ? "Yes" : "No"],
    ["First trigger (issued)", fmtT(trigT)],
  ]);
  renderEvolution();
  renderEvoTable();
}

function renderEvolution() {
  const el = $("evo");
  el.replaceChildren();
  legend($("evoLegend"), [
    [COL.fc, "CHD forecast (fcastonly+obsv)", "line"],
    [COL.ob, "CHD observed", "line"],
    ...(S.showG ? [[COL.gdacs, "GDACS"]] : []),
    ...(S.showA ? [[COL.adam, "ADAM"]] : []),
  ]);
  if (!S.storm) return;
  const thr = curThr();
  const W = Math.max(el.clientWidth || 1100, 700);
  const fh = 190, gap = 30, m = { l: 64, r: 20, t: 24, b: 34 };
  const H = m.t + 3 * fh + 2 * gap + m.b;
  const svg = svgEl("svg", { width: W, height: H, viewBox: `0 0 ${W} ${H}` }, el);
  const x0 = m.l, x1 = W - m.r;

  // shared time domain
  let tMin = Infinity, tMax = -Infinity;
  const seriesOf = (w) => IDX.series.get(key(S.storm, S.iso3, w)) || [];
  const obsvOf = (w) => IDX.obsv.get(key(S.storm, S.iso3, w)) || [];
  const extOf = (src, w) => IDX.ext.get(key(src, S.storm, S.iso3, w)) || [];
  for (const w of WS_LEVELS) {
    for (const arr of [seriesOf(w), obsvOf(w),
      S.showG ? extOf("gdacs", w) : [], S.showA ? extOf("adam", w) : []]) {
      for (const p of arr) {
        const ms = Date.parse(p.t + "Z");
        if (ms < tMin) tMin = ms; if (ms > tMax) tMax = ms;
      }
    }
  }
  if (!isFinite(tMin)) {
    svgEl("text", { x: W / 2, y: H / 2, class: "nodata", "text-anchor": "middle" }, svg)
      .textContent = "No time series data for this storm/country.";
    return;
  }
  const span = Math.max(tMax - tMin, 1);
  const x = (t) => x0 + ((Date.parse(t + "Z") - tMin) / span) * (x1 - x0);

  const hoverData = [];
  WS_LEVELS.forEach((w, fi) => {
    const top = m.t + fi * (fh + gap);
    const sv = seriesOf(w), ov = obsvOf(w);
    const gd = S.showG ? extOf("gdacs", w) : [], ad = S.showA ? extOf("adam", w) : [];
    const isSel = w === S.ws;
    svgEl("text", { x: (x0 + x1) / 2, y: top - 8, "text-anchor": "middle", class: "facet-title" }, svg)
      .textContent = `${w} kt${isSel ? "  ·  trigger level" : ""}`;

    let fmaxV = isSel ? thr : 1;
    for (const arr of [sv.map((p) => p.tot), ov.map((p) => p.pop),
      gd.map((p) => p.pop), ad.map((p) => p.pop)]) {
      for (const v of arr) if (v != null && v > fmaxV) fmaxV = v;
    }
    const yMax = fmaxV * 1.1;
    const y = (v) => top + fh * (1 - v / yMax);
    yAxis(svg, x0, x1, y, niceTicks(yMax, 3));
    svgEl("line", { x1: x0, x2: x1, y1: y(0), y2: y(0), stroke: COL.axis, "stroke-width": 1 }, svg);

    if (!sv.length && !ov.length) {
      svgEl("text", { x: (x0 + x1) / 2, y: top + fh / 2, class: "nodata", "text-anchor": "middle" }, svg)
        .textContent = `No exposure at ${w} kt`;
    }
    const linePath = (pts, val, color) => {
      if (!pts.length) return;
      const d = pts.map((p, j) => `${j ? "L" : "M"}${x(p.t).toFixed(1)},${y(val(p)).toFixed(1)}`).join(" ");
      svgEl("path", { d, fill: "none", stroke: color, "stroke-width": 2, "stroke-linejoin": "round", "stroke-linecap": "round" }, svg);
      for (const p of pts) {
        svgEl("circle", { cx: x(p.t), cy: y(val(p)), r: 4, fill: color, stroke: "#fff", "stroke-width": 2 }, svg);
      }
    };
    linePath(sv, (p) => p.tot, COL.fc);
    linePath(ov, (p) => p.pop, COL.ob);
    for (const p of gd) marker(svg, x(p.t), y(p.pop), "diamond", COL.gdacs);
    for (const p of ad) marker(svg, x(p.t), y(p.pop), "x", COL.adam);

    if (isSel) {
      refLine(svg, x0, x1, y(Math.min(thr, yMax)), `threshold ${fmt(thr)}`);
    }
    hoverData.push({ top, fh, w, sv, ov, gd, ad, y });
  });

  // x ticks on bottom facet
  const bottomY = m.t + 3 * fh + 2 * gap;
  const nx = 6;
  for (let i = 0; i <= nx; i++) {
    const ms = tMin + (span * i) / nx;
    const d = new Date(ms);
    svgEl("text", { x: x0 + ((x1 - x0) * i) / nx, y: bottomY + 20, "text-anchor": "middle", class: "tick" }, svg)
      .textContent = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
  }
  svgEl("text", {
    x: 14, y: H / 2, class: "axis-title",
    transform: `rotate(-90 14 ${H / 2})`, "text-anchor": "middle",
  }, svg).textContent = "Population exposed";

  // crosshair + tooltip
  const hair = svgEl("line", { y1: m.t, y2: bottomY, stroke: COL.axis, "stroke-width": 1, visibility: "hidden" }, svg);
  const hit = svgEl("rect", { x: x0, y: m.t, width: x1 - x0, height: bottomY - m.t, fill: "transparent" }, svg);
  const allTimes = [...new Set(WS_LEVELS.flatMap((w) =>
    [...seriesOf(w), ...obsvOf(w)].map((p) => p.t)))].sort();
  hit.addEventListener("pointermove", (ev) => {
    const rect = svg.getBoundingClientRect();
    const px = ev.clientX - rect.left;
    let best = null, bd = Infinity;
    for (const t of allTimes) {
      const d = Math.abs(x(t) - px);
      if (d < bd) { bd = d; best = t; }
    }
    if (!best) return;
    hair.setAttribute("x1", x(best)); hair.setAttribute("x2", x(best));
    hair.setAttribute("visibility", "visible");
    const py = ev.clientY - rect.top;
    const fac = hoverData.find((f) => py >= f.top && py <= f.top + f.fh) || hoverData[0];
    const at = (arr, val) => {
      const m2 = arr.find((p) => p.t === best);
      return m2 ? fmt(val(m2)) : null;
    };
    const rows = [];
    const fv = at(fac.sv, (p) => p.tot); if (fv) rows.push([COL.fc, "forecast", fv]);
    const ov2 = at(fac.ov, (p) => p.pop); if (ov2) rows.push([COL.ob, "observed", ov2]);
    const gv = at(fac.gd, (p) => p.pop); if (gv) rows.push([COL.gdacs, "GDACS", gv]);
    const av = at(fac.ad, (p) => p.pop); if (av) rows.push([COL.adam, "ADAM", av]);
    tipShow(ev.clientX, ev.clientY, `${fmtT(best)} — ${fac.w} kt`, rows);
  });
  hit.addEventListener("pointerleave", () => { tipHide(); hair.setAttribute("visibility", "hidden"); });
}

function renderEvoTable() {
  const wrap = $("evoTable");
  wrap.replaceChildren();
  if (!S.storm) return;
  const tbl = document.createElement("table");
  const head = tbl.createTHead().insertRow();
  for (const h of ["Issued/valid time", ...WS_LEVELS.map((w) => `Forecast ${w} kt`), ...WS_LEVELS.map((w) => `Observed ${w} kt`)]) {
    const th = document.createElement("th"); th.textContent = h; head.appendChild(th);
  }
  const sOf = (w) => new Map((IDX.series.get(key(S.storm, S.iso3, w)) || []).map((p) => [p.t, p.tot]));
  const oOf = (w) => new Map((IDX.obsv.get(key(S.storm, S.iso3, w)) || []).map((p) => [p.t, p.pop]));
  const sMaps = WS_LEVELS.map(sOf), oMaps = WS_LEVELS.map(oOf);
  const times = [...new Set([...sMaps, ...oMaps].flatMap((m2) => [...m2.keys()]))].sort();
  const body = tbl.createTBody();
  for (const t of times) {
    const tr = body.insertRow();
    tr.insertCell().textContent = fmtT(t);
    for (const m2 of [...sMaps, ...oMaps]) {
      const td = tr.insertCell();
      const v = m2.get(t);
      td.textContent = v != null ? fmt(v) : "—";
      if (v == null) td.className = "na";
    }
  }
  wrap.appendChild(tbl);
}

/* ----------------------------------------------------- issued section */

function issuedTimes() {
  if (!S.storm) return [];
  const set = new Set();
  for (const w of WS_LEVELS) {
    for (const p of IDX.series.get(key(S.storm, S.iso3, w)) || []) set.add(p.t);
  }
  return [...set].sort();
}

function renderIssued() {
  const times = issuedTimes();
  const slider = $("issued");
  const sec = slider.closest("section");
  if (!times.length) {
    sec.style.display = "none";
    return;
  }
  sec.style.display = "";
  slider.max = times.length - 1;
  if (S.issuedIdx == null || S.issuedIdx >= times.length) {
    const trigT = triggerTime(S.storm);
    // first issued time at/after the trigger (ext trigger times fall between
    // CHD issued times)
    const i = trigT ? times.findIndex((x) => x >= trigT) : -1;
    S.issuedIdx = i >= 0 ? i : times.length - 1;
  }
  slider.value = S.issuedIdx;
  const t = times[S.issuedIdx];
  $("issuedVal").textContent = fmtT(t);
  const thr = curThr();
  const rowAt = (w) => (IDX.series.get(key(S.storm, S.iso3, w)) || []).find((p) => p.t === t);
  const selRow = rowAt(S.ws);
  const v = selRow ? selRow.tot : null;
  // trigger-source value known by this time: CHD forecast at this issued
  // time, or the latest GDACS/ADAM estimate at/before it
  let trigV = v;
  if (S.trigSrc !== "chd") {
    trigV = null;
    for (const p of trigSeries(S.storm, S.ws)) {
      if (p.t <= t && p.v != null) trigV = p.v;
    }
  }
  tiles($("issuedTiles"), [
    [`Forecast value at ${S.ws} kt`, fmt(v)],
    ...(S.trigSrc === "chd" ? [] : [[`${SRC_LABEL[S.trigSrc]} value by this time`, fmt(trigV)]]),
    [`Meets threshold (${SRC_LABEL[S.trigSrc]})`, trigV != null && trigV >= thr ? "Yes" : "No"],
  ]);
  $("bufTitle").textContent = `Track buffers at ${fmtT(t)}`;
  $("itTitle").textContent = `Forecast value at ${fmtT(t)}`;
  renderIssuedBar(t, thr, rowAt);
  renderBufferMap(t);
}

function renderIssuedBar(t, thr, rowAt) {
  const el = $("issuedBar");
  el.replaceChildren();
  legend($("itLegend"), [
    [COL.ob, "Observed part"],
    [COL.fc, "Fcastonly part"],
    ...(S.showG ? [[COL.gdacs, "GDACS (nearest)"]] : []),
    ...(S.showA ? [[COL.adam, "ADAM (nearest)"]] : []),
  ]);
  const W = Math.max(el.clientWidth || 440, 320), H = 400;
  const m = { l: 64, r: 14, t: 12, b: 40 };
  const svg = svgEl("svg", { width: W, height: H, viewBox: `0 0 ${W} ${H}` }, el);
  const tMs = Date.parse(t + "Z");
  const nearest = (src, w) => {
    const arr = IDX.ext.get(key(src, S.storm, S.iso3, w)) || [];
    let best = null, bd = Infinity;
    for (const p of arr) {
      const d = Math.abs(Date.parse(p.t + "Z") - tMs);
      if (d < bd) { bd = d; best = p.pop; }
    }
    return best;
  };
  const cols = WS_LEVELS.map((w) => {
    const r = rowAt(w);
    return {
      w, ob: r ? r.ob || 0 : 0, fo: r ? r.fo || 0 : 0,
      gd: S.showG ? nearest("gdacs", w) : null,
      ad: S.showA ? nearest("adam", w) : null,
    };
  });
  let max = thr;
  for (const c of cols) max = Math.max(max, c.ob + c.fo, c.gd || 0, c.ad || 0);
  const yMax = max * 1.15;
  const y = (v) => m.t + (H - m.t - m.b) * (1 - v / yMax);
  const x0 = m.l, x1 = W - m.r;
  yAxis(svg, x0, x1, y, niceTicks(yMax));
  svgEl("text", {
    x: 14, y: (m.t + H - m.b) / 2, class: "axis-title",
    transform: `rotate(-90 14 ${(m.t + H - m.b) / 2})`, "text-anchor": "middle",
  }, svg).textContent = "Population exposed";
  const band = (x1 - x0) / 3, barW = Math.min(24, band * 0.4);
  const yBase = y(0);
  cols.forEach((c, i) => {
    const cx = x0 + band * (i + 0.5);
    // observed segment: square both ends (baseline segment)
    if (c.ob > 0) {
      svgEl("rect", { x: cx - barW / 2, y: y(c.ob), width: barW, height: yBase - y(c.ob), fill: COL.ob }, svg);
    }
    // fcastonly on top, 2px surface gap, rounded cap
    if (c.fo > 0) {
      const gapPx = c.ob > 0 ? 2 : 0;
      roundTopBar(svg, cx - barW / 2, y(c.ob + c.fo), barW, y(c.ob) - gapPx, COL.fc, 1);
    }
    if (c.gd != null) marker(svg, cx - barW / 2 - 8, y(c.gd), "diamond", COL.gdacs, 6);
    if (c.ad != null) marker(svg, cx + barW / 2 + 8, y(c.ad), "x", COL.adam, 6);
    svgEl("text", { x: cx, y: H - m.b + 18, "text-anchor": "middle", class: "tick" }, svg).textContent = `${c.w} kt`;
    const hit = svgEl("rect", { x: cx - band / 2, y: m.t, width: band, height: H - m.t - m.b, fill: "transparent" }, svg);
    hit.addEventListener("pointermove", (ev) => {
      tipShow(ev.clientX, ev.clientY, `${c.w} kt — ${fmtT(t)}`, [
        [COL.fc, "forecast total", fmt(c.ob + c.fo)],
        [COL.ob, "observed part", fmt(c.ob)],
        [COL.fc, "fcastonly part", fmt(c.fo)],
        ...(c.gd != null ? [[COL.gdacs, "GDACS (nearest)", fmt(c.gd)]] : []),
        ...(c.ad != null ? [[COL.adam, "ADAM (nearest)", fmt(c.ad)]] : []),
      ]);
    });
    hit.addEventListener("pointerleave", tipHide);
  });
  svgEl("line", { x1: x0, x2: x1, y1: yBase, y2: yBase, stroke: COL.axis, "stroke-width": 1 }, svg);
  refLine(svg, x0, x1, y(Math.min(thr, yMax)), `threshold ${fmt(thr)}`);
}

async function renderBufferMap(t) {
  if (!bufferMap) {
    bufferMap = L.map("bufferMap", { scrollWheelZoom: false });
    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      attribution: "&copy; OpenStreetMap &copy; CARTO", subdomains: "abcd",
    }).addTo(bufferMap);
    bufferLayerGroup = L.featureGroup().addTo(bufferMap);
  }
  bufferMap.invalidateSize();
  const storm = S.storm, iso3 = S.iso3;
  if (!cacheBuffers[storm]) {
    cacheBuffers[storm] = fetch(`data/buffers/${storm}.json`).then((r) => r.ok ? r.json() : { fcastonly: {}, obsv: {} });
  }
  const buf = await cacheBuffers[storm];
  if (storm !== S.storm || iso3 !== S.iso3) return; // stale async
  bufferLayerGroup.clearLayers();
  // observed so far: latest obsv valid_time <= issued time
  const ovT = Object.keys(buf.obsv).filter((k2) => k2 <= t).sort().pop();
  if (ovT) {
    for (const w of WS_LEVELS) {
      const g = buf.obsv[ovT][String(w)];
      if (g) L.geoJSON(g, { style: { color: COL.ob, weight: 1.5, opacity: 0.8, fillColor: COL.ob, fillOpacity: 0.12 } })
        .bindTooltip(`Observed so far — ${w} kt`, { sticky: true }).addTo(bufferLayerGroup);
    }
  }
  const fo = buf.fcastonly[t] || {};
  for (const w of WS_LEVELS) {
    const g = fo[String(w)];
    if (g) L.geoJSON(g, { style: { color: COL.ws[w], weight: 1.5, opacity: 0.9, fillColor: COL.ws[w], fillOpacity: 0.35 } })
      .bindTooltip(`Forecast — ${w} kt`, { sticky: true }).addTo(bufferLayerGroup);
  }
  const c = country();
  let bounds = bufferLayerGroup.getLayers().length ? bufferLayerGroup.getBounds() : null;
  if (c && c.bbox) {
    const cb = L.latLngBounds([[c.bbox[1], c.bbox[0]], [c.bbox[3], c.bbox[2]]]);
    bounds = bounds ? bounds.extend(cb) : cb;
  }
  if (bounds) bufferMap.fitBounds(bounds, { padding: [30, 30] });
}

/* ------------------------------------------------------------ controls */

function rebuildThrSlider() {
  const opts = thresholdOptions(S.ws);
  const slider = $("thr");
  slider.max = opts.length - 1;
  const cur = curThr();
  let idx = opts.indexOf(cur);
  if (idx < 0) {
    idx = opts.reduce((bi, v, i) => Math.abs(v - cur) < Math.abs(opts[bi] - cur) ? i : bi, 0);
    S.thrMem[key(S.iso3, S.ws, S.trigSrc)] = opts[idx];
  }
  slider.value = idx;
  slider.dataset.opts = JSON.stringify(opts);
  updateThrLabel();
}

function updateThrLabel() {
  $("thrVal").textContent = fmt(curThr());
  const opts = thresholdOptions(S.ws);
  const sliderTop = opts[opts.length - 1] || 0;
  // show the scale break only when there is a real gap up to the total pop
  $("thrBreak").classList.toggle("off", sliderTop >= totalPop() * 0.98);
  $("popCap").textContent = `${fmt(totalPop())} (total pop)`;
}

function renderMetrics() {
  const rows = rankedRows(Infinity);
  const n = rows.filter((r) => r.trigT).length;
  $("mTrig").textContent = `${n}/${rows.length}`;
  $("mRP").textContent = n ? `≈ ${((S.years + 1) / n).toFixed(1)} yr` : "—";
}

function renderCountryHeader() {
  const c = country();
  $("countryTitle").textContent = `Storms by exposure — ${c ? c.name : S.iso3}`;
  const nAll = rankedRows(Infinity).length;
  const topN = $("topN");
  topN.max = Math.max(nAll, 6);
  topN.min = Math.min(5, nAll);
  if (S.topN > nAll) S.topN = nAll;
  topN.value = S.topN;
  $("topNVal").textContent = S.topN;
}

function refreshCountry() {
  renderCountryHeader();
  renderMetrics();
  renderRanked();
  renderTable();
  renderTrackMap();
}
function refreshAll() {
  rebuildThrSlider();
  refreshCountry();
  renderStorm();
  renderIssued();
}

function wire() {
  const cSel = $("country");
  for (const c of S.core.countries) {
    const o = document.createElement("option");
    o.value = c.iso3; o.textContent = `${c.name} (${c.iso3})`;
    cSel.appendChild(o);
  }
  if (!S.core.countries.some((c) => c.iso3 === S.iso3)) S.iso3 = S.core.countries[0].iso3;
  cSel.value = S.iso3;
  cSel.addEventListener("change", () => {
    S.iso3 = cSel.value; S.storm = null; S.issuedIdx = null;
    refreshAll();
  });
  for (const r of document.querySelectorAll("#wsRadios input")) {
    r.addEventListener("change", () => {
      S.ws = +r.value; S.issuedIdx = null;
      refreshAll();
    });
  }
  for (const r of document.querySelectorAll("#srcRadios input")) {
    r.addEventListener("change", () => {
      S.trigSrc = r.value; S.issuedIdx = null;
      refreshAll();
    });
  }
  const thr = $("thr");
  thr.addEventListener("input", () => {           // live on drag
    const opts = JSON.parse(thr.dataset.opts || "[]");
    S.thrMem[key(S.iso3, S.ws, S.trigSrc)] = opts[+thr.value] ?? 0;
    updateThrLabel();
    refreshCountry();
    renderStorm();
    renderIssued();
  });
  $("topN").addEventListener("input", () => {
    S.topN = +$("topN").value;
    $("topNVal").textContent = S.topN;
    renderRanked();
    renderTrackMap();
  });
  $("showGdacs").addEventListener("change", () => { S.showG = $("showGdacs").checked; refreshAll(); });
  $("showAdam").addEventListener("change", () => { S.showA = $("showAdam").checked; refreshAll(); });
  $("storm").addEventListener("change", () => {
    S.storm = $("storm").value; S.issuedIdx = null;
    renderStorm(); renderIssued();
  });
  $("issued").addEventListener("input", () => {   // live on drag
    S.issuedIdx = +$("issued").value;
    renderIssued();
  });
  $("trackDetails").addEventListener("toggle", () => renderTrackMap());
  window.addEventListener("resize", () => { renderRanked(); renderStorm(); renderIssued(); });

  // sticky storm selector: stack below the top bar once scrolled into the
  // single-storm sections
  const setTopH = () =>
    document.documentElement.style.setProperty("--topbar-h", $("topbar").offsetHeight + "px");
  setTopH();
  window.addEventListener("resize", setTopH);
  new IntersectionObserver(
    ([e]) => $("stormRow").classList.toggle(
      "stuck",
      !e.isIntersecting && e.boundingClientRect.top < $("topbar").offsetHeight + 2
    ),
    { rootMargin: `-${$("topbar").offsetHeight + 2}px 0px 0px 0px` }
  ).observe($("stormSentinel"));
}

async function init() {
  const core = await (await fetch("data/core.json")).json();
  S.core = core;
  S.years = core.record_years;
  buildIndexes(core);
  wire();
  refreshAll();
}

init();
