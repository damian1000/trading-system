/* Thin renderer over the server's snapshot JSON: every number is computed server-side by
   risk-engine's calculators; this file only formats and signs what /api/stream pushes. */

const $ = (id) => document.getElementById(id);

const money = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const qty = new Intl.NumberFormat("en-US");

const fmt = (n, dp = 4) => (n == null ? "—" : n.toFixed(dp));

function signed(value, text) {
  const cls = value > 0 ? "pos" : value < 0 ? "neg" : "";
  return `<span class="${cls}">${text}</span>`;
}

function arrow(el, value, text) {
  el.textContent = text;
  el.classList.toggle("up", value > 0);
  el.classList.toggle("down", value < 0);
  el.classList.toggle("pos", value > 0);
  el.classList.toggle("neg", value < 0);
}

function renderStats(snapshot) {
  const position = snapshot.positions[0];
  const report = snapshot.report;
  if (position) {
    arrow($("st-position"), position.quantity, qty.format(position.quantity));
    $("st-last").textContent = money.format(position.lastPrice);
  }
  if (report) {
    $("st-valuation").textContent = money.format(report.valuation);
    const pnl = report.pnl ? report.pnl.actual : null;
    if (pnl != null) arrow($("st-pnl"), pnl, money.format(pnl));
  }
}

function renderPositions(positions) {
  if (positions.length === 0) {
    $("positions").innerHTML =
      '<p class="empty">awaiting the first fill&hellip;</p>';
    return;
  }
  const rows = positions
    .map(
      (p) => `<tr>
        <td>${p.symbol}</td>
        <td>${signed(p.quantity, qty.format(p.quantity))}</td>
        <td>${money.format(p.lastPrice)}</td>
        <td>${new Date(p.lastTimeMillis).toLocaleTimeString()}</td>
      </tr>`,
    )
    .join("");
  $("positions").innerHTML = `<div class="report-block"><table class="risk">
      <thead><tr><th>Symbol</th><th>Net Qty</th><th>Last</th><th>Updated</th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div>`;
}

function renderReport(report) {
  if (!report) {
    $("report").innerHTML =
      '<p class="empty">no marks yet &mdash; the report prices off the first fill</p>';
    return;
  }
  const g = report.greeks;
  const pct = Math.round(report.confidence * 100);
  const varBlock = (label, m) => `<tr>
      <td>${label}</td>
      <td>${money.format(m.valueAtRisk)}</td>
      <td>${money.format(m.expectedShortfall)}</td>
    </tr>`;
  let html = `<div class="report-block">
      <h3>Valuation <span class="sub">mark-to-market</span></h3>
      <div class="valuation">${money.format(report.valuation)}</div>
    </div>
    <div class="report-block">
      <h3>Greeks <span class="sub">bump-and-reprice</span></h3>
      <table class="risk"><tbody>
        <tr><td>Delta</td><td>${signed(g.delta, fmt(g.delta, 2))}</td></tr>
        <tr><td>Gamma</td><td>${fmt(g.gamma)}</td></tr>
        <tr><td>Vega</td><td>${fmt(g.vega, 2)}</td></tr>
        <tr><td>Theta</td><td>${signed(g.theta, fmt(g.theta, 2))}</td></tr>
        <tr><td>Rho</td><td>${fmt(g.rho, 2)}</td></tr>
      </tbody></table>
    </div>
    <div class="report-block">
      <h3>VaR / Expected Shortfall <span class="sub">${pct}% &middot; 1-day</span></h3>
      <table class="risk">
        <thead><tr><th>Method</th><th>VaR</th><th>ES</th></tr></thead>
        <tbody>
          ${varBlock("Parametric", report.var.parametric)}
          ${varBlock("Historical", report.var.historical)}
        </tbody>
      </table>
    </div>`;
  if (report.pnl) {
    const p = report.pnl;
    const row = (label, v) =>
      `<tr><td>${label}</td><td>${signed(v, money.format(v))}</td></tr>`;
    html += `<div class="report-block">
      <h3>Day PnL <span class="sub">attribution vs session open</span></h3>
      <table class="risk"><tbody>
        ${row("Actual", p.actual)}
        ${row("Delta", p.delta)}
        ${row("Gamma", p.gamma)}
        ${row("Vega", p.vega)}
        ${row("Theta", p.theta)}
        ${row("Rho", p.rho)}
        ${row("Explained", p.explained)}
        ${row("Residual", p.residual)}
      </tbody></table>
    </div>`;
  }
  $("report").innerHTML = html;
}

function renderLimits(limits) {
  if (limits.symbols.length === 0) {
    $("limits").innerHTML =
      '<p class="empty">awaiting the first fill&hellip;</p>';
    return;
  }
  const pct = (u) => `${Math.round(u * 100)}%`;
  const rows = limits.symbols
    .map(
      (s) => `<tr>
        <td>${s.symbol}</td>
        <td>${qty.format(Math.abs(s.netQuantity))} / ${qty.format(limits.maxPosition)}</td>
        <td>${money.format(s.notional)} / ${money.format(limits.maxNotional)}</td>
        <td>${pct(Math.max(s.positionUtilisation, s.notionalUtilisation))}</td>
        <td>${s.breached ? '<span class="neg">BREACH</span>' : '<span class="pos">OK</span>'}</td>
      </tr>`,
    )
    .join("");
  let html = `<div class="report-block"><table class="risk">
      <thead><tr><th>Symbol</th><th>Net</th><th>Notional</th><th>Util</th><th>Status</th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div>`;
  if (limits.events.length > 0) {
    const events = limits.events
      .map(
        (e) => `<tr>
          <td>${new Date(e.ts).toLocaleTimeString()}</td>
          <td>${e.symbol}</td>
          <td>${e.kind}</td>
          <td>${e.breached ? '<span class="neg">breach</span>' : '<span class="pos">cleared</span>'}</td>
          <td>${money.format(e.value)} / ${money.format(e.limit)}</td>
        </tr>`,
      )
      .join("");
    html += `<div class="report-block">
      <h3>Breach events <span class="sub">newest first</span></h3>
      <table class="risk">
        <thead><tr><th>Time</th><th>Symbol</th><th>Limit</th><th>Event</th><th>Value</th></tr></thead>
        <tbody>${events}</tbody>
      </table></div>`;
  }
  if (limits.malformed > 0) {
    html += `<p class="empty">malformed records skipped: ${qty.format(limits.malformed)}</p>`;
  }
  $("limits").innerHTML = html;
}

function renderSession(snapshot) {
  const open = snapshot.openPrice;
  const rows = [
    ["Session open", open == null ? "—" : money.format(open)],
    ["Instruments", String(snapshot.positions.length)],
  ]
    .map(([k, v]) => `<tr><td>${k}</td><td>${v}</td></tr>`)
    .join("");
  $("session").innerHTML =
    `<div class="report-block"><table class="risk"><tbody>${rows}</tbody></table></div>`;
}

let updates = 0;

function render(snapshot) {
  updates++;
  $("updates").textContent = String(updates);
  renderStats(snapshot);
  renderPositions(snapshot.positions);
  renderReport(snapshot.report);
  renderLimits(snapshot.limits);
  renderSession(snapshot);
}

function connect() {
  const stream = new EventSource("/api/stream");
  stream.onopen = () => {
    $("stream").textContent = "live";
  };
  stream.onerror = () => {
    $("stream").textContent = "reconnecting";
  };
  stream.onmessage = (event) => render(JSON.parse(event.data));
}

connect();
