import { buyers, opportunities as sampleOpportunities, sectors, snapshot, suppliers } from "./sample-data.js";

let opportunities = [];
let opportunityFeed = { state: "loading", source: null, builtAt: null };
let appCapabilities = { emailConfigured: false, loaded: false };

const ANALYTICS_STORAGE_KEY = "publicSignalAnalyticsSession";
const ANALYTICS_TARGETS = Object.freeze({
  app_open: ["app"],
  page_open: ["page-pipeline", "page-markets", "page-buyers", "page-suppliers", "page-watches"],
  opportunity_brief_open: ["opportunity-brief"],
  primary_cta_click: ["primary-cta"],
  watch_start: ["watch-start", "watch-notice"],
  opportunity_saved: ["notice-bookmark"],
  filter_apply: ["filter-sector", "filter-deadline", "filter-value", "filter-evidence", "filter-buyer"],
  table_search_apply: ["table-search"],
  source_notice_open: ["source-notice"],
  watch_preview: ["watch-preview"],
  watch_saved: ["watch-saved"],
});

export function analyticsOptedOut(globalObject = globalThis) {
  const navigatorObject = globalObject.navigator || {};
  const doNotTrack = navigatorObject.doNotTrack ?? globalObject.doNotTrack;
  return navigatorObject.globalPrivacyControl === true
    || doNotTrack === true
    || ["1", "yes", "true"].includes(String(doNotTrack).toLowerCase());
}

function analyticsSessionId() {
  if (analyticsOptedOut()) return null;
  try {
    const existing = window.sessionStorage.getItem(ANALYTICS_STORAGE_KEY);
    if (existing && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(existing)) return existing;
    const generated = typeof crypto.randomUUID === "function" ? crypto.randomUUID() : ([...crypto.getRandomValues(new Uint8Array(16))].map((value, index) => (index === 6 ? (value & 0x0f) | 0x40 : index === 8 ? (value & 0x3f) | 0x80 : value).toString(16).padStart(2, "0")).join("").replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, "$1-$2-$3-$4-$5"));
    window.sessionStorage.setItem(ANALYTICS_STORAGE_KEY, generated);
    return generated;
  } catch {
    return null;
  }
}

let analyticsSent = 0;
function trackAnalytics(eventType, targetSlug) {
  if (analyticsSent >= 100 || analyticsOptedOut() || !ANALYTICS_TARGETS[eventType]?.includes(targetSlug)) return;
  const sessionId = analyticsSessionId();
  if (!sessionId) return;
  analyticsSent += 1;
  const body = JSON.stringify({ sessionId, events: [{ eventType, targetSlug }] });
  const blob = new Blob([body], { type: "application/json" });
  try {
    if (typeof navigator.sendBeacon === "function" && navigator.sendBeacon("/api/events", blob)) return;
  } catch {
    // Analytics must never interfere with the product flow.
  }
  void fetch("/api/events", { method: "POST", headers: { "content-type": "application/json" }, body, keepalive: true }).catch(() => {});
}

const icons = {
  inbox: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M4 4h16v13H4z"/><path d="M4 13h4l2 3h4l2-3h4"/></svg>',
  layers: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="m12 3 9 5-9 5-9-5 9-5Z"/><path d="m3 12 9 5 9-5M3 16l9 5 9-5"/></svg>',
  building: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M4 21V6l8-3 8 3v15"/><path d="M8 9h2m4 0h2M8 13h2m4 0h2M8 17h2m4 0h2M2 21h20"/></svg>',
  briefcase: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><rect x="3" y="7" width="18" height="13" rx="2"/><path d="M8 7V5a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2M3 12h18M10 12v2h4v-2"/></svg>',
  bell: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9ZM10 21h4"/></svg>',
  chevrons: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="m8 9 4-4 4 4m0 6-4 4-4-4"/></svg>',
  menu: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M4 7h16M4 12h16M4 17h16"/></svg>',
  search: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/></svg>',
  plus: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M12 5v14M5 12h14"/></svg>',
  bookmark: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M6 4h12v17l-6-4-6 4z"/></svg>',
  bookmarkFilled: '<svg viewBox="0 0 24 24" fill="currentColor" stroke="currentColor"><path d="M6 4h12v17l-6-4-6 4z"/></svg>',
  mail: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><rect x="3" y="5" width="18" height="14" rx="2"/><path d="m3 7 9 6 9-6"/></svg>',
  external: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M14 4h6v6M20 4l-9 9"/><path d="M18 13v6a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h6"/></svg>',
  check: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="m5 12 4 4L19 6"/></svg>',
  info: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="9"/><path d="M12 11v6m0-10h.01"/></svg>',
  file: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M6 3h8l4 4v14H6z"/><path d="M14 3v5h5M9 13h6m-6 4h6"/></svg>',
  filter: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M4 6h16M7 12h10m-7 6h4"/></svg>',
  clock: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>',
};

const state = {
  view: ["pipeline", "markets", "buyers", "suppliers", "watches"].includes(new URL(window.location.href).searchParams.get("view")) ? new URL(window.location.href).searchParams.get("view") : "pipeline",
  selectedId: null,
  sector: "All sectors",
  deadline: 90,
  minValue: 0,
  buyer: "",
  evidence: 0,
  sort: "deadline",
  tableSearch: "",
  saved: new Set(JSON.parse(localStorage.getItem("publicSignalSaved") || "[]")),
};

const watchTurnstile = {
  id: null,
  token: "",
  generation: 0,
};

const viewRoot = document.querySelector("#view-root");
const toast = document.querySelector("#toast");
const sidebar = document.querySelector("#sidebar");
const scrim = document.querySelector("#mobile-scrim");
let toastTimer;

function icon(name) {
  return icons[name] || "";
}

function injectIcons(root = document) {
  root.querySelectorAll("[data-icon]").forEach((node) => {
    node.innerHTML = icon(node.dataset.icon);
    node.setAttribute("aria-hidden", "true");
  });
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatCurrency(value) {
  if (!Number.isFinite(value) || value <= 0) return "Not stated";
  if (value >= 1_000_000) return `€${(value / 1_000_000).toLocaleString("en-IE", { maximumFractionDigits: 2 })}m`;
  if (value >= 1_000) return `€${Math.round(value / 1_000).toLocaleString("en-IE")}k`;
  return `€${value.toLocaleString("en-IE")}`;
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString("en-IE");
}

function formatSnapshotDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "refresh time unavailable";
  return `built ${new Intl.DateTimeFormat("en-IE", { dateStyle: "medium", timeStyle: "short", timeZone: "Europe/Dublin" }).format(date)}`;
}

function formatDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Not stated";
  return new Intl.DateTimeFormat("en-IE", { day: "numeric", month: "short", year: "numeric", timeZone: "Europe/Dublin" }).format(date);
}

function deadlineLabel(days) {
  if (!Number.isFinite(days) || days === 9999) return "Deadline not stated";
  if (days < 0) return "Closed";
  if (days === 0) return "Closes today";
  if (days === 1) return "Closes tomorrow";
  return `Closes in ${days} days`;
}

function sourceLabel(value) {
  return ({ ted_tender: "TED", national_live: "eTenders", dail_tracker: "Dáil Tracker", private_snapshot: "Dáil Tracker snapshot" })[value] || String(value || "Source").replaceAll("_", " ");
}

function classificationLabel(item) {
  const cpv = String(item.cpv || "").trim();
  const sector = String(item.sector || "Unclassified").trim();
  if (!cpv || cpv.toLowerCase() === sector.toLowerCase()) return sector;
  return `${sector} · CPV ${cpv}`;
}

function updateNavigationCounts() {
  const pipelineCount = document.querySelector('[data-view="pipeline"] .nav-count');
  const watchCount = document.querySelector('[data-view="watches"] .nav-count');
  if (pipelineCount) pipelineCount.textContent = opportunityFeed.state === "loading" ? "…" : String(opportunities.length);
  if (watchCount) watchCount.textContent = String(state.saved.size);
}

function hasEvidenceScore(item) {
  return Number.isFinite(item.evidence);
}

function liveOpportunity(item) {
  const evidence = item.evidence == null ? Number.NaN : Number(item.evidence);
  return {
    id: item.id,
    title: item.title || item.id,
    buyer: item.buyer || "Buyer not stated",
    sector: item.sector || "Unclassified",
    cpv: item.cpv || null,
    deadline: item.deadline || "Not stated",
    daysToDeadline: Number.isFinite(Number(item.daysToDeadline)) ? Number(item.daysToDeadline) : 9999,
    estimate: item.estimate != null && Number(item.estimate) > 0 ? Number(item.estimate) : null,
    evidence: Number.isFinite(evidence) ? evidence : null,
    source: item.source || "Procurement snapshot",
    sourceUrl: item.sourceUrl || "",
    published: "Current snapshot",
    coverage: ["Current procurement snapshot", "Source notice link"],
    caution: item.caution || "Review the source notice before relying on this summary.",
    liveSnapshot: true,
  };
}

async function loadLiveOpportunities() {
  try {
    const [response, healthResponse] = await Promise.all([
      fetch("/api/opportunities?within_days=365&limit=200"),
      fetch("/api/health").catch(() => null),
    ]);
    if (!response.ok) throw new Error("Opportunity feed unavailable");
    const payload = await response.json();
    if (healthResponse?.ok) {
      const health = await healthResponse.json();
      appCapabilities = { emailConfigured: health.emailConfigured === true, loaded: true };
    } else {
      appCapabilities = { emailConfigured: false, loaded: true };
    }
    if (!Array.isArray(payload.opportunities) || !payload.opportunities.length) throw new Error("Opportunity feed is empty");
    opportunities = payload.opportunities.map(liveOpportunity).filter((item) => item.id);
    if (!opportunities.length) throw new Error("Opportunity feed is incompatible");
    opportunityFeed = { state: "live", source: payload.source || "private_snapshot", builtAt: payload.builtAt || null };
    state.selectedId = null;
  } catch {
    opportunities = sampleOpportunities;
    state.selectedId = sampleOpportunities[0].id;
    opportunityFeed = { state: "fallback", source: "prototype", builtAt: null };
  }
  updateNavigationCounts();
  render();
}

function showToast(message) {
  clearTimeout(toastTimer);
  toast.textContent = message;
  toast.classList.add("is-visible");
  toastTimer = setTimeout(() => toast.classList.remove("is-visible"), 3400);
}

function syncNavigationState() {
  document.querySelectorAll(".nav-item").forEach((item) => {
    const active = item.dataset.view === state.view;
    item.classList.toggle("is-active", active);
    if (active) item.setAttribute("aria-current", "page");
    else item.removeAttribute("aria-current");
  });
}

function setView(view) {
  if (view !== "watches") resetWatchTurnstile();
  state.view = view;
  const viewUrl = new URL(window.location.href);
  if (view === "pipeline") viewUrl.searchParams.delete("view");
  else viewUrl.searchParams.set("view", view);
  window.history.replaceState(null, "", viewUrl);
  trackAnalytics("page_open", `page-${view}`);
  syncNavigationState();
  closeMobileNav();
  render();
  viewRoot.focus({ preventScroll: true });
}

function viewHeader(title, description, metaLabel, metaValue) {
  return `
    <header class="view-header">
      <div class="view-heading"><h1>${escapeHtml(title)}</h1><p>${escapeHtml(description)}</p></div>
      <div class="header-meta"><strong>${escapeHtml(metaValue)}</strong>${escapeHtml(metaLabel)}</div>
    </header>`;
}

function filteredOpportunities() {
  const buyerQuery = state.buyer.trim().toLowerCase();
  const filtered = opportunities.filter((item) => {
    const sectorMatch = state.sector === "All sectors" || item.sector === state.sector;
    const deadlineMatch = item.daysToDeadline <= Number(state.deadline);
    const valueMatch = state.minValue === 0 || (Number.isFinite(item.estimate) && item.estimate >= Number(state.minValue));
    const buyerMatch = !buyerQuery || item.buyer.toLowerCase().includes(buyerQuery) || item.title.toLowerCase().includes(buyerQuery);
    const evidenceMatch = state.evidence === 0 || (hasEvidenceScore(item) && item.evidence >= Number(state.evidence));
    return sectorMatch && deadlineMatch && valueMatch && buyerMatch && evidenceMatch;
  });
  return filtered.sort((left, right) => {
    if (state.sort === "value") return (right.estimate || 0) - (left.estimate || 0) || left.daysToDeadline - right.daysToDeadline;
    if (state.sort === "buyer") return left.buyer.localeCompare(right.buyer, "en-IE") || left.daysToDeadline - right.daysToDeadline;
    return left.daysToDeadline - right.daysToDeadline || left.title.localeCompare(right.title, "en-IE");
  });
}

function filtersAreActive() {
  return state.buyer.trim() || state.sector !== "All sectors" || state.deadline !== 90 || state.minValue !== 0 || state.evidence !== 0 || state.sort !== "deadline";
}

function renderPipeline() {
  const results = filteredOpportunities();
  if (!results.some((item) => item.id === state.selectedId)) state.selectedId = results[0]?.id || null;
  const selected = opportunities.find((item) => item.id === state.selectedId);
  const urgent = results.filter((item) => item.daysToDeadline <= 7).length;
  const valued = results.filter((item) => Number.isFinite(item.estimate)).length;
  const sourceCount = new Set(results.map((item) => item.source)).size;
  const highEvidence = results.filter((item) => hasEvidenceScore(item) && item.evidence >= 85).length;
  const sectorOptions = ["All sectors", ...new Set(opportunities.map((item) => item.sector))];
  const liveSnapshot = opportunityFeed.state === "live";
  const loadingSnapshot = opportunityFeed.state === "loading";
  const feedStatus = liveSnapshot
    ? `<section class="feed-status is-live" role="status"><span class="feed-status-dot" aria-hidden="true"></span><strong>Dáil Tracker snapshot connected</strong><span>${opportunities.length} source-linked notices loaded, ${formatSnapshotDate(opportunityFeed.builtAt)}</span></section>`
    : loadingSnapshot
      ? '<section class="feed-status is-loading" role="status"><span class="feed-status-dot" aria-hidden="true"></span><strong>Loading procurement snapshot</strong><span>Connecting to the current Dáil Tracker copy</span></section>'
      : '<section class="feed-status is-fallback" role="status"><span class="feed-status-dot" aria-hidden="true"></span><strong>Snapshot temporarily unavailable</strong><span>Showing labelled prototype records</span></section>';

  if (loadingSnapshot) {
    viewRoot.innerHTML = `
      ${viewHeader("Opportunity desk", "Connecting to the current procurement snapshot.", "Please wait", "Loading data")}
      ${feedStatus}
      <section class="pipeline-loading" aria-label="Loading opportunities" aria-busy="true">
        <div class="skeleton-filters"><span></span><span></span><span></span><span></span></div>
        <div class="skeleton-workspace"><div>${Array.from({ length: 6 }, () => '<span class="skeleton-row"></span>').join("")}</div><aside><span></span><span></span><span></span><span></span></aside></div>
      </section>`;
    return;
  }

  viewRoot.innerHTML = `
    ${viewHeader(
      "Opportunity desk",
      liveSnapshot ? "Browse the current procurement snapshot, then open the source notice or save a watch." : loadingSnapshot ? "Connecting to the current procurement snapshot." : "Browse the prototype workspace and sample evidence briefs.",
      liveSnapshot ? "Dáil Tracker snapshot" : loadingSnapshot ? "Please wait" : `TED ${snapshot.tedRefreshed}`,
      liveSnapshot ? `${opportunities.length} notices loaded` : loadingSnapshot ? "Loading data" : snapshot.label,
    )}
    ${feedStatus}
    <section class="summary-strip" aria-label="Opportunity summary">
      <div class="summary-item"><strong>${results.length}</strong><span>matched loaded notices</span></div>
      <div class="summary-item"><strong>${urgent}</strong><span>closing within 7 days</span></div>
      <div class="summary-item"><strong>${liveSnapshot ? valued : loadingSnapshot ? "…" : highEvidence}</strong><span>${liveSnapshot ? "with advertised value" : loadingSnapshot ? "checking provenance" : "strong evidence coverage"}</span></div>
      <div class="summary-item"><strong>${liveSnapshot ? sourceCount : loadingSnapshot ? "…" : snapshot.tedOpen}</strong><span>${liveSnapshot ? "procurement sources" : loadingSnapshot ? "connecting feed" : "open TED notices in snapshot"}</span></div>
    </section>
    <section class="filters ${liveSnapshot ? "is-live" : ""}" aria-label="Opportunity filters">
      <div class="field"><label for="buyer-filter">Buyer, notice or keyword</label><input id="buyer-filter" type="search" value="${escapeHtml(state.buyer)}" placeholder="Try Tipperary or engineering" /></div>
      <div class="field"><label for="sector-filter">Sector</label><select id="sector-filter">${sectorOptions.map((value) => `<option ${value === state.sector ? "selected" : ""}>${escapeHtml(value)}</option>`).join("")}</select></div>
      <div class="field"><label for="deadline-filter">Deadline</label><select id="deadline-filter">
        <option value="14" ${state.deadline === 14 ? "selected" : ""}>Next 14 days</option>
        <option value="30" ${state.deadline === 30 ? "selected" : ""}>Next 30 days</option>
        <option value="60" ${state.deadline === 60 ? "selected" : ""}>Next 60 days</option>
        <option value="90" ${state.deadline === 90 ? "selected" : ""}>Next 90 days</option>
      </select></div>
      <div class="field"><label for="value-filter">Advertised estimate</label><select id="value-filter">
        <option value="0" ${state.minValue === 0 ? "selected" : ""}>Any value</option>
        <option value="250000" ${state.minValue === 250000 ? "selected" : ""}>€250k and above</option>
        <option value="1000000" ${state.minValue === 1000000 ? "selected" : ""}>€1m and above</option>
        <option value="5000000" ${state.minValue === 5000000 ? "selected" : ""}>€5m and above</option>
      </select></div>
      ${liveSnapshot ? `<div class="field"><label for="sort-filter">Sort by</label><select id="sort-filter">
        <option value="deadline" ${state.sort === "deadline" ? "selected" : ""}>Deadline soonest</option>
        <option value="value" ${state.sort === "value" ? "selected" : ""}>Highest value</option>
        <option value="buyer" ${state.sort === "buyer" ? "selected" : ""}>Buyer name</option>
      </select></div>` : `<div class="field"><label for="evidence-filter">Evidence coverage</label><select id="evidence-filter">
        <option value="0" ${state.evidence === 0 ? "selected" : ""}>Any coverage</option>
        <option value="70" ${state.evidence === 70 ? "selected" : ""}>70% and above</option>
        <option value="85" ${state.evidence === 85 ? "selected" : ""}>85% and above</option>
        <option value="90" ${state.evidence === 90 ? "selected" : ""}>90% and above</option>
      </select></div>`}
      <div class="filter-actions"><span>Showing ${results.length} of ${opportunities.length} loaded</span>${filtersAreActive() ? '<button type="button" id="clear-filters">Reset</button>' : ""}</div>
    </section>
    ${liveSnapshot ? '<p class="filters-note">Evidence scores are not assigned to snapshot notices. Every record links to its originating notice for review.</p>' : ""}
    <section class="pipeline-workspace">
      <div class="opportunity-list" aria-label="Filtered opportunities">
        <div class="list-header"><span>Opportunity</span><span>Buyer</span><span>Value</span><span>Deadline</span><span>Source</span></div>
        ${results.length ? results.map(opportunityRow).join("") : emptyResults()}
      </div>
      ${selected ? detailPane(selected) : '<aside class="detail-pane"><p>Select an opportunity to inspect its source record.</p></aside>'}
    </section>`;

  bindPipelineEvents();
}

function opportunityRow(item) {
  const selected = item.id === state.selectedId;
  return `
    <button class="opportunity-row ${selected ? "is-selected" : ""}" type="button" data-opportunity="${item.id}" aria-pressed="${selected}">
      <span class="opportunity-name" data-mobile-estimate="${Number.isFinite(item.estimate) ? `${formatCurrency(item.estimate)} advertised estimate` : "Value not stated"}"><strong>${escapeHtml(item.title)}</strong><span>${escapeHtml(classificationLabel(item))}</span></span>
      <span class="buyer-cell"><strong>${escapeHtml(item.buyer)}</strong><span>${escapeHtml(sourceLabel(item.source))}</span></span>
      <span class="money-cell"><strong>${formatCurrency(item.estimate)}</strong><span>${Number.isFinite(item.estimate) ? "advertised value" : "value not stated"}</span></span>
      <span class="deadline-cell ${item.daysToDeadline <= 7 ? "is-urgent" : ""}"><strong>${deadlineLabel(item.daysToDeadline)}</strong><span>${formatDate(item.deadline)}</span></span>
      ${hasEvidenceScore(item)
    ? `<span class="evidence-ring" style="--score:${item.evidence}" title="${item.evidence}% evidence coverage" aria-label="${item.evidence}% evidence coverage"></span>`
    : `<span class="source-badge">${escapeHtml(sourceLabel(item.source))}</span>`}
    </button>`;
}

function emptyResults() {
  return `<div class="empty-state"><div><span class="empty-icon">${icon("filter")}</span><h2>No notices match these filters</h2><p>Broaden the deadline, sector or value range. A zero result is retained as useful information.</p><button class="button" type="button" id="clear-filters">Clear filters</button></div></div>`;
}

function detailPane(item) {
  const saved = state.saved.has(item.id);
  if (item.liveSnapshot) return liveDetailPane(item, saved);
  const singleBid = item.signals.singleBidPct == null ? "Sample too small" : `${item.signals.singleBidPct}% of lots`;
  return `
    <aside class="detail-pane" aria-label="Opportunity evidence brief">
      <div class="detail-kicker"><span class="source-badge">${item.source}</span><span class="sector-badge">${escapeHtml(item.sector)}</span></div>
      <h2>${escapeHtml(item.title)}</h2>
      <p class="detail-buyer">${escapeHtml(item.buyer)} · Notice ${item.id}</p>
      <div class="detail-actions">
        <button class="button ${saved ? "" : "button-primary"}" type="button" id="save-opportunity">${icon(saved ? "bookmarkFilled" : "bookmark")}<span>${saved ? "Watching" : "Watch notice"}</span></button>
        <button class="button" type="button" id="email-brief">${icon("mail")}<span>Email brief</span></button>
        <a class="button button-quiet" data-analytics-event="source_notice_open" href="${item.sourceUrl}" target="_blank" rel="noreferrer">${icon("external")}<span>Source</span></a>
      </div>
      <div class="fact-line">
        <div class="fact"><strong>${formatCurrency(item.estimate)}</strong><span>advertised estimate</span></div>
        <div class="fact"><strong>${item.deadline}</strong><span>submission deadline</span></div>
        <div class="fact"><strong>${item.evidence}%</strong><span>evidence coverage</span></div>
      </div>
      <div class="section-heading"><h3>Buyer and market evidence</h3><a class="source-link" data-analytics-event="source_notice_open" href="${item.sourceUrl}" target="_blank" rel="noreferrer">Open notice ${icon("external")}</a></div>
      <ul class="signal-list">
        <li><span>Previous buyer awards</span><strong>${formatNumber(item.signals.buyerAwards)}</strong></li>
        <li><span>Previous awards in this sector</span><strong>${formatNumber(item.signals.sectorAwards)}</strong></li>
        <li><span>Known company suppliers in sector</span><strong>${formatNumber(item.signals.knownSuppliers)}</strong></li>
        <li><span>Recent buyer competition sample</span><strong>${formatNumber(item.signals.competitionLots)} lots</strong></li>
        <li><span>Single-bid context</span><strong>${singleBid}</strong></li>
        <li><span>Related estimated expiries</span><strong>${formatNumber(item.signals.expiring)}</strong></li>
        <li><span>${escapeHtml(item.signals.paymentMeaning)}</span><strong>${formatNumber(item.signals.paymentLines)} lines</strong></li>
      </ul>
      <div class="section-heading"><h3>Evidence included</h3><button type="button" id="coverage-help">How coverage works</button></div>
      <div class="coverage-list">${item.coverage.map((label) => `<span class="coverage-item">${icon("check")}${escapeHtml(label)}</span>`).join("")}</div>
      <div class="caution-box"><strong>Analyst note:</strong> ${escapeHtml(item.caution)}</div>
    </aside>`;
}

function liveDetailPane(item, saved) {
  const sourceAction = item.sourceUrl
    ? `<a class="button button-quiet" data-analytics-event="source_notice_open" href="${escapeHtml(item.sourceUrl)}" target="_blank" rel="noreferrer">${icon("external")}<span>Source notice</span></a>`
    : "";
  return `
    <aside class="detail-pane" aria-label="Opportunity source brief">
      <div class="detail-kicker"><span class="source-badge">${escapeHtml(sourceLabel(item.source))}</span><span class="sector-badge">${escapeHtml(item.sector)}</span></div>
      <h2>${escapeHtml(item.title)}</h2>
      <p class="detail-buyer">${escapeHtml(item.buyer)} · Notice ${escapeHtml(item.id)}</p>
      <div class="detail-actions">
        <button class="button ${saved ? "" : "button-primary"}" type="button" id="save-opportunity">${icon(saved ? "bookmarkFilled" : "bookmark")}<span>${saved ? "Watching" : "Watch notice"}</span></button>
        <button class="button" type="button" id="email-brief">${icon("mail")}<span>${appCapabilities.emailConfigured ? "Email watch" : "Build watch"}</span></button>
        ${sourceAction}
      </div>
      <div class="fact-line">
        <div class="fact"><strong>${formatCurrency(item.estimate)}</strong><span>advertised estimate</span></div>
        <div class="fact"><strong>${formatDate(item.deadline)}</strong><span>${deadlineLabel(item.daysToDeadline)}</span></div>
        <div class="fact"><strong>Not scored</strong><span>evidence coverage</span></div>
      </div>
      <div class="section-heading"><h3>Snapshot record</h3>${item.sourceUrl ? `<a class="source-link" data-analytics-event="source_notice_open" href="${escapeHtml(item.sourceUrl)}" target="_blank" rel="noreferrer">Open source notice ${icon("external")}</a>` : ""}</div>
      <ul class="signal-list">
        <li><span>Source</span><strong>${escapeHtml(sourceLabel(item.source))}</strong></li>
        <li><span>Classification</span><strong>${escapeHtml(classificationLabel(item))}</strong></li>
        <li><span>Deadline status</span><strong>${deadlineLabel(item.daysToDeadline)}</strong></li>
      </ul>
      <div class="section-heading"><h3>Evidence included</h3></div>
      <div class="coverage-list">${item.coverage.map((label) => `<span class="coverage-item">${icon("check")}${escapeHtml(label)}</span>`).join("")}</div>
      <div class="caution-box"><strong>Review note:</strong> ${escapeHtml(item.caution)} Historic buyer and supplier signals are not inferred into this public notice view.</div>
    </aside>`;
}

function bindPipelineEvents() {
  document.querySelectorAll("[data-opportunity]").forEach((row) => row.addEventListener("click", () => {
    state.selectedId = row.dataset.opportunity;
    trackAnalytics("opportunity_brief_open", "opportunity-brief");
    renderPipeline();
    if (window.matchMedia("(max-width: 960px)").matches) {
      requestAnimationFrame(() => document.querySelector(".detail-pane")?.scrollIntoView({ behavior: "smooth", block: "start" }));
    }
  }));
  const mappings = [
    ["buyer-filter", "buyer", "input", (value) => value],
    ["sector-filter", "sector", "change", (value) => value],
    ["deadline-filter", "deadline", "change", Number],
    ["value-filter", "minValue", "change", Number],
    ["sort-filter", "sort", "change", (value) => value],
    ["evidence-filter", "evidence", "change", Number],
  ];
  mappings.forEach(([id, key, event, transform]) => document.querySelector(`#${id}`)?.addEventListener(event, (e) => {
    state[key] = transform(e.target.value);
    const target = { sector: "filter-sector", deadline: "filter-deadline", minValue: "filter-value", evidence: "filter-evidence", buyer: "filter-buyer" }[key];
    if (target && (event !== "input" || e.target.value.trim())) trackAnalytics("filter_apply", target);
    if (event === "input") window.clearTimeout(e.target._timer);
    if (event === "input") e.target._timer = window.setTimeout(renderPipeline, 120);
    else renderPipeline();
  }));
  document.querySelector("#clear-filters")?.addEventListener("click", () => {
    Object.assign(state, { sector: "All sectors", deadline: 90, minValue: 0, buyer: "", evidence: 0, sort: "deadline" });
    trackAnalytics("filter_apply", "filter-sector");
    renderPipeline();
  });
  document.querySelector("#save-opportunity")?.addEventListener("click", () => {
    const adding = !state.saved.has(state.selectedId);
    if (adding) state.saved.add(state.selectedId);
    else state.saved.delete(state.selectedId);
    localStorage.setItem("publicSignalSaved", JSON.stringify([...state.saved]));
    updateNavigationCounts();
    if (adding) trackAnalytics("opportunity_saved", "notice-bookmark");
    showToast(state.saved.has(state.selectedId) ? "Notice added to your watchlist." : "Notice removed from your watchlist.");
    renderPipeline();
  });
  document.querySelector("#email-brief")?.addEventListener("click", () => {
    trackAnalytics("primary_cta_click", "primary-cta");
    trackAnalytics("watch_start", "watch-start");
    setView("watches");
  });
  document.querySelector("#coverage-help")?.addEventListener("click", () => showToast("Coverage reflects available source lanes. It is not a confidence score or bid recommendation."));
  injectIcons(viewRoot);
}

function prototypeNotice(message) {
  return `<section class="feed-status is-preview" role="note"><span class="feed-status-dot" aria-hidden="true"></span><strong>Research preview</strong><span>${escapeHtml(message)} These figures are not part of the live opportunity feed.</span></section>`;
}

function renderMarkets() {
  viewRoot.innerHTML = `
    ${viewHeader("Sector map", "Compare where the corpus is commercially useful before building a watch or commissioning a brief.", "Rows are not market-size estimates", "Evidence readiness")}
    ${prototypeNotice("Illustrative sector analysis from the development corpus.")}
    <section class="summary-strip" aria-label="Sector data summary">
      <div class="summary-item"><strong>20,597</strong><span>awards with usable CPV</span></div>
      <div class="summary-item"><strong>49,225</strong><span>awards with bid counts</span></div>
      <div class="summary-item"><strong>2,655</strong><span>expiry signals across sources</span></div>
      <div class="summary-item"><strong>1,393</strong><span>entities in three registers</span></div>
    </section>
    <section class="table-region">
      <div class="table-toolbar"><div><h2>Sector evidence profile</h2><p>Classified national awards, national expiry signals and current TED notices.</p></div><div class="field"><label for="table-search">Filter sectors</label><input id="table-search" type="search" value="${escapeHtml(state.tableSearch)}" placeholder="Search by sector or CPV" /></div></div>
      <div class="data-table-wrap"><table class="data-table"><thead><tr><th>Sector</th><th>CPV</th><th class="numeric">Awards</th><th class="numeric">Valued</th><th>Bid-count coverage</th><th class="numeric">18m expiries</th><th class="numeric">Open TED</th><th>Commercial readiness</th></tr></thead><tbody>
        ${sectors.filter((s) => `${s.name} ${s.cpv}`.toLowerCase().includes(state.tableSearch.toLowerCase())).map((sector) => `<tr><td><strong>${escapeHtml(sector.name)}</strong></td><td>${sector.cpv}</td><td class="numeric">${formatNumber(sector.awards)}</td><td class="numeric">${formatNumber(sector.valued)} <small>${Math.round((sector.valued / sector.awards) * 100)}%</small></td><td><div class="coverage-bar" aria-label="${sector.bidCoverage}%"><span style="width:${sector.bidCoverage}%"></span></div><small>${sector.bidCoverage}%</small></td><td class="numeric">${sector.expiries}</td><td class="numeric">${sector.openTed}</td><td><span class="quality-badge ${sector.readiness.startsWith("Strong") ? "is-strong" : "is-developing"}">${escapeHtml(sector.readiness)}</span></td></tr>`).join("")}
      </tbody></table></div>
    </section>
    <div class="page-note">${icon("info")}<span>Award values, advertised estimates, disclosed payments and purchase orders remain separate. Expiry dates are procurement signals derived from advertised terms, not verified contract events.</span></div>`;
  bindTableSearch();
}

function renderBuyers() {
  viewRoot.innerHTML = `
    ${viewHeader("Buyer dossiers", "Find public bodies with enough award, competition and payment evidence to support account planning.", "Payment meaning shown per publisher", "Buyer evidence")}
    ${prototypeNotice("Illustrative buyer profiles pending a reviewed public data contract.")}
    <section class="table-region">
      <div class="table-toolbar"><div><h2>Evidence-rich buyers</h2><p>High-volume examples from the current corpus.</p></div><div class="field"><label for="table-search">Find a buyer</label><input id="table-search" type="search" value="${escapeHtml(state.tableSearch)}" placeholder="Council, department or agency" /></div></div>
      <div class="data-table-wrap"><table class="data-table"><thead><tr><th>Buyer</th><th class="numeric">Awards</th><th class="numeric">Known suppliers</th><th class="numeric">Disclosure lines</th><th>Money meaning</th><th class="numeric">Competition lots</th><th class="numeric">Single-bid context</th><th class="numeric">Expiry signals</th></tr></thead><tbody>
        ${buyers.filter((b) => b.name.toLowerCase().includes(state.tableSearch.toLowerCase())).map((buyer) => `<tr><td><strong>${escapeHtml(buyer.name)}</strong></td><td class="numeric">${formatNumber(buyer.awards)}</td><td class="numeric">${formatNumber(buyer.suppliers)}</td><td class="numeric">${formatNumber(buyer.paymentLines)}</td><td><span class="state-badge ${buyer.money === "Spent" ? "is-active" : ""}">${buyer.money}</span></td><td class="numeric">${buyer.lots}</td><td class="numeric">${buyer.singleBidPct}%</td><td class="numeric">${buyer.expiries}</td></tr>`).join("")}
      </tbody></table></div>
    </section>
    <div class="page-note">${icon("info")}<span>Single-bid rates are factual context with a denominator, never a buyer score. Central purchasing bodies also require separate interpretation from ordinary contracting authorities.</span></div>`;
  bindTableSearch();
}

function renderSuppliers() {
  viewRoot.innerHTML = `
    ${viewHeader("Supplier footprints", "Trace known company activity across national awards, TED and public-body disclosures.", "Human verification required", "Entity chain")}
    ${prototypeNotice("Illustrative supplier profiles pending entity-level review.")}
    <section class="table-region">
      <div class="table-toolbar"><div><h2>Cross-register supplier examples</h2><p>Company-number anchored where the source match supports it.</p></div><div class="field"><label for="table-search">Find a supplier</label><input id="table-search" type="search" value="${escapeHtml(state.tableSearch)}" placeholder="Company or CRO number" /></div></div>
      <div class="data-table-wrap"><table class="data-table"><thead><tr><th>Supplier</th><th>CRO</th><th class="numeric">National awards</th><th class="numeric">National buyers</th><th class="numeric">TED awards</th><th class="numeric">Disclosure lines</th><th class="numeric">Publishers</th><th>Entity status</th></tr></thead><tbody>
        ${suppliers.filter((s) => `${s.name} ${s.company}`.toLowerCase().includes(state.tableSearch.toLowerCase())).map((supplier) => `<tr><td><strong>${escapeHtml(supplier.name)}</strong></td><td>${supplier.company}</td><td class="numeric">${supplier.awards}</td><td class="numeric">${supplier.authorities}</td><td class="numeric">${supplier.tedAwards}</td><td class="numeric">${formatNumber(supplier.paymentLines)}</td><td class="numeric">${supplier.publishers}</td><td><span class="quality-badge ${supplier.match.startsWith("Verified") ? "is-strong" : "is-developing"}">${escapeHtml(supplier.match)}</span></td></tr>`).join("")}
      </tbody></table></div>
    </section>
    <div class="page-note">${icon("info")}<span>Trading names, acquisitions, professional partnerships and ambiguous CRO matches can create false joins. Paid briefs should verify the subject entity before making a current-status claim.</span></div>`;
  bindTableSearch();
}

function bindTableSearch() {
  document.querySelector("#table-search")?.addEventListener("input", (event) => {
    state.tableSearch = event.target.value;
    if (event.target.value.trim()) trackAnalytics("table_search_apply", "table-search");
    window.clearTimeout(event.target._timer);
    event.target._timer = window.setTimeout(render, 100);
  });
  injectIcons(viewRoot);
}

function savedNoticesMarkup() {
  const savedItems = opportunities.filter((item) => state.saved.has(item.id));
  if (!savedItems.length) {
    return `<div class="saved-empty"><span>${icon("bookmark")}</span><h3>No saved notices yet</h3><p>Save a notice from the Opportunity desk to keep it close while you assess the source.</p><button class="button" type="button" data-view="pipeline">Browse opportunities</button></div>`;
  }
  return savedItems.slice(0, 10).map((item) => `
    <button class="watch-rule saved-rule" type="button" data-saved-opportunity="${escapeHtml(item.id)}">
      <span class="watch-rule-head"><strong>${escapeHtml(item.title)}</strong><span class="state-badge">Saved</span></span>
      <span class="saved-rule-buyer">${escapeHtml(item.buyer)}</span>
      <span class="watch-rule-meta"><span>${deadlineLabel(item.daysToDeadline)}</span><span>${formatCurrency(item.estimate)}</span></span>
    </button>`).join("");
}

function renderWatches() {
  const selected = opportunities.find((item) => item.id === state.selectedId);
  const watchSectors = [...new Set(opportunities.map((item) => item.sector))].slice(0, 8);
  const liveSnapshot = opportunityFeed.state === "live";
  const deliveryReady = appCapabilities.emailConfigured;
  viewRoot.innerHTML = `
    ${viewHeader("Watches & digests", "Save useful notices and turn focused filters into an email digest when delivery is enabled.", deliveryReady ? "07:10 UTC on weekdays" : "Email provider not connected", deliveryReady ? "Automated delivery" : "Draft mode")}
    ${deliveryReady ? "" : '<section class="feed-status is-preview" role="status"><span class="feed-status-dot" aria-hidden="true"></span><strong>Email delivery is not active</strong><span>You can save a watch draft locally. Connect the verified sending service before inviting subscribers.</span></section>'}
    <section class="watches-layout">
      <div class="watch-list">
        <div class="panel-heading"><div><h2>Saved notices</h2><p>Stored in this browser for quick review.</p></div><span class="state-badge">${state.saved.size} saved</span></div>
        ${savedNoticesMarkup()}
      </div>
      <div class="automation-builder">
        <div class="panel-heading"><div><h2>${deliveryReady ? "Create an email watch" : "Create a watch draft"}</h2><p>${liveSnapshot ? "Filter the current snapshot by sector, buyer, value and deadline." : "Only send when a notice clears your evidence threshold."}</p></div>${icon("mail")}</div>
        <form class="automation-form" id="watch-form">
          <div class="form-grid">
            <div class="field"><label for="watch-name">Watch name</label><input id="watch-name" name="name" value="${selected ? escapeHtml(`${selected.sector} opportunities`) : "New opportunity watch"}" required /></div>
            <div class="field"><label for="watch-email">Delivery email</label><input id="watch-email" name="email" type="email" placeholder="${deliveryReady ? "bidteam@company.ie" : "Delivery not configured"}" ${deliveryReady ? "required" : "disabled"} /></div>
            <div class="field is-wide"><span class="field-label">Sectors</span><div class="check-group">
              ${watchSectors.map((sector) => `<label class="check-pill"><input type="checkbox" name="sectors" value="${escapeHtml(sector)}" ${selected?.sector === sector ? "checked" : ""} /><span>${escapeHtml(sector)}</span></label>`).join("")}
            </div></div>
            <div class="field"><label for="watch-buyers">Buyer name contains</label><input id="watch-buyers" name="buyers" placeholder="Council, HSE, OPW" value="${selected ? escapeHtml(selected.buyer) : ""}" /></div>
            <div class="field"><label for="watch-value">Minimum advertised estimate</label><select id="watch-value" name="minValue"><option value="0">Any value</option><option value="250000">€250k</option><option value="500000">€500k</option><option value="1000000">€1m</option><option value="5000000">€5m</option></select></div>
            <div class="field"><label for="watch-deadline">Deadline window</label><select id="watch-deadline" name="deadline"><option value="30">Next 30 days</option><option value="60" selected>Next 60 days</option><option value="90">Next 90 days</option></select></div>
            <div class="field"><label for="watch-cadence">Digest cadence</label><select id="watch-cadence" name="cadence"><option value="weekday">Weekday morning</option><option value="weekly">Monday summary</option></select></div>
            <div class="field is-wide"><label for="watch-evidence">Minimum evidence coverage</label><div class="range-line"><input id="watch-evidence" name="evidence" type="range" min="0" max="95" step="5" value="${liveSnapshot ? "0" : "70"}" ${liveSnapshot ? "disabled" : ""} /><output class="range-value" for="watch-evidence">${liveSnapshot ? "Not scored" : "70%"}</output></div>${liveSnapshot ? '<small>Current snapshot notices have source links but are not assigned an evidence score.</small>' : ""}</div>
          </div>
          <div class="turnstile-field" id="turnstile-field" hidden>
            <div id="turnstile-widget"></div>
            <p id="turnstile-status" role="status"></p>
          </div>
          <div class="automation-preview" aria-live="polite"><h3>Digest preview</h3><div id="watch-preview"></div></div>
          <div class="form-actions"><button class="button button-primary" type="submit">${icon("bell")}<span>${deliveryReady ? "Save watch" : "Save draft"}</span></button><button class="button" type="button" id="preview-email">${icon("mail")}<span>Preview matches</span></button><small>${deliveryReady ? "Double opt-in. Unsubscribe link in every email." : "Drafts stay in this browser until delivery is connected."}</small></div>
        </form>
      </div>
    </section>`;
  bindWatchForm();
  injectIcons(viewRoot);
}

function getWatchFormValues() {
  const form = document.querySelector("#watch-form");
  const data = new FormData(form);
  return {
    name: String(data.get("name") || "Opportunity watch"),
    email: String(data.get("email") || ""),
    sectors: data.getAll("sectors").map(String),
    buyers: String(data.get("buyers") || "").split(",").map((value) => value.trim()).filter(Boolean),
    minValue: Number(data.get("minValue") || 0),
    deadlineDays: Number(data.get("deadline") || 60),
    cadence: String(data.get("cadence") || "weekday"),
    minEvidence: Number(data.get("evidence") || 0),
    includeExpiries: true,
    turnstileToken: watchTurnstile.token,
  };
}

function resetWatchTurnstile() {
  watchTurnstile.generation += 1;
  try {
    if (watchTurnstile.id !== null && typeof window.turnstile?.reset === "function") window.turnstile.reset(watchTurnstile.id);
  } catch {
    // A stale widget must never prevent navigation.
  }
  document.querySelector("#turnstile-widget")?.replaceChildren();
  watchTurnstile.id = null;
  watchTurnstile.token = "";
}

async function turnstileApi(generation) {
  if (window.turnstile) return window.turnstile;
  let loader = document.querySelector("script[data-turnstile-loader]");
  if (!loader) {
    loader = document.createElement("script");
    loader.src = "https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit";
    loader.async = true;
    loader.defer = true;
    loader.dataset.turnstileLoader = "true";
    document.head.appendChild(loader);
  }
  const deadline = Date.now() + 8_000;
  while (!window.turnstile && Date.now() < deadline && generation === watchTurnstile.generation) {
    await new Promise((resolve) => window.setTimeout(resolve, 50));
  }
  return generation === watchTurnstile.generation ? window.turnstile || null : null;
}

async function configureTurnstile(form) {
  const generation = watchTurnstile.generation;
  const field = document.querySelector("#turnstile-field");
  const status = document.querySelector("#turnstile-status");
  try {
    const response = await fetch("/api/config");
    if (!response.ok) throw new Error("Verification settings are unavailable.");
    const config = await response.json();
    if (generation !== watchTurnstile.generation || !form.isConnected) return;
    if (!config.turnstileSiteKey) return;

    field.hidden = false;
    form.dataset.turnstileRequired = "true";
    status.textContent = "Complete the verification before saving this watch.";
    const api = await turnstileApi(generation);
    if (generation !== watchTurnstile.generation || !form.isConnected) return;
    if (!api) throw new Error("Verification did not load. Refresh the page and try again.");
    watchTurnstile.id = api.render("#turnstile-widget", {
      sitekey: config.turnstileSiteKey,
      theme: "light",
      callback: (token) => {
        if (generation !== watchTurnstile.generation || !form.isConnected) return;
        watchTurnstile.token = token;
        status.textContent = "Verification complete.";
      },
      "expired-callback": () => {
        if (generation !== watchTurnstile.generation || !form.isConnected) return;
        watchTurnstile.token = "";
        status.textContent = "Verification expired. Complete it again before saving.";
      },
      "error-callback": () => {
        if (generation !== watchTurnstile.generation || !form.isConnected) return;
        watchTurnstile.token = "";
        status.textContent = "Verification could not load. Refresh the page and try again.";
      },
    });
  } catch (error) {
    if (generation !== watchTurnstile.generation || !form.isConnected) return;
    field.hidden = false;
    status.textContent = error.message || "Verification is unavailable. Try again shortly.";
  }
}

function watchMatches(values) {
  return opportunities.filter((item) => {
    const sectorMatch = !values.sectors.length || values.sectors.includes(item.sector);
    const buyerMatch = !values.buyers.length || values.buyers.some((buyer) => item.buyer.toLowerCase().includes(buyer.toLowerCase()));
    const evidenceMatch = values.minEvidence === 0 || (hasEvidenceScore(item) && item.evidence >= values.minEvidence);
    return sectorMatch && buyerMatch && item.estimate >= values.minValue && item.daysToDeadline <= values.deadlineDays && evidenceMatch;
  });
}

function updateWatchPreview() {
  const values = getWatchFormValues();
  const matches = watchMatches(values);
  document.querySelector(".range-value").textContent = opportunityFeed.state === "live" ? "Not scored" : `${values.minEvidence}%`;
  document.querySelector("#watch-preview").innerHTML = `
    <div class="preview-line"><span>Current matches</span><strong>${matches.length}</strong></div>
    <div class="preview-line"><span>Next delivery</span><strong>${appCapabilities.emailConfigured ? values.cadence === "weekly" ? "Monday, 07:10 UTC" : "Next weekday, 07:10 UTC" : "Not scheduled"}</strong></div>
    <div class="preview-line"><span>Evidence rule</span><strong>${opportunityFeed.state === "live" ? "No score filter" : `${values.minEvidence}% or better`}</strong></div>
    <div class="preview-line"><span>Lead item</span><strong>${matches[0] ? escapeHtml(matches[0].buyer) : "No current match"}</strong></div>`;
}

function bindWatchForm() {
  const form = document.querySelector("#watch-form");
  resetWatchTurnstile();
  document.querySelectorAll("[data-saved-opportunity]").forEach((button) => button.addEventListener("click", () => {
    state.selectedId = button.dataset.savedOpportunity;
    setView("pipeline");
  }));
  document.querySelector(".saved-empty [data-view='pipeline']")?.addEventListener("click", () => setView("pipeline"));
  form.addEventListener("input", updateWatchPreview);
  form.addEventListener("change", updateWatchPreview);
  updateWatchPreview();
  document.querySelector("#preview-email").addEventListener("click", () => {
    trackAnalytics("watch_preview", "watch-preview");
    const matches = watchMatches(getWatchFormValues());
    showToast(matches.length ? `Preview ready with ${matches.length} matching opportunities.` : "Preview ready. No current notices clear this rule.");
  });
  if (appCapabilities.emailConfigured) void configureTurnstile(form);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = form.querySelector('button[type="submit"]');
    const values = getWatchFormValues();
    if (!appCapabilities.emailConfigured) {
      const { email: _email, turnstileToken: _turnstileToken, ...draft } = values;
      localStorage.setItem("publicSignalDraftWatch", JSON.stringify(draft));
      showToast("Watch draft saved in this browser. Email delivery is not connected yet.");
      return;
    }
    if (form.dataset.turnstileRequired === "true" && !values.turnstileToken) {
      showToast("Complete the verification before saving this watch.");
      return;
    }
    button.disabled = true;
    button.textContent = "Saving…";
    let apiResponded = false;
    try {
      const response = await fetch("/api/subscriptions", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(values) });
      apiResponded = true;
      if (!response.ok) throw new Error((await response.json()).error || "Unable to save watch");
      const result = await response.json();
      trackAnalytics("watch_saved", "watch-saved");
      showToast(result.emailSent ? "Watch saved. Check your inbox to confirm delivery." : "Watch saved in preview mode. Add Resend credentials to send confirmation email.");
    } catch (error) {
      if (apiResponded) {
        showToast(error.message || "Unable to save watch.");
      } else {
        localStorage.setItem("publicSignalDraftWatch", JSON.stringify(values));
        showToast("Prototype watch saved in this browser. Connect the Worker API to activate email.");
      }
    } finally {
      button.disabled = false;
      button.innerHTML = `${icon("bell")}<span>${appCapabilities.emailConfigured ? "Save watch" : "Save draft"}</span>`;
    }
  });
}

function render() {
  const renderers = { pipeline: renderPipeline, markets: renderMarkets, buyers: renderBuyers, suppliers: renderSuppliers, watches: renderWatches };
  renderers[state.view]?.();
}

function openMobileNav() {
  sidebar.classList.add("is-open");
  scrim.hidden = false;
  document.querySelector("#mobile-menu").setAttribute("aria-expanded", "true");
}

function closeMobileNav() {
  sidebar.classList.remove("is-open");
  scrim.hidden = true;
  document.querySelector("#mobile-menu").setAttribute("aria-expanded", "false");
}

document.querySelectorAll("[data-view]").forEach((button) => button.addEventListener("click", () => {
  if (button.dataset.view === "watches") {
    trackAnalytics("watch_start", "watch-start");
    if (button.classList.contains("button-primary")) trackAnalytics("primary_cta_click", "primary-cta");
  }
  setView(button.dataset.view);
}));
document.querySelector("#mobile-menu").addEventListener("click", () => sidebar.classList.contains("is-open") ? closeMobileNav() : openMobileNav());
scrim.addEventListener("click", closeMobileNav);
document.querySelector("#global-search").addEventListener("click", () => {
  if (state.view !== "pipeline") setView("pipeline");
  requestAnimationFrame(() => document.querySelector("#buyer-filter")?.focus());
});
document.addEventListener("keydown", (event) => {
  if (event.key === "/" && !["INPUT", "SELECT", "TEXTAREA"].includes(document.activeElement.tagName)) {
    event.preventDefault();
    if (state.view !== "pipeline") setView("pipeline");
    requestAnimationFrame(() => document.querySelector("#buyer-filter")?.focus());
  }
  if (event.key === "Escape") closeMobileNav();
});

document.addEventListener("click", (event) => {
  const target = event.target.closest?.("[data-analytics-event]");
  if (target?.dataset.analyticsEvent === "source_notice_open") trackAnalytics("source_notice_open", "source-notice");
});

injectIcons();
syncNavigationState();
updateNavigationCounts();
render();
void loadLiveOpportunities();
trackAnalytics("app_open", "app");
trackAnalytics("page_open", "page-pipeline");
