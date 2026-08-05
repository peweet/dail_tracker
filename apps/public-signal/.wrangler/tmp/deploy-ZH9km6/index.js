var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// public/sample-data.js
var opportunities = [
  {
    id: "530726-2026",
    title: "Civil engineering consultancy services",
    buyer: "Tipperary County Council",
    sector: "Architecture & Engineering",
    cpv: "71311000",
    deadline: "27 Aug 2026",
    daysToDeadline: 22,
    estimate: 2e6,
    evidence: 94,
    source: "TED",
    sourceUrl: "https://ted.europa.eu/en/notice/-/detail/530726-2026",
    published: "30 Jul 2026",
    procedure: "Open procedure",
    signals: {
      buyerAwards: 789,
      sectorAwards: 41,
      knownSuppliers: 24,
      competitionLots: 86,
      singleBidPct: 5.8,
      expiring: 6,
      paymentLines: 7034,
      paymentMeaning: "Committed purchase orders"
    },
    coverage: ["Buyer history", "Competition", "Purchase orders", "Estimated renewals"],
    caution: "The \u20AC2m figure is an advertised estimate. Historical award bands do not price this scope."
  },
  {
    id: "529804-2026",
    title: "IT consulting, development and support services",
    buyer: "Office of the Revenue Commissioners",
    sector: "IT Services",
    cpv: "72000000",
    deadline: "11 Sep 2026",
    daysToDeadline: 37,
    estimate: 35e5,
    evidence: 91,
    source: "TED",
    sourceUrl: "https://ted.europa.eu/en/notice/-/detail/529804-2026",
    published: "30 Jul 2026",
    procedure: "Open procedure",
    signals: {
      buyerAwards: 193,
      sectorAwards: 15,
      knownSuppliers: 9,
      competitionLots: 30,
      singleBidPct: 3.3,
      expiring: 0,
      paymentLines: 4557,
      paymentMeaning: "Disclosed payments"
    },
    coverage: ["Buyer history", "Competition", "Payments", "Company matching"],
    caution: "Payment disclosures run to 2025. No related national expiry appears in the current 18-month window."
  },
  {
    id: "521603-2026",
    title: "Energy and building performance consultancy",
    buyer: "Health Service Executive (HSE)",
    sector: "Architecture & Engineering",
    cpv: "71314300",
    deadline: "21 Aug 2026",
    daysToDeadline: 16,
    estimate: 15e6,
    evidence: 88,
    source: "TED",
    sourceUrl: "https://ted.europa.eu/en/notice/-/detail/521603-2026",
    published: "24 Jul 2026",
    procedure: "Open procedure",
    signals: {
      buyerAwards: 2763,
      sectorAwards: 87,
      knownSuppliers: 52,
      competitionLots: 327,
      singleBidPct: 12.5,
      expiring: 8,
      paymentLines: 76187,
      paymentMeaning: "Disclosed payments"
    },
    coverage: ["Buyer history", "Competition", "Payments", "Estimated renewals"],
    caution: "The estimate may cover multiple years or lots. Treat it as planned value, not awarded or paid value."
  },
  {
    id: "523161-2026",
    title: "General construction works",
    buyer: "Dublin City Council",
    sector: "Construction",
    cpv: "45000000",
    deadline: "27 Aug 2026",
    daysToDeadline: 22,
    estimate: 478e4,
    evidence: 87,
    source: "TED",
    sourceUrl: "https://ted.europa.eu/en/notice/-/detail/523161-2026",
    published: "27 Jul 2026",
    procedure: "Open procedure",
    signals: {
      buyerAwards: 1873,
      sectorAwards: 164,
      knownSuppliers: 93,
      competitionLots: 230,
      singleBidPct: 33.9,
      expiring: 11,
      paymentLines: 39226,
      paymentMeaning: "Committed purchase orders"
    },
    coverage: ["Buyer history", "Competition", "Purchase orders", "Estimated renewals"],
    caution: "Construction comparables vary materially by project scope and site conditions."
  },
  {
    id: "528297-2026",
    title: "Architectural services for office accommodation",
    buyer: "Health Information and Quality Authority",
    sector: "Architecture & Engineering",
    cpv: "71221000",
    deadline: "28 Aug 2026",
    daysToDeadline: 23,
    estimate: 8e5,
    evidence: 78,
    source: "TED",
    sourceUrl: "https://ted.europa.eu/en/notice/-/detail/528297-2026",
    published: "29 Jul 2026",
    procedure: "Open procedure",
    signals: {
      buyerAwards: 74,
      sectorAwards: 9,
      knownSuppliers: 7,
      competitionLots: 12,
      singleBidPct: null,
      expiring: 1,
      paymentLines: 0,
      paymentMeaning: "No matched disclosure"
    },
    coverage: ["Buyer history", "Award values", "Source notice"],
    caution: "The competition sample is too small for a buyer-level rate. The app should not manufacture one."
  },
  {
    id: "531413-2026",
    title: "Enterprise software platform",
    buyer: "South East Technological University (SETU)",
    sector: "Software",
    cpv: "48000000",
    deadline: "28 Aug 2026",
    daysToDeadline: 23,
    estimate: 3e5,
    evidence: 82,
    source: "TED",
    sourceUrl: "https://ted.europa.eu/en/notice/-/detail/531413-2026",
    published: "29 Jul 2026",
    procedure: "Open procedure",
    signals: {
      buyerAwards: 188,
      sectorAwards: 12,
      knownSuppliers: 10,
      competitionLots: 42,
      singleBidPct: 14.3,
      expiring: 3,
      paymentLines: 0,
      paymentMeaning: "No matched disclosure"
    },
    coverage: ["Buyer history", "Competition", "Estimated renewals"],
    caution: "Supplier names require review where institutions use trading names or group entities."
  },
  {
    id: "527525-2026",
    title: "Financial and management consultancy services",
    buyer: "Longford County Council",
    sector: "Business & Consulting",
    cpv: "79412000",
    deadline: "2 Oct 2026",
    daysToDeadline: 58,
    estimate: 294e4,
    evidence: 74,
    source: "TED",
    sourceUrl: "https://ted.europa.eu/en/notice/-/detail/527525-2026",
    published: "28 Jul 2026",
    procedure: "Open procedure",
    signals: {
      buyerAwards: 221,
      sectorAwards: 18,
      knownSuppliers: 13,
      competitionLots: 19,
      singleBidPct: null,
      expiring: 2,
      paymentLines: 3982,
      paymentMeaning: "Committed purchase orders"
    },
    coverage: ["Buyer history", "Purchase orders", "Source notice"],
    caution: "Only 23% of classified consulting awards carry a sum-safe contract value."
  },
  {
    id: "529524-2026",
    title: "Laboratory equipment maintenance services",
    buyer: "State Laboratory",
    sector: "Repair & Maintenance",
    cpv: "50000000",
    deadline: "31 Aug 2026",
    daysToDeadline: 26,
    estimate: 4e6,
    evidence: 70,
    source: "TED",
    sourceUrl: "https://ted.europa.eu/en/notice/-/detail/529524-2026",
    published: "28 Jul 2026",
    procedure: "Open procedure",
    signals: {
      buyerAwards: 61,
      sectorAwards: 5,
      knownSuppliers: 4,
      competitionLots: 8,
      singleBidPct: null,
      expiring: 1,
      paymentLines: 0,
      paymentMeaning: "No matched disclosure"
    },
    coverage: ["Buyer history", "Source notice"],
    caution: "Thin buyer and competition samples. A brief should lead with the gap, not a derived conclusion."
  }
];

// worker/index.js
var JSON_HEADERS = { "content-type": "application/json; charset=utf-8" };
function normaliseSubscription(input = {}) {
  const email = String(input.email || "").trim().toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw new Error("Enter a valid delivery email.");
  const name = String(input.name || "Opportunity watch").trim().slice(0, 100) || "Opportunity watch";
  const sectors = cleanStringList(input.sectors, 8, 80);
  const buyers = cleanStringList(input.buyers, 8, 100);
  const cadence = input.cadence === "weekly" ? "weekly" : "weekday";
  const minValue = clampInteger(input.minValue, 0, 2e9, 0);
  const deadlineDays = clampInteger(input.deadlineDays, 1, 365, 60);
  const minEvidence = clampInteger(input.minEvidence, 0, 100, 70);
  return {
    name,
    email,
    sectors,
    buyers,
    cadence,
    minValue,
    deadlineDays,
    minEvidence,
    includeExpiries: input.includeExpiries !== false
  };
}
__name(normaliseSubscription, "normaliseSubscription");
function cleanStringList(value, maxItems, maxLength) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((item) => String(item).trim().slice(0, maxLength)).filter(Boolean))].slice(0, maxItems);
}
__name(cleanStringList, "cleanStringList");
function clampInteger(value, min, max, fallback) {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.min(max, Math.max(min, Math.round(number)));
}
__name(clampInteger, "clampInteger");
function matchOpportunity(item, subscription) {
  const sectors = subscription.sectors || [];
  const buyers = subscription.buyers || [];
  const sectorMatch = !sectors.length || sectors.includes(item.sector);
  const buyerMatch = !buyers.length || buyers.some((buyer) => String(item.buyer || "").toLowerCase().includes(buyer.toLowerCase()));
  const estimate = Number(item.estimate || item.estimated_value_eur || 0);
  const deadline = Number(item.daysToDeadline ?? item.days_to_deadline ?? 9999);
  const evidence = Number(item.evidence ?? item.evidence_coverage ?? 0);
  return sectorMatch && buyerMatch && estimate >= subscription.minValue && deadline <= subscription.deadlineDays && evidence >= subscription.minEvidence;
}
__name(matchOpportunity, "matchOpportunity");
function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}
__name(json, "json");
function html(body, status = 200) {
  return new Response(body, { status, headers: { "content-type": "text/html; charset=utf-8" } });
}
__name(html, "html");
function escapeHtml(value) {
  return String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");
}
__name(escapeHtml, "escapeHtml");
function formatCurrency(value) {
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return "Estimate not stated";
  return new Intl.NumberFormat("en-IE", { style: "currency", currency: "EUR", maximumFractionDigits: 0 }).format(number);
}
__name(formatCurrency, "formatCurrency");
function appUrl(env, request) {
  const configured = String(env.APP_URL || "").trim();
  if (configured && !configured.includes("example.com")) return configured.replace(/\/$/, "");
  return request ? new URL(request.url).origin : configured.replace(/\/$/, "");
}
__name(appUrl, "appUrl");
async function loadOpportunities(env) {
  if (!env.PROCUREMENT_FEED_URL) return opportunities;
  const headers = { accept: "application/json" };
  if (env.PROCUREMENT_FEED_TOKEN) headers.authorization = `Bearer ${env.PROCUREMENT_FEED_TOKEN}`;
  const response = await fetch(env.PROCUREMENT_FEED_URL, { headers });
  if (!response.ok) throw new Error(`Procurement feed returned ${response.status}`);
  const payload = await response.json();
  const rows = Array.isArray(payload) ? payload : payload.result || payload.items || payload.data || [];
  return rows.map((item) => ({
    id: item.id || item.publication_number || item.resource_id,
    title: item.title || item.contract_name || "Public procurement opportunity",
    buyer: item.buyer || item.buyer_name || "Buyer not stated",
    sector: item.sector || item.cpv_division || "Other",
    deadline: item.deadline || item.submission_deadline,
    daysToDeadline: item.daysToDeadline ?? item.days_to_deadline ?? daysUntil(item.submission_deadline),
    estimate: item.estimate ?? item.estimated_value_eur ?? 0,
    evidence: item.evidence ?? item.evidence_coverage ?? 70,
    source: item.source || "Feed",
    sourceUrl: item.sourceUrl || item.notice_url || item.detail_url || "",
    caution: item.caution || "Review the source notice before relying on this summary."
  }));
}
__name(loadOpportunities, "loadOpportunities");
function daysUntil(value) {
  const deadline = new Date(value);
  if (Number.isNaN(deadline.getTime())) return 9999;
  return Math.ceil((deadline.getTime() - Date.now()) / 864e5);
}
__name(daysUntil, "daysUntil");
async function sendEmail(env, { to, subject, htmlBody, idempotencyKey }) {
  if (!env.RESEND_API_KEY) return { sent: false, reason: "RESEND_API_KEY is not configured" };
  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      authorization: `Bearer ${env.RESEND_API_KEY}`,
      "content-type": "application/json",
      "idempotency-key": idempotencyKey
    },
    body: JSON.stringify({ from: env.EMAIL_FROM, to: [to], subject, html: htmlBody })
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.message || `Email provider returned ${response.status}`);
  return { sent: true, id: payload.id };
}
__name(sendEmail, "sendEmail");
function emailShell(content, footer) {
  return `<!doctype html><html><body style="margin:0;background:#f5f2eb;color:#292622;font-family:Arial,sans-serif"><div style="max-width:640px;margin:0 auto;padding:28px 18px"><div style="font-size:12px;font-weight:800;letter-spacing:.08em;color:#a24731">PUBLICSIGNAL</div><div style="margin-top:14px;border:1px solid #d9d2c7;border-radius:10px;background:#fff;padding:24px">${content}</div><p style="color:#746f68;font-size:11px;line-height:1.5">${footer}</p></div></body></html>`;
}
__name(emailShell, "emailShell");
function confirmationEmail(subscription, confirmUrl) {
  return emailShell(
    `<h1 style="margin:0 0 10px;font-size:22px">Confirm ${escapeHtml(subscription.name)}</h1><p style="font-size:14px;line-height:1.55">PublicSignal will send a ${subscription.cadence === "weekly" ? "weekly" : "weekday"} evidence digest to this address when opportunities match your rules.</p><p style="margin:22px 0"><a href="${escapeHtml(confirmUrl)}" style="display:inline-block;padding:10px 16px;border-radius:6px;background:#a24731;color:#fff;text-decoration:none;font-weight:700">Confirm this watch</a></p><p style="color:#746f68;font-size:12px">If you did not request this watch, ignore this email. It will remain inactive.</p>`,
    "PublicSignal separates advertised estimates, awarded values and disclosed payments. Every digest links back to source evidence."
  );
}
__name(confirmationEmail, "confirmationEmail");
function renderDigest(subscription, matches, unsubscribeUrl) {
  const rows = matches.slice(0, 12).map((item) => `
    <div style="padding:16px 0;border-top:1px solid #e3ddd3">
      <div style="font-size:11px;color:#746f68">${escapeHtml(item.sector)} \xB7 ${escapeHtml(item.source || "Source")}</div>
      <h2 style="margin:5px 0 4px;font-size:16px">${escapeHtml(item.title)}</h2>
      <p style="margin:0 0 8px;color:#4f4a45;font-size:13px">${escapeHtml(item.buyer)}</p>
      <table role="presentation" style="width:100%;font-size:12px"><tr><td>${formatCurrency(item.estimate)}</td><td style="text-align:center">${escapeHtml(item.deadline || `${item.daysToDeadline} days`)}</td><td style="text-align:right">${Number(item.evidence || 0)}% evidence</td></tr></table>
      ${item.sourceUrl ? `<p style="margin:10px 0 0"><a href="${escapeHtml(item.sourceUrl)}" style="color:#a24731">Open source notice</a></p>` : ""}
    </div>`).join("");
  return emailShell(
    `<h1 style="margin:0;font-size:22px">${escapeHtml(subscription.name)}</h1><p style="margin:7px 0 20px;color:#746f68;font-size:13px">${matches.length} current opportunities clear your filters.</p>${rows || '<p style="font-size:13px">No current notices clear this rule.</p>'}`,
    `Advertised estimates are planning indicators, not awarded or paid values. <a href="${escapeHtml(unsubscribeUrl)}" style="color:#746f68">Unsubscribe this watch</a>.`
  );
}
__name(renderDigest, "renderDigest");
async function createSubscription(request, env) {
  if (!env.DB) return json({ error: "D1 binding DB is not configured." }, 503);
  let subscription;
  try {
    subscription = normaliseSubscription(await request.json());
  } catch (error) {
    return json({ error: error.message }, 400);
  }
  const recent = await env.DB.prepare("SELECT COUNT(*) AS n FROM subscriptions WHERE email=? AND created_at >= datetime('now', '-15 minutes')").bind(subscription.email).first();
  if (Number(recent?.n || 0) >= 3) return json({ error: "Too many recent watch requests for this email. Try again later." }, 429);
  const confirmToken = crypto.randomUUID();
  const unsubscribeToken = crypto.randomUUID();
  const result = await env.DB.prepare(`
    INSERT INTO subscriptions
      (name, email, confirm_token, unsubscribe_token, status, cadence, sectors_json, buyers_json, min_value_eur, deadline_days, min_evidence, include_expiries)
    VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
  `).bind(
    subscription.name,
    subscription.email,
    confirmToken,
    unsubscribeToken,
    subscription.cadence,
    JSON.stringify(subscription.sectors),
    JSON.stringify(subscription.buyers),
    subscription.minValue,
    subscription.deadlineDays,
    subscription.minEvidence,
    subscription.includeExpiries ? 1 : 0
  ).run();
  const base = appUrl(env, request);
  const confirmUrl = `${base}/api/subscriptions/confirm?token=${encodeURIComponent(confirmToken)}`;
  const delivery = await sendEmail(env, {
    to: subscription.email,
    subject: `Confirm your PublicSignal watch: ${subscription.name}`,
    htmlBody: confirmationEmail(subscription, confirmUrl),
    idempotencyKey: `confirm-${result.meta.last_row_id}`
  });
  return json({ id: result.meta.last_row_id, status: "pending", emailSent: delivery.sent }, 201);
}
__name(createSubscription, "createSubscription");
async function confirmSubscription(request, env) {
  const token = new URL(request.url).searchParams.get("token");
  if (!token || !env.DB) return html(statusPage("Confirmation link is invalid", "This watch could not be confirmed."), 400);
  const result = await env.DB.prepare("UPDATE subscriptions SET status='active', confirmed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE confirm_token=? AND status='pending'").bind(token).run();
  if (!result.meta.changes) return html(statusPage("Watch already handled", "This confirmation link is no longer active."), 404);
  return html(statusPage("Watch confirmed", "Your evidence digest is active. You can close this page."));
}
__name(confirmSubscription, "confirmSubscription");
async function unsubscribe(request, env) {
  const token = new URL(request.url).searchParams.get("token");
  if (!token || !env.DB) return html(statusPage("Unsubscribe link is invalid", "No watch was changed."), 400);
  const result = await env.DB.prepare("UPDATE subscriptions SET status='unsubscribed', updated_at=CURRENT_TIMESTAMP WHERE unsubscribe_token=? AND status!='unsubscribed'").bind(token).run();
  return html(statusPage(result.meta.changes ? "Watch unsubscribed" : "Watch already inactive", result.meta.changes ? "No further digests will be sent for this watch." : "No active watch matched this link."));
}
__name(unsubscribe, "unsubscribe");
function statusPage(title, message) {
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>${escapeHtml(title)}</title></head><body style="margin:0;background:#f5f2eb;color:#292622;font-family:Arial,sans-serif"><main style="max-width:560px;margin:12vh auto;padding:28px"><p style="color:#a24731;font-weight:800;letter-spacing:.08em">PUBLICSIGNAL</p><h1>${escapeHtml(title)}</h1><p>${escapeHtml(message)}</p></main></body></html>`;
}
__name(statusPage, "statusPage");
function rowToSubscription(row) {
  return {
    id: row.id,
    name: row.name,
    email: row.email,
    cadence: row.cadence,
    sectors: JSON.parse(row.sectors_json || "[]"),
    buyers: JSON.parse(row.buyers_json || "[]"),
    minValue: row.min_value_eur,
    deadlineDays: row.deadline_days,
    minEvidence: row.min_evidence,
    includeExpiries: Boolean(row.include_expiries),
    unsubscribeToken: row.unsubscribe_token
  };
}
__name(rowToSubscription, "rowToSubscription");
async function sendDueDigests(env, scheduledTime = Date.now()) {
  if (!env.DB) throw new Error("D1 binding DB is not configured");
  const date = new Date(scheduledTime);
  const isMonday = date.getUTCDay() === 1;
  const digestDate = date.toISOString().slice(0, 10);
  const { results } = await env.DB.prepare("SELECT * FROM subscriptions WHERE status='active' AND (cadence='weekday' OR (cadence='weekly' AND ?=1))").bind(isMonday ? 1 : 0).all();
  const opportunities2 = await loadOpportunities(env);
  const base = appUrl(env);
  for (const row of results || []) {
    const subscription = rowToSubscription(row);
    const digestKey = `${digestDate}-${subscription.cadence}`;
    const existing = await env.DB.prepare("SELECT id FROM delivery_log WHERE subscription_id=? AND digest_key=?").bind(subscription.id, digestKey).first();
    if (existing) continue;
    const matches = opportunities2.filter((item) => matchOpportunity(item, subscription));
    if (!matches.length) continue;
    let status = "sent";
    let providerId = null;
    let errorMessage = null;
    try {
      const unsubscribeUrl = `${base}/api/subscriptions/unsubscribe?token=${encodeURIComponent(subscription.unsubscribeToken)}`;
      const delivery = await sendEmail(env, {
        to: subscription.email,
        subject: `${matches.length} PublicSignal matches for ${subscription.name}`,
        htmlBody: renderDigest(subscription, matches, unsubscribeUrl),
        idempotencyKey: `digest-${subscription.id}-${digestKey}`
      });
      status = delivery.sent ? "sent" : "preview";
      providerId = delivery.id || null;
      errorMessage = delivery.reason || null;
    } catch (error) {
      status = "failed";
      errorMessage = String(error.message || error).slice(0, 500);
    }
    await env.DB.prepare("INSERT INTO delivery_log (subscription_id, digest_key, provider_message_id, opportunity_count, status, error_message) VALUES (?, ?, ?, ?, ?, ?)").bind(subscription.id, digestKey, providerId, matches.length, status, errorMessage).run();
  }
}
__name(sendDueDigests, "sendDueDigests");
async function previewDigest(request, env) {
  let subscription;
  try {
    subscription = normaliseSubscription(await request.json());
  } catch (error) {
    return json({ error: error.message }, 400);
  }
  const matches = (await loadOpportunities(env)).filter((item) => matchOpportunity(item, subscription));
  return json({ count: matches.length, matches: matches.slice(0, 12).map(({ id, title, buyer, sector, deadline, daysToDeadline, estimate, evidence, sourceUrl }) => ({ id, title, buyer, sector, deadline, daysToDeadline, estimate, evidence, sourceUrl })) });
}
__name(previewDigest, "previewDigest");
async function handleApi(request, env) {
  const url = new URL(request.url);
  if (url.pathname === "/api/health" && request.method === "GET") return json({ ok: true, app: "public-signal", emailConfigured: Boolean(env.RESEND_API_KEY), feed: env.PROCUREMENT_FEED_URL ? "remote" : "sample" });
  if (url.pathname === "/api/subscriptions" && request.method === "POST") return createSubscription(request, env);
  if (url.pathname === "/api/subscriptions/confirm" && request.method === "GET") return confirmSubscription(request, env);
  if (url.pathname === "/api/subscriptions/unsubscribe" && request.method === "GET") return unsubscribe(request, env);
  if (url.pathname === "/api/digests/preview" && request.method === "POST") return previewDigest(request, env);
  return json({ error: "API route not found" }, 404);
}
__name(handleApi, "handleApi");
var index_default = {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) return handleApi(request, env);
    const response = await env.ASSETS.fetch(request);
    const secured = new Response(response.body, response);
    secured.headers.set("content-security-policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'");
    secured.headers.set("referrer-policy", "strict-origin-when-cross-origin");
    secured.headers.set("x-content-type-options", "nosniff");
    secured.headers.set("permissions-policy", "camera=(), microphone=(), geolocation=()");
    if (url.protocol === "https:") secured.headers.set("strict-transport-security", "max-age=31536000; includeSubDomains");
    return secured;
  },
  async scheduled(controller, env, ctx) {
    ctx.waitUntil(sendDueDigests(env, controller.scheduledTime));
  }
};
export {
  index_default as default,
  matchOpportunity,
  normaliseSubscription,
  renderDigest
};
//# sourceMappingURL=index.js.map
