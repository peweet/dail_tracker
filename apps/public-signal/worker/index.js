import { opportunities as sampleOpportunities } from "../public/sample-data.js";

const JSON_HEADERS = { "content-type": "application/json; charset=utf-8" };

export const ANALYTICS_EVENT_TARGETS = Object.freeze({
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

const ANALYTICS_MAX_BODY_BYTES = 8192;
const ANALYTICS_MAX_EVENTS = 1;
const ANALYTICS_SESSION_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function validateAnalyticsEvent(input) {
  if (!input || typeof input !== "object") return null;
  const eventType = String(input.eventType || "");
  const targetSlug = String(input.targetSlug || "");
  if (!Object.hasOwn(ANALYTICS_EVENT_TARGETS, eventType)) return null;
  if (!ANALYTICS_EVENT_TARGETS[eventType].includes(targetSlug)) return null;
  return { eventType, targetSlug };
}

export function normaliseAnalyticsPayload(input) {
  if (!input || typeof input !== "object") throw new Error("Analytics payload must be an object.");
  const sessionId = String(input.sessionId || "");
  if (!ANALYTICS_SESSION_ID.test(sessionId)) throw new Error("Analytics session is invalid.");
  const rawEvents = Array.isArray(input.events) ? input.events : [input];
  if (!rawEvents.length || rawEvents.length > ANALYTICS_MAX_EVENTS) throw new Error("Analytics event batch is out of bounds.");
  const events = rawEvents.map(validateAnalyticsEvent).filter(Boolean);
  if (events.length !== rawEvents.length) throw new Error("Analytics event is not allowed.");
  return { sessionId, events };
}

export async function hashAnalyticsSession(sessionId, env = {}) {
  const salt = String(env.ANALYTICS_HASH_SALT || "").trim();
  if (!salt) throw new Error("Analytics hash salt is not configured.");
  const bytes = new TextEncoder().encode(`${salt}:session:${sessionId}`);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("").slice(0, 32);
}

export async function analyticsLimiterKey(request, env, sessionHash) {
  const salt = String(env.ANALYTICS_HASH_SALT || "").trim();
  if (!salt) throw new Error("Analytics hash salt is not configured.");
  const ip = String(request.headers.get("cf-connecting-ip") || "").trim();
  const material = ip ? `ip:${ip}` : `session:${sessionHash}`;
  const bytes = new TextEncoder().encode(`${salt}:limiter:${material}`);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("").slice(0, 32);
}

export async function consumeAnalyticsLimit(env, sessionHash) {
  if (typeof env.ANALYTICS_LIMITER?.limit !== "function") return { allowed: false, configured: false, unavailable: true };
  try {
    const { success } = await env.ANALYTICS_LIMITER.limit({ key: `analytics:${sessionHash}` });
    return { allowed: success, configured: true };
  } catch {
    return { allowed: false, configured: true, unavailable: true };
  }
}

export async function ingestAnalytics(request, env) {
  if (!env.DB) return json({ error: "D1 binding DB is not configured." }, 503);
  if (!String(env.ANALYTICS_HASH_SALT || "").trim()) return json({ error: "Analytics is not configured." }, 503);
  const contentLength = Number(request.headers.get("content-length") || 0);
  if (contentLength > ANALYTICS_MAX_BODY_BYTES) return json({ error: "Analytics payload is too large." }, 413);
  let payload;
  try {
    const raw = await request.text();
    if (new TextEncoder().encode(raw).length > ANALYTICS_MAX_BODY_BYTES) return json({ error: "Analytics payload is too large." }, 413);
    payload = normaliseAnalyticsPayload(JSON.parse(raw));
  } catch (error) {
    return json({ error: error.message }, 400);
  }
  const sessionHash = await hashAnalyticsSession(payload.sessionId, env);
  const rate = await consumeAnalyticsLimit(env, await analyticsLimiterKey(request, env, sessionHash));
  if (!rate.allowed) return json({ error: rate.unavailable ? "Analytics protection is temporarily unavailable." : "Too many analytics events." }, rate.unavailable ? 503 : 429);
  const statements = payload.events.map(({ eventType, targetSlug }) => env.DB.prepare(
    "INSERT INTO analytics_events (session_hash, event_type, target_slug) VALUES (?, ?, ?)",
  ).bind(sessionHash, eventType, targetSlug));
  try {
    await env.DB.batch(statements);
  } catch {
    return json({ error: "Analytics storage is temporarily unavailable." }, 503);
  }
  return json({ accepted: statements.length }, 202);
}

export async function purgeAnalytics(env, now = Date.now()) {
  if (!env.DB) return { deleted: 0, retentionDays: 90 };
  const configured = Number(env.ANALYTICS_RETENTION_DAYS || 90);
  const retentionDays = Number.isFinite(configured) ? Math.min(365, Math.max(30, Math.round(configured))) : 90;
  const cutoff = new Date(now - retentionDays * 86_400_000).toISOString().slice(0, 19).replace("T", " ");
  const result = await env.DB.prepare("DELETE FROM analytics_events WHERE occurred_at < ?").bind(cutoff).run();
  return { deleted: Number(result.meta?.changes || 0), retentionDays };
}

export function normaliseSubscription(input = {}) {
  const email = String(input.email || "").trim().toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw new Error("Enter a valid delivery email.");

  const name = String(input.name || "Opportunity watch").trim().slice(0, 100) || "Opportunity watch";
  const sectors = cleanStringList(input.sectors, 8, 80);
  const buyers = cleanStringList(input.buyers, 8, 100);
  const cadence = input.cadence === "weekly" ? "weekly" : "weekday";
  const minValue = clampInteger(input.minValue, 0, 2_000_000_000, 0);
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
    includeExpiries: input.includeExpiries !== false,
    turnstileToken: String(input.turnstileToken || "").slice(0, 2048),
  };
}

function cleanStringList(value, maxItems, maxLength) {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((item) => String(item).trim().slice(0, maxLength)).filter(Boolean))].slice(0, maxItems);
}

function clampInteger(value, min, max, fallback) {
  const number = Number(value);
  if (!Number.isFinite(number)) return fallback;
  return Math.min(max, Math.max(min, Math.round(number)));
}

export function matchOpportunity(item, subscription) {
  const sectors = subscription.sectors || [];
  const buyers = subscription.buyers || [];
  const sectorMatch = !sectors.length || sectors.includes(item.sector);
  const buyerMatch = !buyers.length || buyers.some((buyer) => String(item.buyer || "").toLowerCase().includes(buyer.toLowerCase()));
  const estimate = Number(item.estimate || item.estimated_value_eur || 0);
  const deadline = Number(item.daysToDeadline ?? item.days_to_deadline ?? 9999);
  const evidence = Number(item.evidence ?? item.evidence_coverage ?? 0);
  return sectorMatch && buyerMatch && estimate >= subscription.minValue && deadline <= subscription.deadlineDays && evidence >= subscription.minEvidence;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: JSON_HEADERS });
}

function html(body, status = 200) {
  return new Response(body, { status, headers: { "content-type": "text/html; charset=utf-8" } });
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
  const number = Number(value);
  if (!Number.isFinite(number) || number <= 0) return "Estimate not stated";
  return new Intl.NumberFormat("en-IE", { style: "currency", currency: "EUR", maximumFractionDigits: 0 }).format(number);
}

function appUrl(env, request) {
  const configured = String(env.APP_URL || "").trim();
  if (configured && !configured.includes("example.com")) return configured.replace(/\/$/, "");
  return request ? new URL(request.url).origin : configured.replace(/\/$/, "");
}

export async function validateTurnstile(token, env, request) {
  if (!env.TURNSTILE_SECRET_KEY) return { valid: true, configured: false };
  if (!token) return { valid: false, status: 403, error: "Complete the verification before saving this watch." };

  let response;
  try {
    response = await fetch("https://challenges.cloudflare.com/turnstile/v0/siteverify", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        secret: env.TURNSTILE_SECRET_KEY,
        response: token,
        remoteip: request.headers.get("cf-connecting-ip") || undefined,
        idempotency_key: crypto.randomUUID(),
      }),
    });
  } catch {
    return { valid: false, status: 503, error: "Verification is temporarily unavailable. Try again shortly." };
  }
  const result = await response.json().catch(() => ({}));
  if (response.ok && result.success) return { valid: true, configured: true };
  return { valid: false, status: 403, error: "Verification failed or expired. Complete it again and retry." };
}

export async function consumeSubscriptionLimit(env, email) {
  if (typeof env.SUBSCRIPTION_LIMITER?.limit !== "function") return { allowed: true, configured: false };
  try {
    const { success } = await env.SUBSCRIPTION_LIMITER.limit({ key: `subscription:${email}` });
    return { allowed: success, configured: true };
  } catch {
    return { allowed: false, configured: true, unavailable: true };
  }
}

async function loadOpportunities(env) {
  if (!env.PROCUREMENT_FEED_URL) return sampleOpportunities;
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
    caution: item.caution || "Review the source notice before relying on this summary.",
  }));
}

function daysUntil(value) {
  const deadline = new Date(value);
  if (Number.isNaN(deadline.getTime())) return 9999;
  return Math.ceil((deadline.getTime() - Date.now()) / 86_400_000);
}

async function sendEmail(env, { to, subject, htmlBody, idempotencyKey }) {
  if (!env.RESEND_API_KEY) return { sent: false, reason: "RESEND_API_KEY is not configured" };
  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      authorization: `Bearer ${env.RESEND_API_KEY}`,
      "content-type": "application/json",
      "idempotency-key": idempotencyKey,
    },
    body: JSON.stringify({ from: env.EMAIL_FROM, to: [to], subject, html: htmlBody }),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.message || `Email provider returned ${response.status}`);
  return { sent: true, id: payload.id };
}

function emailShell(content, footer) {
  return `<!doctype html><html><body style="margin:0;background:#f5f2eb;color:#292622;font-family:Arial,sans-serif"><div style="max-width:640px;margin:0 auto;padding:28px 18px"><div style="font-size:12px;font-weight:800;letter-spacing:.08em;color:#a24731">PUBLICSIGNAL</div><div style="margin-top:14px;border:1px solid #d9d2c7;border-radius:10px;background:#fff;padding:24px">${content}</div><p style="color:#746f68;font-size:11px;line-height:1.5">${footer}</p></div></body></html>`;
}

function confirmationEmail(subscription, confirmUrl) {
  return emailShell(
    `<h1 style="margin:0 0 10px;font-size:22px">Confirm ${escapeHtml(subscription.name)}</h1><p style="font-size:14px;line-height:1.55">PublicSignal will send a ${subscription.cadence === "weekly" ? "weekly" : "weekday"} evidence digest to this address when opportunities match your rules.</p><p style="margin:22px 0"><a href="${escapeHtml(confirmUrl)}" style="display:inline-block;padding:10px 16px;border-radius:6px;background:#a24731;color:#fff;text-decoration:none;font-weight:700">Confirm this watch</a></p><p style="color:#746f68;font-size:12px">If you did not request this watch, ignore this email. It will remain inactive.</p>`,
    "PublicSignal separates advertised estimates, awarded values and disclosed payments. Every digest links back to source evidence.",
  );
}

export function renderDigest(subscription, matches, unsubscribeUrl) {
  const rows = matches.slice(0, 12).map((item) => `
    <div style="padding:16px 0;border-top:1px solid #e3ddd3">
      <div style="font-size:11px;color:#746f68">${escapeHtml(item.sector)} · ${escapeHtml(item.source || "Source")}</div>
      <h2 style="margin:5px 0 4px;font-size:16px">${escapeHtml(item.title)}</h2>
      <p style="margin:0 0 8px;color:#4f4a45;font-size:13px">${escapeHtml(item.buyer)}</p>
      <table role="presentation" style="width:100%;font-size:12px"><tr><td>${formatCurrency(item.estimate)}</td><td style="text-align:center">${escapeHtml(item.deadline || `${item.daysToDeadline} days`)}</td><td style="text-align:right">${Number(item.evidence || 0)}% evidence</td></tr></table>
      ${item.sourceUrl ? `<p style="margin:10px 0 0"><a href="${escapeHtml(item.sourceUrl)}" style="color:#a24731">Open source notice</a></p>` : ""}
    </div>`).join("");
  return emailShell(
    `<h1 style="margin:0;font-size:22px">${escapeHtml(subscription.name)}</h1><p style="margin:7px 0 20px;color:#746f68;font-size:13px">${matches.length} current opportunities clear your filters.</p>${rows || '<p style="font-size:13px">No current notices clear this rule.</p>'}`,
    `Advertised estimates are planning indicators, not awarded or paid values. <a href="${escapeHtml(unsubscribeUrl)}" style="color:#746f68">Unsubscribe this watch</a>.`,
  );
}

async function createSubscription(request, env) {
  if (!env.DB) return json({ error: "D1 binding DB is not configured." }, 503);
  let subscription;
  try {
    subscription = normaliseSubscription(await request.json());
  } catch (error) {
    return json({ error: error.message }, 400);
  }

  const verification = await validateTurnstile(subscription.turnstileToken, env, request);
  if (!verification.valid) return json({ error: verification.error }, verification.status);
  const rate = await consumeSubscriptionLimit(env, subscription.email);
  if (!rate.allowed) {
    return json(
      { error: rate.unavailable ? "Subscription protection is temporarily unavailable. Try again shortly." : "Too many recent watch requests for this email. Try again shortly." },
      rate.unavailable ? 503 : 429,
    );
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
    subscription.includeExpiries ? 1 : 0,
  ).run();

  const base = appUrl(env, request);
  const confirmUrl = `${base}/api/subscriptions/confirm?token=${encodeURIComponent(confirmToken)}`;
  const delivery = await sendEmail(env, {
    to: subscription.email,
    subject: `Confirm your PublicSignal watch: ${subscription.name}`,
    htmlBody: confirmationEmail(subscription, confirmUrl),
    idempotencyKey: `confirm-${result.meta.last_row_id}`,
  });

  return json({ id: result.meta.last_row_id, status: "pending", emailSent: delivery.sent }, 201);
}

async function confirmSubscription(request, env) {
  const token = new URL(request.url).searchParams.get("token");
  if (!token || !env.DB) return html(statusPage("Confirmation link is invalid", "This watch could not be confirmed."), 400);
  const result = await env.DB.prepare("UPDATE subscriptions SET status='active', confirmed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE confirm_token=? AND status='pending'").bind(token).run();
  if (!result.meta.changes) return html(statusPage("Watch already handled", "This confirmation link is no longer active."), 404);
  return html(statusPage("Watch confirmed", "Your evidence digest is active. You can close this page."));
}

async function unsubscribe(request, env) {
  const token = new URL(request.url).searchParams.get("token");
  if (!token || !env.DB) return html(statusPage("Unsubscribe link is invalid", "No watch was changed."), 400);
  const result = await env.DB.prepare("UPDATE subscriptions SET status='unsubscribed', updated_at=CURRENT_TIMESTAMP WHERE unsubscribe_token=? AND status!='unsubscribed'").bind(token).run();
  return html(statusPage(result.meta.changes ? "Watch unsubscribed" : "Watch already inactive", result.meta.changes ? "No further digests will be sent for this watch." : "No active watch matched this link."));
}

function statusPage(title, message) {
  return `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>${escapeHtml(title)}</title></head><body style="margin:0;background:#f5f2eb;color:#292622;font-family:Arial,sans-serif"><main style="max-width:560px;margin:12vh auto;padding:28px"><p style="color:#a24731;font-weight:800;letter-spacing:.08em">PUBLICSIGNAL</p><h1>${escapeHtml(title)}</h1><p>${escapeHtml(message)}</p></main></body></html>`;
}

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
    unsubscribeToken: row.unsubscribe_token,
  };
}

async function sendDueDigests(env, scheduledTime = Date.now()) {
  if (!env.DB) throw new Error("D1 binding DB is not configured");
  const date = new Date(scheduledTime);
  const isMonday = date.getUTCDay() === 1;
  const digestDate = date.toISOString().slice(0, 10);
  const { results } = await env.DB.prepare("SELECT * FROM subscriptions WHERE status='active' AND (cadence='weekday' OR (cadence='weekly' AND ?=1))").bind(isMonday ? 1 : 0).all();
  const opportunities = await loadOpportunities(env);
  const base = appUrl(env);

  for (const row of results || []) {
    const subscription = rowToSubscription(row);
    const digestKey = `${digestDate}-${subscription.cadence}`;
    const existing = await env.DB.prepare("SELECT id FROM delivery_log WHERE subscription_id=? AND digest_key=?").bind(subscription.id, digestKey).first();
    if (existing) continue;
    const matches = opportunities.filter((item) => matchOpportunity(item, subscription));
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
        idempotencyKey: `digest-${subscription.id}-${digestKey}`,
      });
      status = delivery.sent ? "sent" : "preview";
      providerId = delivery.id || null;
      errorMessage = delivery.reason || null;
    } catch (error) {
      status = "failed";
      errorMessage = String(error.message || error).slice(0, 500);
    }
    await env.DB.prepare("INSERT INTO delivery_log (subscription_id, digest_key, provider_message_id, opportunity_count, status, error_message) VALUES (?, ?, ?, ?, ?, ?)")
      .bind(subscription.id, digestKey, providerId, matches.length, status, errorMessage).run();
  }
}

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

async function handleApi(request, env) {
  const url = new URL(request.url);
  if (url.pathname === "/api/health" && request.method === "GET") return json({ ok: true, app: "public-signal", emailConfigured: Boolean(env.RESEND_API_KEY), analyticsConfigured: Boolean(String(env.ANALYTICS_HASH_SALT || "").trim()) && typeof env.ANALYTICS_LIMITER?.limit === "function", feed: env.PROCUREMENT_FEED_URL ? "remote" : "sample" });
  if (url.pathname === "/api/config" && request.method === "GET") return json({ turnstileSiteKey: env.TURNSTILE_SECRET_KEY ? env.TURNSTILE_SITE_KEY || null : null });
  if (url.pathname === "/api/events" && request.method === "POST") return ingestAnalytics(request, env);
  if (url.pathname === "/api/subscriptions" && request.method === "POST") return createSubscription(request, env);
  if (url.pathname === "/api/subscriptions/confirm" && request.method === "GET") return confirmSubscription(request, env);
  if (url.pathname === "/api/subscriptions/unsubscribe" && request.method === "GET") return unsubscribe(request, env);
  if (url.pathname === "/api/digests/preview" && request.method === "POST") return previewDigest(request, env);
  return json({ error: "API route not found" }, 404);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname.startsWith("/api/")) return handleApi(request, env);
    const response = await env.ASSETS.fetch(request);
    const secured = new Response(response.body, response);
    secured.headers.set("content-security-policy", "default-src 'self'; script-src 'self' https://challenges.cloudflare.com; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self' https://challenges.cloudflare.com; frame-src https://challenges.cloudflare.com; object-src 'none'; base-uri 'self'; frame-ancestors 'none'; form-action 'self'");
    secured.headers.set("referrer-policy", "no-referrer");
    secured.headers.set("x-content-type-options", "nosniff");
    secured.headers.set("permissions-policy", "camera=(), microphone=(), geolocation=()");
    if (url.protocol === "https:") secured.headers.set("strict-transport-security", "max-age=31536000; includeSubDomains");
    return secured;
  },

  async scheduled(controller, env, ctx) {
    ctx.waitUntil(purgeAnalytics(env));
    ctx.waitUntil(sendDueDigests(env, controller.scheduledTime));
  },
};
