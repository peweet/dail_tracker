import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { analyticsLimiterKey, consumeAnalyticsLimit, consumeSubscriptionLimit, hashAnalyticsSession, ingestAnalytics, matchOpportunity, normaliseAnalyticsPayload, normaliseSubscription, purgeAnalytics, renderDigest, validateAnalyticsEvent, validateTurnstile } from "./index.js";

test("normaliseSubscription bounds user-controlled filters", () => {
  const result = normaliseSubscription({
    name: "  Civil engineering  ",
    email: " BIDTEAM@Example.ie ",
    sectors: ["Architecture & Engineering", "Architecture & Engineering"],
    buyers: ["Council"],
    cadence: "weekly",
    minValue: -4,
    deadlineDays: 900,
    minEvidence: 101,
  });
  assert.equal(result.name, "Civil engineering");
  assert.equal(result.email, "bidteam@example.ie");
  assert.deepEqual(result.sectors, ["Architecture & Engineering"]);
  assert.equal(result.minValue, 0);
  assert.equal(result.deadlineDays, 365);
  assert.equal(result.minEvidence, 100);
});

test("normaliseSubscription rejects malformed email", () => {
  assert.throws(() => normaliseSubscription({ email: "not-an-email" }), /valid delivery email/);
});

test("matchOpportunity keeps money, deadline and evidence filters independent", () => {
  const subscription = normaliseSubscription({
    email: "bid@example.ie",
    sectors: ["IT Services"],
    buyers: ["Revenue"],
    minValue: 1_000_000,
    deadlineDays: 60,
    minEvidence: 80,
  });
  const opportunity = { sector: "IT Services", buyer: "Office of the Revenue Commissioners", estimate: 3_500_000, daysToDeadline: 37, evidence: 91 };
  assert.equal(matchOpportunity(opportunity, subscription), true);
  assert.equal(matchOpportunity({ ...opportunity, estimate: 500_000 }, subscription), false);
  assert.equal(matchOpportunity({ ...opportunity, evidence: 70 }, subscription), false);
});

test("renderDigest escapes supplier-facing content and labels estimates", () => {
  const result = renderDigest(
    { name: "<Market>", cadence: "weekday" },
    [{ title: "A & B", buyer: "Buyer <script>", sector: "IT", source: "TED", estimate: 500000, deadline: "20 Aug", evidence: 80, sourceUrl: "https://example.ie" }],
    "https://example.ie/unsubscribe",
  );
  assert.match(result, /&lt;Market&gt;/);
  assert.match(result, /Buyer &lt;script&gt;/);
  assert.doesNotMatch(result, /Buyer <script>/);
  assert.match(result, /Advertised estimates are planning indicators/);
});

test("validateTurnstile is optional only when no secret is configured", async () => {
  const request = new Request("https://publicsignal.ie/api/subscriptions");
  assert.deepEqual(await validateTurnstile("", {}, request), { valid: true, configured: false });
  const missing = await validateTurnstile("", { TURNSTILE_SECRET_KEY: "secret" }, request);
  assert.equal(missing.valid, false);
  assert.equal(missing.status, 403);
});

test("validateTurnstile submits the visitor token to Siteverify", async () => {
  const originalFetch = globalThis.fetch;
  let call;
  globalThis.fetch = async (url, options) => {
    call = { url, options };
    return new Response(JSON.stringify({ success: true }), { status: 200 });
  };
  try {
    const request = new Request("https://publicsignal.ie/api/subscriptions", { headers: { "cf-connecting-ip": "203.0.113.7" } });
    const result = await validateTurnstile("visitor-token", { TURNSTILE_SECRET_KEY: "secret" }, request);
    assert.deepEqual(result, { valid: true, configured: true });
    assert.equal(call.url, "https://challenges.cloudflare.com/turnstile/v0/siteverify");
    assert.match(call.options.body, /visitor-token/);
    assert.match(call.options.body, /203\.0\.113\.7/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("consumeSubscriptionLimit rejects the fourth live request", async () => {
  let calls = 0;
  const limiter = { limit: async () => ({ success: ++calls < 4 }) };
  assert.equal((await consumeSubscriptionLimit({ SUBSCRIPTION_LIMITER: limiter }, "bid@example.ie")).allowed, true);
  assert.equal((await consumeSubscriptionLimit({ SUBSCRIPTION_LIMITER: limiter }, "bid@example.ie")).allowed, true);
  assert.equal((await consumeSubscriptionLimit({ SUBSCRIPTION_LIMITER: limiter }, "bid@example.ie")).allowed, true);
  assert.equal((await consumeSubscriptionLimit({ SUBSCRIPTION_LIMITER: limiter }, "bid@example.ie")).allowed, false);
});

test("consumeAnalyticsLimit bounds a hashed session independently", async () => {
  const keys = [];
  const limiter = { limit: async ({ key }) => { keys.push(key); return { success: keys.length < 2 }; } };
  assert.equal((await consumeAnalyticsLimit({ ANALYTICS_LIMITER: limiter }, "deadbeef")).allowed, true);
  assert.equal((await consumeAnalyticsLimit({ ANALYTICS_LIMITER: limiter }, "deadbeef")).allowed, false);
  assert.deepEqual(keys, ["analytics:deadbeef", "analytics:deadbeef"]);
});

test("analytics payload only accepts known semantic events and UUID sessions", () => {
  const sessionId = "123e4567-e89b-42d3-a456-426614174000";
  assert.deepEqual(validateAnalyticsEvent({ eventType: "page_open", targetSlug: "page-pipeline", query: "ignored" }), { eventType: "page_open", targetSlug: "page-pipeline" });
  assert.equal(validateAnalyticsEvent({ eventType: "page_open", targetSlug: "search-term" }), null);
  assert.deepEqual(normaliseAnalyticsPayload({ sessionId, events: [{ eventType: "app_open", targetSlug: "app", path: "/?email=secret" }] }), { sessionId, events: [{ eventType: "app_open", targetSlug: "app" }] });
  assert.throws(() => normaliseAnalyticsPayload({ sessionId: "bid@example.ie", events: [{ eventType: "app_open", targetSlug: "app" }] }), /session is invalid/);
  assert.throws(() => normaliseAnalyticsPayload({ sessionId, events: [{ eventType: "app_open", targetSlug: "app" }, { eventType: "app_open", targetSlug: "app" }] }), /out of bounds/);
});

test("analytics ingestion stores only hashed sessions and bounded fields", async () => {
  const calls = [];
  const db = {
    prepare(sql) {
      return {
        bind(...args) {
          calls.push({ sql, args });
          return { sql, args };
        },
      };
    },
    async batch(statements) { calls.push({ batch: statements }); },
  };
  const response = await ingestAnalytics(new Request("https://publicsignal.ie/api/events", {
    method: "POST",
    body: JSON.stringify({ sessionId: "123e4567-e89b-42d3-a456-426614174000", events: [{ eventType: "watch_start", targetSlug: "watch-start", email: "private@example.ie" }] }),
  }), { DB: db, ANALYTICS_HASH_SALT: "test-salt", ANALYTICS_LIMITER: { limit: async () => ({ success: true }) } });
  assert.equal(response.status, 202);
  assert.deepEqual(await response.json(), { accepted: 1 });
  const insert = calls.find((call) => call.sql?.startsWith("INSERT INTO analytics_events"));
  assert.equal(insert.args[1], "watch_start");
  assert.equal(insert.args[2], "watch-start");
  assert.match(insert.args[0], /^[0-9a-f]{32}$/);
  assert.notEqual(insert.args[0], "123e4567-e89b-42d3-a456-426614174000");
});

test("analytics ingestion rejects oversized requests", async () => {
  const response = await ingestAnalytics(new Request("https://publicsignal.ie/api/events", {
    method: "POST",
    headers: { "content-type": "application/json", "content-length": "9000" },
    body: "{}",
  }), { DB: {}, ANALYTICS_HASH_SALT: "test-salt", ANALYTICS_LIMITER: { limit: async () => ({ success: true }) } });
  assert.equal(response.status, 413);
});

test("analytics ingestion fails closed when the limiter binding is absent", async () => {
  const response = await ingestAnalytics(new Request("https://publicsignal.ie/api/events", {
    method: "POST",
    body: JSON.stringify({ sessionId: "123e4567-e89b-42d3-a456-426614174000", eventType: "app_open", targetSlug: "app" }),
  }), { DB: { prepare() {} }, ANALYTICS_HASH_SALT: "test-salt" });
  assert.equal(response.status, 503);
  assert.match((await response.json()).error, /temporarily unavailable/);
});

test("analytics ingestion hides D1 batch failures", async () => {
  const db = {
    prepare() { return { bind() { return {}; } }; },
    async batch() { throw new Error("private database details"); },
  };
  const response = await ingestAnalytics(new Request("https://publicsignal.ie/api/events", {
    method: "POST",
    body: JSON.stringify({ sessionId: "123e4567-e89b-42d3-a456-426614174000", eventType: "app_open", targetSlug: "app" }),
  }), { DB: db, ANALYTICS_HASH_SALT: "test-salt", ANALYTICS_LIMITER: { limit: async () => ({ success: true }) } });
  assert.equal(response.status, 503);
  const body = await response.json();
  assert.deepEqual(body, { error: "Analytics storage is temporarily unavailable." });
  assert.doesNotMatch(JSON.stringify(body), /private database details/);
});

test("analytics ingestion fails closed without a private hash salt", async () => {
  const response = await ingestAnalytics(new Request("https://publicsignal.ie/api/events", { method: "POST", body: "{}" }), { DB: {} });
  assert.equal(response.status, 503);
  assert.match((await response.json()).error, /not configured/);
  await assert.rejects(() => hashAnalyticsSession("123e4567-e89b-42d3-a456-426614174000", {}), /not configured/);
});

test("analytics limiter key is stable across rotating sessions from one IP", async () => {
  const env = { ANALYTICS_HASH_SALT: "test-salt" };
  const request = (ip) => new Request("https://publicsignal.ie/api/events", { headers: ip ? { "cf-connecting-ip": ip } : {} });
  const firstHash = await hashAnalyticsSession("123e4567-e89b-42d3-a456-426614174000", env);
  const secondHash = await hashAnalyticsSession("123e4567-e89b-42d3-a456-426614174001", env);
  const firstKey = await analyticsLimiterKey(request("203.0.113.7"), env, firstHash);
  const secondKey = await analyticsLimiterKey(request("203.0.113.7"), env, secondHash);
  assert.equal(firstKey, secondKey);
  assert.notEqual(firstKey, await analyticsLimiterKey(request("203.0.113.8"), env, firstHash));
  assert.notEqual(firstKey, firstHash);
});

test("analytics retention uses the configured bounded window", async () => {
  let query;
  const db = { prepare(sql) { query = sql; return { bind(value) { return { async run() { return { meta: { changes: 4 }, value }; } }; } }; } };
  const result = await purgeAnalytics({ DB: db, ANALYTICS_RETENTION_DAYS: "45" }, Date.parse("2026-08-05T00:00:00Z"));
  assert.equal(result.deleted, 4);
  assert.equal(result.retentionDays, 45);
  assert.equal(query, "DELETE FROM analytics_events WHERE occurred_at < ?");
});

test("watch Turnstile lifecycle invalidates stale async renders", async () => {
  const source = await readFile(new URL("../public/app.js", import.meta.url), "utf8");
  assert.match(source, /function resetWatchTurnstile\(\)/);
  assert.match(source, /generation !== watchTurnstile\.generation/);
  assert.match(source, /!form\.isConnected/);
  assert.match(source, /turnstile\.reset/);
});
