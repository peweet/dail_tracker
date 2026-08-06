import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { analyticsLimiterKey, consumeAnalyticsLimit, consumeSubscriptionLimit, createSubscription, emailConfiguration, hashAnalyticsSession, ingestAnalytics, matchOpportunity, normaliseAnalyticsPayload, normaliseFeedOpportunity, normaliseSubscription, proxyContracts, proxyOpportunities, purgeAnalytics, purgeOperationalData, renderDigest, snapshotFreshness, validateAnalyticsEvent, validateTurnstile } from "./index.js";

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

test("email delivery is ready only with a key, sender and reviewed domain", () => {
  assert.deepEqual(emailConfiguration({ EMAIL_FROM: "PublicSignal <alerts@publicsignal.ie>" }), { ready: false, status: "missing_api_key" });
  assert.deepEqual(emailConfiguration({ RESEND_API_KEY: "secret", EMAIL_FROM: "PublicSignal <alerts@publicsignal.ie>" }), { ready: false, status: "domain_not_verified" });
  assert.deepEqual(emailConfiguration({ RESEND_API_KEY: "secret", EMAIL_FROM: "PublicSignal <alerts@publicsignal.ie>", RESEND_DOMAIN_VERIFIED: "true" }), { ready: true, status: "ready" });
});

test("subscription creation fails before a database write when Resend is not ready", async () => {
  let prepared = false;
  const response = await createSubscription(new Request("https://publicsignal.ie/api/subscriptions", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ email: "bid@example.ie" }),
  }), { DB: { prepare: () => { prepared = true; } }, EMAIL_FROM: "PublicSignal <alerts@publicsignal.ie>" });
  assert.equal(response.status, 503);
  assert.equal(prepared, false);
  assert.deepEqual(await response.json(), { error: "Email delivery is not configured yet." });
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
  assert.equal(matchOpportunity({ ...opportunity, evidence: null }, subscription), false);
  assert.equal(matchOpportunity({ ...opportunity, evidence: null }, { ...subscription, minEvidence: 0 }), true);
});

test("normaliseFeedOpportunity does not invent an evidence score", () => {
  const unscored = normaliseFeedOpportunity({ id: "ted:no-score", source_identity: "no-score", source_lane: "ted_tender" });
  assert.equal(unscored.evidence, null);
  assert.equal(unscored.estimate, null);
  assert.equal(unscored.title, "TED notice no-score");
  assert.equal(normaliseFeedOpportunity({ id: "ted:scored", evidence: "82" }).evidence, 82);
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
  assert.deepEqual(validateAnalyticsEvent({ eventType: "page_open", targetSlug: "page-overview" }), { eventType: "page_open", targetSlug: "page-overview" });
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

test("snapshot freshness uses a bounded threshold and fails closed", () => {
  const now = Date.parse("2026-08-06T12:00:00Z");
  assert.deepEqual(snapshotFreshness("2026-08-06T00:00:00Z", {}, now), { status: "fresh", stale: false, ageHours: 12, maxAgeHours: 48 });
  assert.deepEqual(snapshotFreshness("2026-08-03T00:00:00Z", { SNAPSHOT_MAX_AGE_HOURS: "24" }, now), { status: "stale", stale: true, ageHours: 84, maxAgeHours: 24 });
  assert.deepEqual(snapshotFreshness(null, {}, now), { status: "unavailable", stale: true, ageHours: null, maxAgeHours: 48 });
});

test("operational retention removes abandoned watches and old delivery logs", async () => {
  const queries = [];
  const db = { prepare(sql) { queries.push(sql); return { async run() { return { meta: { changes: 2 } }; } }; } };
  assert.deepEqual(await purgeOperationalData({ DB: db }), { pendingSubscriptions: 2, deliveryLogs: 2 });
  assert.match(queries[0], /status='pending'.*'-7 days'/);
  assert.match(queries[1], /delivery_log.*'-365 days'/);
});

test("watch Turnstile lifecycle invalidates stale async renders", async () => {
  const source = await readFile(new URL("../public/app.js", import.meta.url), "utf8");
  assert.match(source, /function resetWatchTurnstile\(\)/);
  assert.match(source, /generation !== watchTurnstile\.generation/);
  assert.match(source, /!form\.isConnected/);
  assert.match(source, /turnstile\.reset/);
});

test("launch shell publishes the overview and trust surfaces", async () => {
  const [index, privacy, terms, methodology, robots, sitemap] = await Promise.all([
    readFile(new URL("../public/index.html", import.meta.url), "utf8"),
    readFile(new URL("../public/privacy.html", import.meta.url), "utf8"),
    readFile(new URL("../public/terms.html", import.meta.url), "utf8"),
    readFile(new URL("../public/methodology.html", import.meta.url), "utf8"),
    readFile(new URL("../public/robots.txt", import.meta.url), "utf8"),
    readFile(new URL("../public/sitemap.xml", import.meta.url), "utf8"),
  ]);
  assert.match(index, /data-view="overview"/);
  assert.match(index, /rel="canonical" href="https:\/\/publicsignal\.ie\/"/);
  assert.match(privacy, /Analytics events are deleted after 90 days/);
  assert.match(terms, /source notice/);
  assert.match(methodology, /never unioned into one total/);
  assert.match(robots, /Disallow: \/_private\//);
  assert.match(sitemap, /publicsignal\.ie\/methodology\.html/);
});

test("opportunity proxy preserves only the stable feed fields", async () => {
  const originalFetch = globalThis.fetch;
  let call;
  globalThis.fetch = async (url, options) => {
    call = { url: String(url), options };
    return new Response(JSON.stringify({ opportunities: [{ id: "ted:ABC", buyer_display_name: "Buyer", value_eur: 10, source_url: "https://source.example/notice", private_field: "drop" }] }), { status: 200 });
  };
  try {
    const response = await proxyOpportunities(new Request("https://publicsignal.ie/api/opportunities?within_days=30"), { PROCUREMENT_FEED_URL: "https://api.example/v1/procurement/opportunities", PROCUREMENT_FEED_TOKEN: "secret" });
    assert.equal(response.status, 200);
    const expected = normaliseFeedOpportunity({ id: "ted:ABC", buyer_display_name: "Buyer", value_eur: 10, source_url: "https://source.example/notice", private_field: "drop" });
    delete expected.deadline;
    assert.deepEqual(await response.json(), { source: "dail_tracker", builtAt: null, opportunities: [expected] });
    assert.equal(call.options.headers.authorization, "Bearer secret");
    assert.match(call.url, /within_days=30/);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("opportunity proxy returns a bounded unavailable response on remote failure", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("upstream details", { status: 503 });
  try {
    const response = await proxyOpportunities(new Request("https://publicsignal.ie/api/opportunities"), { PROCUREMENT_FEED_URL: "https://api.example/v1/procurement/opportunities", PROCUREMENT_FEED_TOKEN: "secret" });
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), { error: "Opportunity feed unavailable." });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("opportunity proxy reads the private snapshot through the asset binding", async () => {
  const snapshot = {
    schema: "publicsignal-procurement-snapshot/1",
    built_at: "2026-08-06T10:30:00+00:00",
    feed: {
      opportunities: [
        ...Array.from({ length: 250 }, (_, index) => ({ id: `ted:live-${index}`, source_identity: `live-${index}`, source_lane: "ted_tender", buyer_display_name: "Buyer", deadline: "2099-01-01", cpv_division: "72", source_url: `https://source.example/live-${index}` })),
        { id: "national:other", source_identity: "other", source_lane: "national_live", buyer_display_name: "Other", deadline: "2099-01-01", cpv_division: null, source_url: "https://source.example/other" },
      ],
    },
    contracts: {
      sectors: { status: "reviewed", rows: [] },
      buyers: { status: "reviewed", rows: [] },
      suppliers: { status: "reviewed", rows: [] },
    },
  };
  const response = await proxyOpportunities(new Request("https://publicsignal.ie/api/opportunities?sector=72&limit=2000"), {
    ASSETS: { fetch: async () => new Response(JSON.stringify(snapshot), { status: 200 }) },
  });
  assert.equal(response.status, 200);
  const body = await response.json();
  assert.equal(body.source, "private_snapshot");
  assert.equal(body.builtAt, "2026-08-06T10:30:00+00:00");
  assert.equal(body.opportunities.length, 250);
  assert.equal(body.opportunities[0].id, "ted:live-0");
  assert.equal(body.snapshotTotal, 251);
});

test("reviewed contract proxy exposes only the snapshot contracts", async () => {
  const contracts = {
    sectors: { status: "reviewed", rows: [] },
    buyers: { status: "reviewed", rows: [] },
    suppliers: { status: "reviewed", rows: [] },
  };
  const response = await proxyContracts({
    ASSETS: { fetch: async () => new Response(JSON.stringify({ schema: "publicsignal-procurement-snapshot/1", built_at: "2026-08-06T10:30:00+00:00", feed: { opportunities: [] }, contracts }), { status: 200 }) },
  });
  assert.equal(response.status, 200);
  assert.deepEqual((await response.json()).contracts, contracts);
});
