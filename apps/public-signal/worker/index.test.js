import assert from "node:assert/strict";
import test from "node:test";

import { matchOpportunity, normaliseSubscription, renderDigest } from "./index.js";

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
