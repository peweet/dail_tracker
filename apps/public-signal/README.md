# PublicSignal

PublicSignal is a Cloudflare-ready procurement intelligence workspace for Irish SMEs and bid teams. The current product is a free, invite-only beta with no advertising. It exposes a bounded copy of Dáil Tracker procurement data through reviewed, provenance-carrying contracts.

`PublicSignal` is a working product name. A professional trademark and domain search is still required before launch.

## Product surfaces

- Opportunity desk with sector, buyer, deadline, value and evidence-coverage filters.
- Evidence brief for each notice, with source links and explicit data cautions.
- Sector map summarising current notices by source-stated CPV division.
- Buyer dossiers grouped only on the exact buyer names published in current notices.
- Supplier footprints over national award records, with TED activity linked only by exact unique CRO number.
- Saved watches and weekday or weekly email digests.
- Double opt-in confirmation, one-click unsubscribe and delivery logging.
- Responsive desktop and mobile layouts with keyboard and reduced-motion support.

The Opportunity Desk and email-watch matching use the Worker’s reviewed procurement snapshot. Sector, buyer and supplier pages use separate versioned contracts so opportunity estimates, national awards, TED award notices and payments cannot be silently blended.

## Cloudflare architecture

One Worker provides the application and API:

```text
Browser
  |  static assets and /api requests
Cloudflare Worker
  |-- Workers Static Assets: UI
  |-- D1: watches, confirmation state, delivery log and privacy-bounded analytics
  |-- Cron Trigger: 07:10 UTC, Monday to Friday
  |-- Resend API: confirmation and digest email
  `-- Dáil Tracker API: optional live opportunity feed
```

The scheduled handler sends weekday subscriptions on every weekday and weekly subscriptions on Monday. A delivery key prevents the same subscription and cadence being sent twice on the same date. Digests are suppressed when the embedded snapshot is missing a valid build time or is older than `SNAPSHOT_MAX_AGE_HOURS` (48 hours by default); the resulting structured error is visible in Cloudflare observability.

## Local setup

Prerequisites: Node 20+, a Cloudflare account for Worker integration testing, and a Resend account only if real email should be sent. The prototype pins Wrangler 4.86 because the repository currently runs Node 20; newer Wrangler releases require Node 22.

```powershell
Set-Location apps/public-signal
npm install
npx wrangler d1 create public-signal
```

Replace the zero UUID in `wrangler.jsonc` with the D1 ID returned by Cloudflare. Then:

```powershell
Copy-Item .dev.vars.example .dev.vars
npm run db:migrate:local
npm run dev
```

The app works without a Resend key in clearly labelled draft mode. Watch drafts are stored in the browser and are not posted as subscriptions until verified email delivery is configured.

## Email configuration

1. Verify `publicsignal.ie` (or a dedicated sending subdomain) in Resend and add the DNS records Resend supplies.
2. Put `RESEND_API_KEY` in `.dev.vars` locally.
3. For production, run `npx wrangler secret put RESEND_API_KEY`.
4. Set `EMAIL_FROM` and `APP_URL` in `wrangler.jsonc` to the verified production domain.
5. Change `RESEND_DOMAIN_VERIFIED` to `"true"` only after Resend reports the domain as verified, then deploy.
6. Apply the production migration with `npm run db:migrate:remote`.

The Worker reports email as ready only when an API key, sender and explicit domain-verification flag are present. It rejects subscription writes while delivery is unavailable, and removes a pending row if the confirmation send fails. Confirmation emails are idempotent per stored watch. Digests carry their own idempotency key and every message includes a watch-specific unsubscribe link.

Before public signup, configure Cloudflare Turnstile. The Worker also enforces a three-request-per-minute, per-email Worker Rate Limiting binding, with the existing D1 fifteen-minute backstop.

### Subscription protection

Create a Turnstile widget for the deployed hostname, then configure its site key and secret. The site key is exposed only through `/api/config` when the corresponding secret is present. The server validates every token with Cloudflare Siteverify; a widget alone is not sufficient.

```powershell
Set-Location apps/public-signal
npx wrangler secret put TURNSTILE_SECRET_KEY
```

Set `TURNSTILE_SITE_KEY` in the Worker environment, or `.dev.vars` for local development. Do not commit either key. With no `TURNSTILE_SECRET_KEY`, local preview remains usable and no widget appears; never deploy a public signup service in that mode.

`SUBSCRIPTION_LIMITER` uses rate-limit namespace `801503`. It must be unique within the Cloudflare account; change it before deployment if that namespace is already used by another Worker.

## Live data contract

Set `PROCUREMENT_FEED_URL` to a JSON endpoint. The existing Dáil Tracker route is shaped like:

```text
/v1/procurement/opportunities?limit=100
```

It requires `PROCUREMENT_FEED_TOKEN` to match the API's `PUBLIC_SIGNAL_FEED_TOKEN`. Keep both values server-side: the Worker sends the bearer token upstream and never returns it to a browser. The API fails closed when the token is absent or invalid.

The feed returns up to 2,000 opportunities with lane-level coverage and cautions. The current corpus contains more than 200 open notices, so the built snapshot is no longer truncated at the former 200-row ceiling. The Worker only passes through these stable display fields:

- `id`, `title`, `buyer_display_name`
- `cpv_division`, when supplied by the source
- `deadline`, `value_eur`, `source_url`
- `source_lane` and the source's own caution label

National eTenders live records have no CPV field in this snapshot, so they remain unclassified rather than being inferred from their title. Values in both forward lanes are advertised/planned estimates, not awards, payments, or market totals.

The same snapshot contains three reviewed public contracts:

- `publicsignal-sector-notice-summary/1`: one row per stated CPV division in current forward notices.
- `publicsignal-buyer-notice-summary/1`: one row per exact published buyer display name in current forward notices.
- `publicsignal-supplier-award-summary/1`: one row per normalised company-class supplier in national awards. TED award-notice counts are attached only through an exact unique CRO company number. National and TED figures stay in separate columns, and no payment values are joined.

The corresponding private API evidence route is `/v1/procurement/opportunities/{opportunity_id}/brief`. It adds cross-register buyer context only for a curated exact buyer match; award and payment disclosures remain separate lanes. The public Worker does not proxy this detail endpoint yet.

### Private snapshot deployment

On the current Cloudflare Workers Free plan, PublicSignal serves a compact, reviewed snapshot through the Worker itself. The snapshot is built from the Dail Tracker procurement views, uploaded as a Worker asset, and explicitly blocked from direct public paths. The Worker exposes only the public-safe opportunity fields to the website and email scheduler.

Refresh the snapshot after a Dail Tracker data refresh, then deploy the Worker:

```powershell
uv run --no-sync python apps/public-signal/private-api/build_snapshot.py --output apps/public-signal/public/_private/procurement-snapshot.json
Set-Location apps/public-signal
npx wrangler deploy
```

For the beta operator workflow, the guarded script builds the snapshot, rejects a row count at or below the former 200-row cap, checks all three reviewed contracts, runs the Node suite and performs a Wrangler dry run. Add `-Deploy` only after reviewing its timestamp and counts:

```powershell
& apps/public-signal/scripts/refresh.ps1
& apps/public-signal/scripts/refresh.ps1 -Deploy
```

The public UI keeps the last-known-good snapshot visible, labels it overdue after 48 hours, and tells users that email digests are paused. Configure a Cloudflare log alert for the structured `snapshot_stale_digest_suppressed` event before moving beyond the invite-only beta.

`private-api/` is the companion FastAPI container implementation, tested locally and ready if the account is upgraded to Workers Paid. It is not deployed on the current plan because Cloudflare Containers require Workers Paid.

## Privacy-bounded product analytics

PublicSignal records a small first-party event vocabulary to improve navigation and evidence-brief usability. Events contain only an anonymous per-tab session identifier (hashed by the Worker), an allowlisted event type and a semantic target slug. Search terms, form values, email addresses, IP addresses, full user agents and URL query strings are never sent or stored. The browser uses `sessionStorage`, never persistent identity storage, and does not send events when Global Privacy Control or Do Not Track is enabled. Analytics failures are ignored by the UI.

Analytics events are retained for 90 days, then removed by the scheduled Worker cleanup. Set `ANALYTICS_RETENTION_DAYS` between 30 and 365 if the reviewed retention decision changes. Configure the `ANALYTICS_HASH_SALT` Worker secret before production use so session hashes cannot be reproduced from the public source alone. Ingestion fails closed with HTTP 503 until this secret exists:

```powershell
npx wrangler secret put ANALYTICS_HASH_SALT
```

Analytics remains disabled until both this secret and the `ANALYTICS_LIMITER` binding are present.

The Worker applies a separate `ANALYTICS_LIMITER` binding (100 requests per minute per transiently hashed Cloudflare IP, or per hashed session for local requests) in addition to the subscription limiter. Each request carries exactly one event, so the limiter also bounds D1 writes. The IP is used only to derive the limiter key and is never stored, logged or returned. Keep namespace `801504` distinct from the subscription namespace `801503` when deploying.

After applying migration `0002_analytics.sql`, an operator can inspect aggregate reports without exposing session hashes or raw events:

```sql
-- Opens, approximate unique sessions and event counts by day
SELECT date(occurred_at) AS day,
       COUNT(*) AS events,
       COUNT(DISTINCT session_hash) AS approximate_sessions,
       SUM(event_type = 'app_open') AS app_opens,
       SUM(event_type = 'page_open') AS page_opens
FROM analytics_events
GROUP BY day
ORDER BY day DESC;

-- Top semantic actions (no identifiers or search terms)
SELECT event_type, target_slug, COUNT(*) AS events,
       COUNT(DISTINCT session_hash) AS approximate_sessions
FROM analytics_events
GROUP BY event_type, target_slug
ORDER BY events DESC
LIMIT 25;
```

These are approximate session counts because a session is an ephemeral browser tab, not an identified person.

## API routes

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/api/health` | Binding and feed mode check |
| `GET` | `/api/opportunities` | Public-safe proxy for the configured private opportunity feed |
| `GET` | `/api/contracts` | Reviewed sector, buyer and supplier snapshot contracts |
| `POST` | `/api/events` | Validate and store bounded, anonymous product events |
| `POST` | `/api/subscriptions` | Store a pending watch and send confirmation |
| `GET` | `/api/subscriptions/confirm?token=...` | Activate a watch |
| `GET` | `/api/subscriptions/unsubscribe?token=...` | Stop one watch |
| `POST` | `/api/digests/preview` | Return current matches without sending email |

## Verification

```powershell
npm run check
npx wrangler deploy --dry-run
```

For a local scheduled-event test while `wrangler dev` is running:

```text
http://localhost:8787/cdn-cgi/handler/scheduled?format=json
```

## Current Cloudflare deployment

The production Worker and D1 database are deployed at:

```text
https://public-signal.publicsignal.workers.dev
```

`publicsignal.ie` is already configured as the intended application URL, but the
custom-domain route remains commented in `wrangler.jsonc` until the domain is
visible in the .IE registry and has an active Cloudflare zone. Once it is active,
uncomment the `routes` block and run `npx wrangler deploy`.

## Free-beta launch checklist

- Verify the Resend sending domain, add `RESEND_API_KEY` as a Worker secret and set `RESEND_DOMAIN_VERIFIED` only after verification succeeds.
- Configure the live Turnstile hostname and secrets before opening subscriptions publicly.
- Make `hello@publicsignal.ie` and `privacy@publicsignal.ie` operational, and review the published beta privacy notice and terms with the named operator details before invitations are sent.
- Configure a Cloudflare observability alert for stale-snapshot digest suppression and run the guarded refresh workflow each weekday during the beta.
- Activate the custom-domain route only after `publicsignal.ie` resolves in an active Cloudflare zone.
- Rehearse browse, source-open, watch confirmation, digest link and unsubscribe flows on desktop and mobile.
- Add amendment and deadline-change events, not just new-notice polling.
- Complete trademark, company-name and domain clearance for PublicSignal.

Authentication, team roles, billing and shared pursuit workflow are deliberately outside the free-beta launch. Add them only after the six-week beta shows repeat use and willingness to pay.
