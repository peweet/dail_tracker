# PublicSignal

PublicSignal is a Cloudflare-ready procurement intelligence prototype for Irish bid teams and public-sector market advisers. It turns live notices into evidence briefs built around buyer history, previous suppliers, competition context, disclosed payments or purchase orders, and estimated contract-end signals.

`PublicSignal` is a working product name. A professional trademark and domain search is still required before launch.

## Product surfaces

- Opportunity desk with sector, buyer, deadline, value and evidence-coverage filters.
- Evidence brief for each notice, with source links and explicit data cautions.
- Sector map showing where the underlying corpus is commercially usable.
- Buyer dossiers that keep paid and committed disclosures separate.
- Supplier footprints across national awards, TED and public-body disclosures.
- Saved watches and weekday or weekly email digests.
- Double opt-in confirmation, one-click unsubscribe and delivery logging.
- Responsive desktop and mobile layouts with keyboard and reduced-motion support.

The Opportunity Desk and email-watch matching use the Worker’s reviewed procurement snapshot. The Sector map, Buyer dossiers and Supplier footprints remain labelled prototype views until each gets a separate, reviewed public data contract.

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

The scheduled handler sends weekday subscriptions on every weekday and weekly subscriptions on Monday. A delivery key prevents the same subscription and cadence being sent twice on the same date.

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

1. Verify a sending domain in Resend.
2. Put `RESEND_API_KEY` in `.dev.vars` locally.
3. For production, run `npx wrangler secret put RESEND_API_KEY`.
4. Set `EMAIL_FROM` and `APP_URL` in `wrangler.jsonc` to the verified production domain.
5. Apply the production migration with `npm run db:migrate:remote`.

Confirmation emails are idempotent per stored watch. Digests carry their own idempotency key and every message includes a watch-specific unsubscribe link.

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

The feed returns an `opportunities` envelope with lane-level coverage and cautions. The Worker only passes through these stable display fields:

- `id`, `title`, `buyer_display_name`
- `cpv_division`, when supplied by the source
- `deadline`, `value_eur`, `source_url`
- `source_lane` and the source's own caution label

National eTenders live records have no CPV field in this snapshot, so they remain unclassified rather than being inferred from their title. Values in both forward lanes are advertised/planned estimates, not awards, payments, or market totals.

The corresponding private API evidence route is `/v1/procurement/opportunities/{opportunity_id}/brief`. It adds cross-register buyer context only for a curated exact buyer match; award and payment disclosures remain separate lanes. The public Worker does not proxy this detail endpoint yet.

### Private snapshot deployment

On the current Cloudflare Workers Free plan, PublicSignal serves a compact, reviewed snapshot through the Worker itself. The snapshot is built from the Dail Tracker procurement views, uploaded as a Worker asset, and explicitly blocked from direct public paths. The Worker exposes only the public-safe opportunity fields to the website and email scheduler.

Refresh the snapshot after a Dail Tracker data refresh, then deploy the Worker:

```powershell
uv run --no-sync python apps/public-signal/private-api/build_snapshot.py --output apps/public-signal/public/_private/procurement-snapshot.json
Set-Location apps/public-signal
npx wrangler deploy
```

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

## Production work still required

- Add reviewed, separate public data contracts for the Sector map, Buyer dossiers and Supplier footprints before presenting them as live data.
- Add authentication for private workspaces, team roles and shared pursuit notes.
- Configure the live Turnstile hostname and secrets before opening subscriptions publicly.
- Publish the reviewed privacy notice with the analytics vocabulary and 90-day retention policy described above.
- Monitor feed freshness and suppress digests when freshness exceeds the agreed threshold.
- Add amendment and deadline-change events, not just new-notice polling.
- Complete trademark, company-name and domain clearance for PublicSignal.
