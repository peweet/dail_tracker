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

The browser currently uses a small, labelled prototype dataset derived from the August 2026 local snapshot. The Worker can instead fetch an authenticated JSON opportunity feed.

## Cloudflare architecture

One Worker provides the application and API:

```text
Browser
  |  static assets and /api requests
Cloudflare Worker
  |-- Workers Static Assets: UI
  |-- D1: watches, confirmation state and delivery log
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

The app works without a Resend key. Subscriptions are stored as pending and the API reports preview mode, but no email leaves the system.

## Email configuration

1. Verify a sending domain in Resend.
2. Put `RESEND_API_KEY` in `.dev.vars` locally.
3. For production, run `npx wrangler secret put RESEND_API_KEY`.
4. Set `EMAIL_FROM` and `APP_URL` in `wrangler.jsonc` to the verified production domain.
5. Apply the production migration with `npm run db:migrate:remote`.

Confirmation emails are idempotent per stored watch. Digests carry their own idempotency key and every message includes a watch-specific unsubscribe link.

Before public signup, add Cloudflare Turnstile or a Worker rate-limiting binding. The prototype includes a narrow per-email retry limit, but that is not a complete abuse-control layer.

## Live data contract

Set `PROCUREMENT_FEED_URL` to a JSON endpoint. The existing Dáil Tracker route is shaped like:

```text
/v1/procurement/open-tenders?only_open=true&limit=100
```

The Worker accepts an array or an envelope under `data`, `result` or `items`. It recognises these source fields:

- `publication_number` or `resource_id`
- `buyer_name` or `buyer`
- `cpv_division` or `sector`
- `submission_deadline`
- `estimated_value_eur`
- `notice_url` or `detail_url`

If the feed is private, store its bearer token with `npx wrangler secret put PROCUREMENT_FEED_TOKEN`.

The current open-tenders route supplies opportunity facts but not the full buyer and supplier evidence brief. A production feed should compose those extra lanes server-side and attach a per-lane freshness and coverage manifest.

## API routes

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/api/health` | Binding and feed mode check |
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

- Connect the opportunity feed to the deployed API and add the composed evidence endpoint.
- Add authentication for private workspaces, team roles and shared pursuit notes.
- Add Turnstile or platform rate limiting before opening subscriptions publicly.
- Record consent wording and retention periods in a privacy notice.
- Monitor feed freshness and suppress digests when freshness exceeds the agreed threshold.
- Add amendment and deadline-change events, not just new-notice polling.
- Complete trademark, company-name and domain clearance for PublicSignal.
