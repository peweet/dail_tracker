# Pointing `dailtracker.ie` at the Streamlit Cloud app (via Cloudflare)

Streamlit **Community Cloud has no native custom-domain support** — an app only
ever lives at `*.streamlit.app`. This doc gets a real domain (`dailtracker.ie`)
in front of it using a **Cloudflare Worker reverse proxy**, so the address bar
keeps showing `dailtracker.ie` and TLS is handled for free.

Files in `deploy/cloudflare/`:

- `dailtracker-proxy.worker.js` — the Worker (edit `ORIGIN_HOST` only)
- `wrangler.toml` — deploy config (`npx wrangler deploy`)

---

## Why a Worker and not a plain proxied CNAME

Streamlit Cloud routes by the **Host header**. A normal orange-cloud CNAME
forwards `Host: dailtracker.ie`, which Streamlit's router doesn't recognise →
wrong app / 404. The Worker rewrites the request onto `*.streamlit.app` (correct
Host **and** TLS SNI) and passes the `/_stcore/stream` **WebSocket** through —
Streamlit's interactivity dies without WS, so that part is non-negotiable.

> Cloudflare *Origin Rules* (Host Header Override + Resolve Override) is a
> no-Worker alternative, but Streamlit's load balancer is SNI-routed and Origin
> Rules don't reliably set the outbound SNI — so it often half-works. The Worker
> does a clean `fetch()` to `https://dailtracker.streamlit.app`, which gets SNI
> right. Use the Worker.

---

## One-time setup

### 0. Register / connect the domain
`.ie` domains are issued by **IEDR** and require an Irish connection — fine for
this project, just not open registration. Register `dailtracker.ie` with any
IEDR-accredited registrar (Blacknight, Register365, etc.).

### 1. Add the domain to Cloudflare
1. Cloudflare dashboard → **Add a site** → `dailtracker.ie` (Free plan is fine).
2. Cloudflare gives you two nameservers. At your registrar, **change the domain's
   nameservers** to those two. Propagation: minutes to a few hours.

### 2. Set the Streamlit subdomain (the proxy target)
In `share.streamlit.io` → your app → **Settings → General → App URL**, set the
subdomain to **`dailtracker`** so the app is reachable at
`https://dailtracker.streamlit.app`. Confirm that URL loads on its own first.
(If you pick a different subdomain, update `ORIGIN_HOST` in the Worker.)

### 3. Create the DNS records that make the hostnames proxy
The Worker does the real fetching, so the DNS record just needs to **resolve and
be proxied (orange cloud ON)**. Use a placeholder address:

| Type  | Name            | Target / Content | Proxy        |
|-------|-----------------|------------------|--------------|
| AAAA  | `dailtracker.ie`| `100::`          | Proxied (🟠) |
| CNAME | `www`           | `dailtracker.ie` | Proxied (🟠) |

(`100::` is the IPv6 discard prefix — a harmless placeholder; the Worker
intercepts before anything is sent there.)

### 4. SSL/TLS mode
Cloudflare → **SSL/TLS → Overview → Full** (not Flexible, not Off). The Worker
talks HTTPS to Streamlit, so Full is correct. Leave "Always Use HTTPS" on.

### 5. Deploy the Worker
```bash
cd deploy/cloudflare
# edit ORIGIN_HOST in dailtracker-proxy.worker.js if your subdomain differs
npx wrangler login      # one-time, browser auth
npx wrangler deploy
```
`wrangler.toml` already binds the Worker to `dailtracker.ie/*` and
`www.dailtracker.ie/*`.

### 6. Test
- `https://dailtracker.ie` loads the app, address bar stays on `dailtracker.ie`.
- Interact with a widget / change a page — if the UI updates (doesn't hang on
  "Connecting…"), the WebSocket proxy is working.
- `https://www.dailtracker.ie` works too.

---

## Caveats / known edges

- **Unsupported by Streamlit.** Proxying Community Cloud isn't officially
  supported; it works but Streamlit could change routing and break it. If the
  domain becomes important, the durable path is to containerise the app on a host
  with first-class custom domains (Cloud Run / Render / Fly.io) — the Dockerfile
  is trivial (`streamlit run utility/app.py --server.port $PORT`).
- **Cold starts.** Community Cloud sleeps idle apps; the first hit after sleep is
  slow regardless of the proxy.
- **Redirect rewriting** is handled for absolute redirects to `*.streamlit.app`;
  Streamlit rarely emits those, but the Worker covers it.
- **`www` → apex.** If you'd rather `www` 301-redirect to the apex instead of
  also proxying, drop the `www` route from `wrangler.toml` and add a Cloudflare
  Redirect Rule instead.
