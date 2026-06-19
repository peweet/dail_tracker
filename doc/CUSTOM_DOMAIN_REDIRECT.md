# Pointing `dailtracker.ie` at the Streamlit app via a 301 redirect

This is the **simple redirect** path: `dailtracker.ie` issues an HTTP **301** to
`https://dailtracker.streamlit.app`, so the browser lands on the real Streamlit
URL (the address bar ends up showing `*.streamlit.app`). No Worker, no proxy —
just a free **Cloudflare Redirect Rule**. The destination lives in one editable
field, so you can point it at a placeholder now and flip it to the real app URL
once that's locked.

> Want the address bar to keep showing `dailtracker.ie` instead of bouncing to
> `*.streamlit.app`? That's the **reverse-proxy** path — use the pre-built
> Cloudflare Worker in `deploy/cloudflare/` and follow
> [`CUSTOM_DOMAIN_CLOUDFLARE.md`](CUSTOM_DOMAIN_CLOUDFLARE.md) instead. The two
> are mutually exclusive; pick one.

Almost everything below is dashboard work on the registrar / Cloudflare side.
There is **no app-code change** for a 301 redirect.

---

## Phase 0 — Lock the Streamlit destination (do this first)

In `share.streamlit.io` → your app → **Settings → General → App URL**, set the
subdomain to **`dailtracker`** so the app is reachable at
`https://dailtracker.streamlit.app`. Confirm that URL loads on its own before
going further. This is the "streamlit link" you paste into the redirect in
Phase 4 — if it isn't final yet, point the redirect at a placeholder and change
the one field later.

## Phase 1 — Register the domain

`.ie` domains are issued by **IEDR** and require an Irish connection (fine for
this project, just not open registration). Register `dailtracker.ie` with any
IEDR-accredited registrar — **Blacknight** or **Register365** (~€20–30/yr).

## Phase 2 — Put the domain on Cloudflare (Free plan)

1. Cloudflare dashboard → **Add a site** → `dailtracker.ie`.
2. Cloudflare gives you two nameservers. At the registrar, **change the domain's
   nameservers** to those two. Propagation: minutes to a few hours.

## Phase 3 — DNS placeholder so the hostnames resolve *through* Cloudflare

A Redirect Rule only fires on traffic that reaches Cloudflare's edge, so the
hostnames must resolve and be **proxied (orange cloud ON)**. Use the IPv6
discard prefix as a harmless placeholder — nothing is ever sent there because
the redirect intercepts first:

| Type  | Name             | Content          | Proxy        |
|-------|------------------|------------------|--------------|
| AAAA  | `dailtracker.ie` | `100::`          | Proxied (🟠) |
| CNAME | `www`            | `dailtracker.ie` | Proxied (🟠) |

## Phase 4 — Create the Redirect Rule (this is where you "enter the link")

Cloudflare → **Rules → Redirect Rules → Create rule**:

- **When incoming requests match**: `Hostname` `equals` `dailtracker.ie`
  — OR — to cover both, use *Custom filter expression*:
  `(http.host eq "dailtracker.ie") or (http.host eq "www.dailtracker.ie")`
- **Then**:
  - Type: **Static** redirect
  - URL: **`https://dailtracker.streamlit.app`**
  - Status code: **301**
  - **Preserve query string**: ON (Streamlit uses `?page=…` for navigation)

*(Optional — carry the full path too)* Use a **Dynamic** redirect with
expression `concat("https://dailtracker.streamlit.app", http.request.uri.path)`.
For a homepage entry point the Static URL above is enough.

The Destination URL field is the single source of truth — change it here anytime
and the whole domain re-points instantly.

## Phase 5 — TLS

Cloudflare → **SSL/TLS → Overview → Full**, and leave **Always Use HTTPS** on so
`http://` and the `www` host both upgrade and redirect cleanly.

## Phase 6 — Verify

- `http://dailtracker.ie`, `https://dailtracker.ie`, and
  `https://www.dailtracker.ie` all 301 to `dailtracker.streamlit.app`.
- `curl -sI https://dailtracker.ie` shows `301` and the right `Location:` header.

---

## Caveats / known edges

- **Visitors see `*.streamlit.app`.** A 301 hands off entirely; users bookmark
  and share the Streamlit URL, not your domain. If branding matters, switch to
  the reverse-proxy path ([`CUSTOM_DOMAIN_CLOUDFLARE.md`](CUSTOM_DOMAIN_CLOUDFLARE.md)) —
  non-destructive: deploy the existing Worker and swap this Redirect Rule for the
  Worker route.
- **Cold starts.** Community Cloud sleeps idle apps; the first hit after sleep is
  slow regardless of the redirect.
- **Unofficial.** Streamlit Community Cloud has no native custom-domain support.
  The durable path if the domain becomes important is to containerise the app on
  a host with first-class custom domains (Cloud Run / Render / Fly.io) — the
  Dockerfile is trivial (`streamlit run utility/app.py --server.port $PORT`).
