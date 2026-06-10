/**
 * Cloudflare Worker — reverse-proxy a real domain onto a Streamlit Community
 * Cloud app, e.g.  https://dailtracker.ie  ->  https://dailtracker.streamlit.app
 *
 * WHY A WORKER (and not just a proxied CNAME):
 *   Streamlit Community Cloud routes incoming requests by the Host header. A
 *   plain orange-cloud CNAME forwards `Host: dailtracker.ie`, which Streamlit's
 *   load balancer does not recognise -> wrong app / 404. This Worker rewrites
 *   the request onto the *.streamlit.app origin (correct Host + SNI), so routing
 *   works, and passes the WebSocket upgrade through untouched — Streamlit's live
 *   widget interaction and reload run over the `/_stcore/stream` WebSocket, so
 *   WS support is mandatory, not optional.
 *
 * The only thing you must edit is ORIGIN_HOST below.
 */

// Your real *.streamlit.app subdomain (set this in the Streamlit app settings,
// "App URL"/custom subdomain). NO scheme, NO trailing slash.
const ORIGIN_HOST = "dailtracker.streamlit.app";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const publicHost = url.hostname; // e.g. dailtracker.ie — preserve for redirect rewrites

    // Point the request at the Streamlit origin. Because we set url.hostname,
    // fetch() sends `Host: dailtracker.streamlit.app` AND uses it as the TLS SNI,
    // which is exactly what Streamlit Cloud's router needs.
    url.hostname = ORIGIN_HOST;
    url.protocol = "https:";
    url.port = "";

    const proxied = new Request(url.toString(), {
      method: request.method,
      headers: request.headers,
      body: request.body,
      redirect: "manual", // never auto-follow; we rewrite Location ourselves
    });

    // fetch() transparently negotiates the WebSocket upgrade when the incoming
    // request carries `Upgrade: websocket`, returning a 101 response that still
    // carries the live `webSocket` — returning it as-is wires the socket through.
    const response = await fetch(proxied);

    // If Streamlit issues an absolute redirect to its own host, rewrite it back
    // to the public host so the browser never bounces to *.streamlit.app.
    const location = response.headers.get("Location");
    if (location && location.includes(ORIGIN_HOST)) {
      const headers = new Headers(response.headers);
      headers.set("Location", location.replaceAll(ORIGIN_HOST, publicHost));
      return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers,
      });
    }

    // Pass everything else (incl. the 101 + webSocket) straight through.
    return response;
  },
};
