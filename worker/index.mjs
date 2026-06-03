/**
 * sortsafe-proxy — CORS-bypass fetch proxy for the browser-side scrapers.
 *
 *   GET /fetch?url=<encoded https URL>   → fetches the target with browser-like
 *                                          headers, returns the body with CORS
 *                                          headers (always HTTP 200 to the
 *                                          browser; real upstream status is in
 *                                          the `x-proxy-status` header).
 *   GET /healthz                         → "ok"
 *
 * Host allowlist keeps this from being an open proxy. Responses are edge-cached
 * for a few minutes so repeated scrapes don't hammer the target.
 */
const ALLOW = [/(^|\.)amazon\.com$/i];

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const ACCEPT =
  "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8";

const CORS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET,OPTIONS",
  "access-control-allow-headers": "*",
  "access-control-max-age": "86400",
};

export default {
  async fetch(request, _env, ctx) {
    if (request.method === "OPTIONS") return new Response(null, { headers: CORS });

    const url = new URL(request.url);
    if (url.pathname === "/healthz" || url.pathname === "/")
      return new Response("ok", { headers: { ...CORS, "content-type": "text/plain" } });
    if (url.pathname !== "/fetch")
      return new Response("not found", { status: 404, headers: CORS });

    const target = url.searchParams.get("url");
    if (!target) return new Response("missing url param", { status: 400, headers: CORS });

    let t;
    try {
      t = new URL(target);
    } catch {
      return new Response("bad url", { status: 400, headers: CORS });
    }
    if (t.protocol !== "https:" || !ALLOW.some((re) => re.test(t.hostname)))
      return new Response(`host not allowed: ${t.hostname}`, { status: 403, headers: CORS });

    const cache = caches.default;
    const cacheKey = new Request(url.toString(), { method: "GET" });
    const cached = await cache.match(cacheKey);
    if (cached) return withCors(cached);

    let upstream;
    try {
      upstream = await fetch(t.toString(), {
        headers: {
          "user-agent": UA,
          accept: ACCEPT,
          "accept-language": "en-US,en;q=0.9",
          "upgrade-insecure-requests": "1",
        },
        redirect: "follow",
        cf: { cacheTtl: 300, cacheEverything: true },
      });
    } catch (e) {
      return new Response(`upstream fetch failed: ${e}`, { status: 502, headers: CORS });
    }

    const body = await upstream.text();
    const headers = new Headers(CORS);
    headers.set("content-type", upstream.headers.get("content-type") || "text/html; charset=utf-8");
    headers.set("x-proxy-status", String(upstream.status));
    headers.set("x-proxy-len", String(body.length));
    // Always 200 to the browser so the scraper can read partial/blocked bodies too.
    const resp = new Response(body, { status: 200, headers });
    if (upstream.status === 200) ctx.waitUntil(cache.put(cacheKey, resp.clone()));
    return resp;
  },
};

function withCors(res) {
  const h = new Headers(res.headers);
  for (const [k, v] of Object.entries(CORS)) h.set(k, v);
  return new Response(res.body, { status: res.status, headers: h });
}
