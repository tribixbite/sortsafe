/**
 * Local CORS-bypass proxy for the /gpus pipeline.
 *
 * Mirrors what the Cloudflare Worker will eventually do:
 *   GET /fetch?url=<encoded>     → fetches target URL, returns body
 *                                  with permissive CORS headers, spoofed
 *                                  browser User-Agent.
 *   GET /healthz                 → 200 ok
 *
 * In-memory LRU cache (TTL configurable per path family) so repeated
 * scrapes don't burn out Amazon's rate limit on this dev box.
 *
 * Usage:
 *   bun scripts/gpu-proxy-local.ts        # listens on :8787
 *   PROXY_PORT=9999 bun scripts/gpu-proxy-local.ts
 */
const PORT = parseInt(process.env.PROXY_PORT ?? '8787', 10);
const UA = 'Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const ACCEPT = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8';

interface CacheEntry { body: string; status: number; storedAt: number; ttlMs: number; }
const cache = new Map<string, CacheEntry>();
const MAX_CACHE = 500;

const corsHeaders = {
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'GET,OPTIONS',
  'access-control-allow-headers': '*',
  'access-control-max-age': '86400',
};

function pickTtl(url: string): number {
  if (/\/s\?|\/s\//.test(url)) return 5 * 60_000;          // search pages: 5 min
  if (/offer-listing|\/dp\//.test(url)) return 10 * 60_000; // PDPs / offers: 10 min
  return 30 * 60_000;                                       // default: 30 min
}

function evictIfFull() {
  if (cache.size <= MAX_CACHE) return;
  const oldest = [...cache.entries()].sort((a, b) => a[1].storedAt - b[1].storedAt)[0];
  if (oldest) cache.delete(oldest[0]);
}

async function proxyFetch(rawUrl: string): Promise<Response> {
  const cached = cache.get(rawUrl);
  if (cached && Date.now() - cached.storedAt < cached.ttlMs) {
    return new Response(cached.body, {
      status: cached.status,
      headers: { ...corsHeaders, 'content-type': 'text/html; charset=utf-8', 'x-proxy-cache': 'HIT' },
    });
  }
  let upstream: Response;
  try {
    upstream = await fetch(rawUrl, {
      headers: {
        'user-agent': UA,
        'accept': ACCEPT,
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
      },
      redirect: 'follow',
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: 'upstream-fetch-failed', message: (e as Error).message }), {
      status: 502, headers: { ...corsHeaders, 'content-type': 'application/json' },
    });
  }
  const body = await upstream.text();
  // Don't cache obvious bot-detection / soft-error pages or anything Amazon
  // serves as "Sorry! Something went wrong" — otherwise we'd serve them for
  // up to 30 min until TTL expires, blocking recovery.
  const isBotBlock = body.length < 5000 && /Sorry! Something went wrong/i.test(body);
  if (!isBotBlock && upstream.status < 500) {
    cache.set(rawUrl, { body, status: upstream.status, storedAt: Date.now(), ttlMs: pickTtl(rawUrl) });
    evictIfFull();
  }
  return new Response(body, {
    status: upstream.status,
    headers: { ...corsHeaders, 'content-type': 'text/html; charset=utf-8', 'x-proxy-cache': 'MISS' },
  });
}

const server = Bun.serve({
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);
    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: corsHeaders });
    if (url.pathname === '/healthz') {
      return new Response(JSON.stringify({ ok: true, cacheSize: cache.size }), {
        headers: { ...corsHeaders, 'content-type': 'application/json' },
      });
    }
    if (url.pathname === '/fetch') {
      const target = url.searchParams.get('url');
      if (!target) {
        return new Response(JSON.stringify({ error: 'missing url query param' }), {
          status: 400, headers: { ...corsHeaders, 'content-type': 'application/json' },
        });
      }
      try {
        new URL(target); // validate
      } catch {
        return new Response(JSON.stringify({ error: 'invalid url' }), {
          status: 400, headers: { ...corsHeaders, 'content-type': 'application/json' },
        });
      }
      console.log(`[proxy] ${target}`);
      return await proxyFetch(target);
    }
    return new Response('not found', { status: 404, headers: corsHeaders });
  },
});

console.log(`gpu-proxy listening on http://localhost:${server.port}`);
console.log(`  GET /fetch?url=<encoded>`);
console.log(`  GET /healthz`);
