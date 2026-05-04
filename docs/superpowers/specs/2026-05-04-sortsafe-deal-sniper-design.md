# sortsafe — Amazon deal sniper design

**Status:** draft v1 · **Date:** 2026-05-04 · **Author:** willstone (with Claude Opus 4.7)

## 1. Problem statement

Find Amazon deals on **GPUs (3090/4090/5090), RAM, and SSDs** as soon as they appear. Surface the absolute best current offering per filter set without showing stale or expired listings. Personal tool used by the author and ~5 friends. Existing prototype at `/gpus` (migrated to this repo from `tribixbite/torch`) proves the Keepa-driven pipeline; this design extends and hardens it.

### Concrete pain points the design must fix

1. **Discovery stops at 20/model.** Once the pool reaches the floor, no new ASINs are searched. New launches go undetected.
2. **Refresh button uses Amazon scraping that gets bot-blocked** — visitor sees an apparent "Refresh" affordance that fails silently when our IP is flagged.
3. **No per-seller / Amazon Warehouse data.** Keepa `/product` without `offers=N` returns aggregate condition prices but not seller name, ratings, or shipping origin. Cards show "no seller" for non-new listings, and the `Min seller ★` filter excludes everything because the field is null.
4. **No price history surfaced.** A snapshots store exists in IndexedDB but no UI uses it. There's no concept of "this is a $400 drop from the 30-day median."
5. **Stale local infrastructure.** Cron runs on the author's Termux phone; if the device is off, the seed file goes stale.
6. **No alerts.** Friends only see deals if they happen to load the page when the deal is fresh.

### Goals

1. **Sub-15-minute detection latency** for any new used/refurbished/Warehouse listing on a tracked SKU.
2. **Zero stale data** in the visible UI — listings that drop from Keepa's "current" price within a refresh cycle disappear from the page.
3. **Per-seller granularity** — show seller name, rating, ships-from for every listing where Keepa has the data.
4. **Deal scoring** that distinguishes "$1,400 used 3090" (good) from "$1,400 used 3090 when median is $1,300" (mediocre). User-pickable scoring metric.
5. **Multi-channel alerts** when a watcher fires: Discord (shared friend channel), Web Push (per-user opt-in), Email (per-user opt-in).
6. **Multi-category** with rich per-category filters (SSD form factor, NVMe Gen, RAM CAS/kit-config, GPU memory variant).
7. **Friends visit a public URL** with no signup; identity is opt-in via a memorable PIN they can write down to recover their alerts on a new device.

### Non-goals

- Public-scale (>50 concurrent users). Author + ~5 friends only.
- Account/billing/permissions infrastructure. Anonymous + opt-in PIN is enough.
- Cross-region (Amazon US only — Keepa `domain=1`).
- Mobile native app. Mobile web is a first-class target; native is not.
- Deal aggregation from non-Amazon sources (Newegg, Micro Center, etc.).

---

## 2. Architecture overview

```
                            ┌──────────────────────────────────────────────┐
                            │   Cloudflare D1   (sortsafe-db)              │
                            │   ─────────────                              │
                            │   users · pins · push_subs · email_subs      │
                            │   categories · products · offers · snapshots │
                            │   watchers · alert_rules · alert_deliveries  │
                            │   scrape_tasks · keepa_token_log             │
                            └────────────────────┬─────────────────────────┘
                                                 │
                            ┌────────────────────┴─────────────────────────┐
                            │   CF Worker  (sortsafe-api)    [single]      │
                            │   ──────────────────────                     │
                            │                                              │
                            │   READ  endpoints (public, cached at edge)   │
                            │     GET /v1/categories                       │
                            │     GET /v1/products?cat=gpu                 │
                            │     GET /v1/offers?cat=gpu&fresh=900         │
                            │     GET /v1/products/:asin/snapshots         │
                            │                                              │
                            │   WRITE endpoints (PIN-authed)               │
                            │     POST /v1/users (claim a new PIN)         │
                            │     POST /v1/sessions (recover by PIN)       │
                            │     POST /v1/alert-rules                     │
                            │     POST /v1/watchers                        │
                            │     POST /v1/push/subscribe                  │
                            │     POST /v1/email/subscribe                 │
                            │                                              │
                            │   DISTRIBUTED-SCRAPE endpoints (lightly authed)
                            │     POST /v1/scrape-tasks/claim?n=3          │
                            │     POST /v1/scrape-tasks/done               │
                            │                                              │
                            │   PROXY endpoint (rate-limited, host-allowlisted)
                            │     GET /proxy/amazon?u=...                  │
                            │                                              │
                            │   CRON HANDLER  (every 10 min)               │
                            │     • Keepa /product refresh (oldest-first)  │
                            │     • Keepa /search topup (rotation)         │
                            │     • Recompute deal scores                  │
                            │     • Enqueue alert_deliveries → CF Queue    │
                            │                                              │
                            │   QUEUE CONSUMER  (sortsafe-alerts)          │
                            │     • Drain alert_deliveries                 │
                            │     • Dispatch web push, Discord, email      │
                            │     • Mark delivered_at                      │
                            └──────────────────────┬───────────────────────┘
                                                   │
                            ┌──────────────────────┴───────────────────────┐
                            │   Cloudflare Pages  (sortsafe-web)           │
                            │   SvelteKit · adapter-static · SPA           │
                            │   ─────────────                              │
                            │   /                  landing card grid       │
                            │   /:cat              category browser        │
                            │   /watchers          custom-watcher CRUD     │
                            │   /me                pin · email · push      │
                            │                                              │
                            │   Service Worker:                            │
                            │   • IDB hydration from /v1/offers            │
                            │   • Web Push receiver                        │
                            │   • Opportunistic Amazon top-up (Phase 3)    │
                            └──────────────────────────────────────────────┘

                            R2 backups: nightly `wrangler d1 export → r2`
```

### Component boundaries (one Worker, route-dispatched)

The reviewer was right: three Workers is over-engineered for ~5 friends. Single `sortsafe-api` Worker handles:

| Surface | Routes | Auth | Edge cache |
|---|---|---|---|
| Read API | `/v1/categories`, `/v1/products*`, `/v1/offers*` | none (public) | 60-300s |
| Write API | `/v1/users`, `/v1/sessions`, `/v1/alert-rules`, `/v1/watchers`, `/v1/push/*`, `/v1/email/*` | PIN bearer in `Authorization` | none |
| Scrape coordination | `/v1/scrape-tasks/*` | shared HMAC token (in SPA + CLI) | none |
| CORS proxy | `/proxy/amazon` | shared HMAC token + host allowlist + per-IP rate limit | KV LRU 5-30 min |
| Cron | (no public route) | `--cron` event handler | n/a |
| Queue consumer | (no public route) | `--queue` event handler | n/a |

Static SPA lives in `sortsafe-web` Pages project; it talks to the Worker by path.

---

## 3. Data model

D1 (SQLite). Schema migrations live in `migrations/*.sql` and are run via `wrangler d1 migrations apply`.

```sql
-- ── users / auth ────────────────────────────────────────────────────────────

CREATE TABLE users (
  user_id     TEXT PRIMARY KEY,        -- ULID
  pin         TEXT NOT NULL UNIQUE,    -- "rusty-stellar-rocket-quasar" (4 words)
  pin_hash    TEXT NOT NULL,           -- argon2id(pin) — pin lookup checks hash
  created_at  INTEGER NOT NULL,        -- epoch ms
  last_seen   INTEGER NOT NULL,
  email       TEXT UNIQUE,             -- nullable; opt-in for cross-device + email alerts
  email_verified_at INTEGER
);
CREATE INDEX users_email_idx ON users(email);

CREATE TABLE pin_lookups (
  -- Rate-limit pin recovery. Per-IP counter; resets hourly.
  ip          TEXT NOT NULL,
  hour_bucket INTEGER NOT NULL,        -- floor(epoch_ms / 3_600_000)
  count       INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (ip, hour_bucket)
);

CREATE TABLE push_subs (
  sub_id      TEXT PRIMARY KEY,        -- ULID
  user_id     TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  endpoint    TEXT NOT NULL,           -- Web Push endpoint URL
  p256dh      TEXT NOT NULL,           -- subscription keys (base64url)
  auth        TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  user_agent  TEXT,
  UNIQUE(user_id, endpoint)
);

-- ── catalogue ───────────────────────────────────────────────────────────────

CREATE TABLE categories (
  category_id TEXT PRIMARY KEY,        -- 'gpu' | 'ram' | 'ssd'
  display     TEXT NOT NULL,
  search_terms TEXT NOT NULL,          -- JSON array of Keepa /search terms
  msrp_baseline TEXT NOT NULL,         -- JSON map model→USD (only meaningful for GPU)
  enabled     INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE products (
  asin            TEXT PRIMARY KEY,
  category_id     TEXT NOT NULL REFERENCES categories(category_id),
  model           TEXT,                -- '5090', 'DDR5-6000-32GB', '2TB-Gen4-2280' (slug)
  title           TEXT NOT NULL,
  brand           TEXT,
  thumbnail_url   TEXT,
  attrs_json      TEXT NOT NULL DEFAULT '{}',  -- per-category attribute bag
  first_seen      INTEGER NOT NULL,
  last_refreshed  INTEGER NOT NULL,
  active          INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX products_category_idx ON products(category_id);
CREATE INDEX products_model_idx ON products(model);

CREATE TABLE offers (
  offer_id        TEXT PRIMARY KEY,    -- "{asin}__{condition}__{seller_id|'none'}"
  asin            TEXT NOT NULL REFERENCES products(asin) ON DELETE CASCADE,
  condition       TEXT NOT NULL,       -- 'new' | 'used-like-new' | 'used-very-good' | 'used-good' | 'used-acceptable' | 'refurbished' | 'warehouse' | 'unknown'
  price_usd       REAL NOT NULL,       -- snapshot of latest seen price
  seller          TEXT,
  seller_id       TEXT,
  seller_rating   REAL,                -- 0-5
  seller_rating_count INTEGER,
  ships_from      TEXT,
  source          TEXT NOT NULL,       -- 'keepa' | 'browser-scrape' | 'cli-scrape'
  first_seen      INTEGER NOT NULL,
  last_seen       INTEGER NOT NULL,
  available       INTEGER NOT NULL DEFAULT 1  -- soft delete (vs DELETE so we keep history)
);
CREATE INDEX offers_asin_idx ON offers(asin);
CREATE INDEX offers_last_seen_idx ON offers(last_seen);
CREATE INDEX offers_avail_idx ON offers(available, last_seen);

CREATE TABLE snapshots (
  snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  asin            TEXT NOT NULL REFERENCES products(asin) ON DELETE CASCADE,
  condition       TEXT NOT NULL,
  price_usd       REAL NOT NULL,
  taken_at        INTEGER NOT NULL,
  source          TEXT NOT NULL
);
CREATE INDEX snapshots_asin_taken_idx ON snapshots(asin, taken_at);
-- TTL: cron deletes WHERE taken_at < (now - 90 days). Keeps storage bounded.

CREATE TABLE keepa_token_log (
  -- Surfaces our token budget so /me can show "180/600 left this hour"
  -- and so we can stop aggressive cron runs when tokens are depleted.
  ts          INTEGER NOT NULL,        -- epoch ms
  endpoint    TEXT NOT NULL,           -- '/product' | '/search' | '/token'
  cost        INTEGER NOT NULL,
  remaining   INTEGER NOT NULL,
  PRIMARY KEY (ts)
);

-- ── alerts ──────────────────────────────────────────────────────────────────

CREATE TABLE alert_rules (
  rule_id     TEXT PRIMARY KEY,        -- ULID
  user_id     TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  scope       TEXT NOT NULL,           -- 'category' | 'asin' | 'watcher'
  scope_value TEXT NOT NULL,           -- 'gpu', or 'B0XXXXXXX', or watcher_id
  metric      TEXT NOT NULL,           -- 'price_floor' | 'pct_below_median' | 'pct_off_msrp' | 'composite_score'
  threshold   REAL NOT NULL,           -- semantic per metric (e.g. 1500 for floor; 15 for pct)
  conditions  TEXT NOT NULL DEFAULT '[]',  -- JSON: ["used-good","refurbished","warehouse"]
  channels    TEXT NOT NULL DEFAULT '["push"]', -- JSON: ["push","discord","email"]
  cooldown_s  INTEGER NOT NULL DEFAULT 1800,    -- prevent spam: same rule can fire only every X seconds
  active      INTEGER NOT NULL DEFAULT 1,
  created_at  INTEGER NOT NULL,
  last_fired_at INTEGER
);
CREATE INDEX alert_rules_user_idx ON alert_rules(user_id);
CREATE INDEX alert_rules_scope_idx ON alert_rules(scope, scope_value, active);

CREATE TABLE alert_deliveries (
  delivery_id   TEXT PRIMARY KEY,      -- ULID
  rule_id       TEXT NOT NULL REFERENCES alert_rules(rule_id) ON DELETE CASCADE,
  offer_id      TEXT NOT NULL,
  channel       TEXT NOT NULL,         -- 'push' | 'discord' | 'email'
  enqueued_at   INTEGER NOT NULL,
  delivered_at  INTEGER,
  error         TEXT,
  payload_hash  TEXT NOT NULL          -- dedupe: skip if (rule_id, payload_hash) seen in last cooldown
);
CREATE INDEX alert_deliveries_pending_idx ON alert_deliveries(delivered_at) WHERE delivered_at IS NULL;
CREATE INDEX alert_deliveries_dedupe_idx ON alert_deliveries(rule_id, payload_hash, enqueued_at);

-- Single shared Discord webhook config (no per-user channels in v1).
CREATE TABLE discord_config (
  id          INTEGER PRIMARY KEY CHECK(id = 1),
  webhook_url TEXT NOT NULL,
  enabled     INTEGER NOT NULL DEFAULT 1,
  updated_at  INTEGER NOT NULL
);

-- ── custom watchers ────────────────────────────────────────────────────────

CREATE TABLE watchers (
  watcher_id  TEXT PRIMARY KEY,        -- ULID
  user_id     TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  name        TEXT NOT NULL,           -- "DDR5 32GB 6000 CL30"
  search_q    TEXT NOT NULL,           -- Keepa /search term
  category_id TEXT NOT NULL REFERENCES categories(category_id),
  attr_filters TEXT NOT NULL DEFAULT '{}',  -- JSON: {"speed_mhz":{">=":6000},"cas":{"<=":30}}
  conditions  TEXT NOT NULL DEFAULT '["new","used-good","refurbished","warehouse"]',
  active      INTEGER NOT NULL DEFAULT 1,
  created_at  INTEGER NOT NULL,
  last_polled INTEGER
);
CREATE INDEX watchers_user_idx ON watchers(user_id);
CREATE INDEX watchers_active_idx ON watchers(active, last_polled);

-- ── distributed scrape coordination ────────────────────────────────────────

CREATE TABLE scrape_tasks (
  task_id     TEXT PRIMARY KEY,        -- ULID
  asin        TEXT NOT NULL,
  task_type   TEXT NOT NULL,           -- 'pdp' | 'aod' (per-seller offer listing)
  priority    INTEGER NOT NULL DEFAULT 100,  -- lower = higher priority
  claimed_until INTEGER,                 -- epoch ms; NULL = unclaimed
  claimed_by  TEXT,                    -- IP hash or user_id
  attempts    INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  created_at  INTEGER NOT NULL,
  completed_at INTEGER
);
CREATE INDEX scrape_tasks_avail_idx ON scrape_tasks(claimed_until, completed_at, priority);
```

### Storage projections

- 50 ASINs × 4 conditions × 1 snapshot/10min = ~28,800 snapshots/day → 90-day window = ~2.6M rows. At ~80 bytes/row = ~210 MB. D1 free tier is 5 GB. **Comfortable.**
- Cron writes per day: ~8K (snapshots + offer upserts). Free tier is 100K writes/day. **<10%.**
- Reads: 5 friends × 20 visits × 200 offers = 20K/day. Free tier is 5M. **<1%.**

---

## 4. Pipelines

### 4.1 Keepa cron (every 10 min, single Worker scheduled handler)

```
1. SELECT user tokens remaining via /token. If <30, log + skip non-essential phases.
2. ENRICH PHASE
   2a. SELECT asin FROM products WHERE active=1 AND last_refreshed < now-15min
       ORDER BY last_refreshed ASC LIMIT N
       (N adaptive: tokensRemaining - SEARCH_RESERVE - SAFETY_MARGIN)
   2b. POST /product?asin=A,B,C&stats=180
       For each returned product:
         - inferModelFromTitle() + extract per-category attrs
         - upsert into products
         - for each (condition,price>0): upsert offer (mark available=1)
         - any (asin,condition) tuple in DB but missing from this response → mark available=0
         - insert snapshot (asin, condition, price, now, 'keepa')
   2c. Log token cost in keepa_token_log.
3. DISCOVERY PHASE (rotated by category, smallest pool first)
   3a. For category with min(product_count) below floor:
       POST /search?term=<category_search_term>&stats=180
   3b. Insert any new ASIN into products. (Each search ~5-50 tokens depending on hits.)
   3c. Log token cost.
4. SCORE RECOMPUTE
   For every (asin,condition) where last_seen changed in this run:
     - Compute current_price (lowest available offer for that asin+condition)
     - Compute median_30d, median_90d from snapshots (SQLite percentile fallback if needed)
     - Compute deal_score per metric (price_floor needs no median; pct_below_median needs both)
     - Cache into a `current_view` materialised table OR compute on read.
       (Decision: materialise — read endpoints stay fast, recompute only when needed.)
5. ALERT EVALUATION
   SELECT alert_rules WHERE active=1 AND last_fired_at < now-cooldown
   For each rule, check: does the new current_view satisfy threshold?
     If yes: insert alert_deliveries rows (one per channel × per offer)
       payload_hash = SHA256(rule_id|asin|condition|threshold_bucket)
       Skip if (rule_id, payload_hash) seen in alert_deliveries within cooldown
   Enqueue to CF Queue. Mark rule.last_fired_at.
6. SNAPSHOT TTL
   DELETE FROM snapshots WHERE taken_at < now - 90d
   (Run once/day via separate cron at 03:00 UTC, not every 10 min.)
```

**Adaptive batching.** With Power Plan 600 tok/hr, 6 cron runs/hr max, that's 100 tokens/run budget. Reserve 50 for one /search per run, leaves 50 for /product enrichment of ~50 ASINs/run. With ~150 ASINs total across categories at steady state, each ASIN refreshes every ~30 min (3 cron cycles) — better than the current 30-min cron's per-ASIN rate.

### 4.2 Bot-wall detection (parser hardening)

Every Amazon HTML response — whether through `/proxy/amazon` or directly fetched — passes through a single `detectBotWall(html: string): 'ok' | 'captcha' | 'continue-shopping' | 'sorry-page' | 'empty'` function. Detection signals:

- `<title>Sorry! Something went wrong!</title>` (full-page error)
- `Type the characters you see in this image` (captcha challenge)
- `/errors/validateCaptcha` in HTML (captcha endpoint reference)
- `click the button below to continue shopping` (interstitial)
- `<title>Robot Check</title>` or `Robot or human?`
- Body length < 5000 bytes (sentinel for any of the above we missed)
- Missing `#productTitle` AND missing `[data-component-type="s-search-result"]`

If detected, the parser **never** writes to D1 and instead increments an Analytics Engine event with the detected reason. Returns null up the call chain. Discovery and scrape tasks log the detection too (so we know *which* ASIN triggered).

### 4.3 Alert delivery (Cloudflare Queue consumer)

A `sortsafe-alerts` queue. Cron enqueues `alert_delivery` messages. Consumer processes one delivery at a time:

```
1. Lookup delivery + rule + offer + product
2. Build payload:
     - Discord: embed with thumbnail, title, price, condition, % below median, link
     - Push: title=`{model} {condition} ${price}`, body=`{X% below 30d median, was $Y}`, click_url=`/{cat}#asin={asin}`
     - Email: HTML template with same fields + unsub link
3. Channel-specific dispatch
   Discord: POST to discord_config.webhook_url (single shared)
   Push: foreach push_sub for user, web-push (VAPID) — handle 410 by deleting sub
   Email: Resend API; templated HTML; include unsub one-click header
4. Mark delivered_at = now (or error message if failed)
5. On Resend / web-push hard failure (4xx, except 408/429), no retry.
   On 5xx / network: increment attempts, re-enqueue if attempts < 5.
```

Free tier Queues: 1M operations/month. At 5 friends × 5 alerts/day × 3 channels = 75 deliveries/day = 27K/year. **<3% of monthly cap.**

### 4.4 Distributed browser scrape (Phase 3 — opportunistic)

Goal: catch deals between Keepa cron cycles by leveraging visitor browsers' IPs to fetch fresh PDPs. Activate ONLY when:

- A visitor has the page focused (visibilityState === 'visible')
- An ASIN's last refresh was >30 min ago
- The visitor has explicitly opened that card (intent signal — not background)

Coordination: client calls `POST /v1/scrape-tasks/claim?n=1`. Worker SELECTs one task atomically:

```sql
UPDATE scrape_tasks
SET claimed_until = unixepoch('now', '+90 seconds') * 1000,
    claimed_by = ?,
    attempts = attempts + 1
WHERE task_id = (
  SELECT task_id FROM scrape_tasks
  WHERE completed_at IS NULL
    AND attempts < max_attempts
    AND (claimed_until IS NULL OR claimed_until < unixepoch('now') * 1000)
  ORDER BY priority ASC, created_at ASC
  LIMIT 1
)
RETURNING task_id, asin, task_type;
```

Client fetches via `/proxy/amazon`, runs through `detectBotWall`. If bot-walled, POSTs `{task_id, status: 'bot-walled', detected_reason}` — server clears claim, increments attempts, escalates priority. If clean, POSTs parsed offers; server upserts and marks complete.

**Throttle:** at most 1 PDP fetch per visit, max 1 every 5 minutes per visitor. Real browser load profile.

**Consent:** the SPA shows a one-time toggle in `/me` ("Help refresh prices when you visit (uses your browser to fetch from Amazon)") **defaulted off**. Reviewer was right — silent conscription is not OK.

CLI fallback: `bunx sortsafe-scrape` does the same thing headlessly with no rate limit (intent: friend's home server). Same `/v1/scrape-tasks/*` endpoints.

---

## 5. Per-category attribute schemas

Per-category extraction is a layered pipeline:

1. **Keepa structured fields** — `categoryTree`, `features`, `brand`, `model`, `productGroup`. Use these first.
2. **Title regex** — fallback for fields Keepa doesn't structure.
3. **Manual curation** — for top SKUs (especially RAM kits, top SSD models), a JSON file overrides extracted attrs.
4. **Unknown bucket** — if a field can't be extracted, it's `null`. The UI surfaces "Unknown" as a filter option (don't drop the listing).

### 5.1 GPU

```typescript
interface GpuAttrs {
  model: '3090' | '4090' | '5090';      // already extracted via inferModelFromTitle
  memory_gb: number | null;             // 24, 32 — sometimes 12/16 for cut-down variants
  variant: string | null;               // 'Founders Edition', 'AORUS Master', 'TUF OC', etc.
  tier: 'reference' | 'oc' | 'premium' | null;  // heuristic from variant keywords
  power_w: number | null;               // TGP if present
}
```

Filters: model, memory_gb, brand, tier.

### 5.2 RAM

```typescript
interface RamAttrs {
  generation: 'DDR4' | 'DDR5' | null;
  capacity_gb: number | null;           // total kit capacity
  kit_config: string | null;            // '2x16', '4x16', '1x32'
  speed_mhz: number | null;             // 6000, 7200
  cas_latency: number | null;           // CL30, CL36
  ecc: boolean | null;
  rgb: boolean | null;
}
```

Filters: generation, capacity, speed range, CL range, kit config, brand, RGB on/off, ECC on/off.

Deal metric: $/GB (especially valuable for RAM where capacities vary a lot).

### 5.3 SSD

```typescript
interface SsdAttrs {
  form_factor: 'M.2 2280' | 'M.2 2242' | 'M.2 22110' | '2.5"' | 'mSATA' | 'U.2' | 'AIC' | null;
  interface: 'NVMe Gen3' | 'NVMe Gen4' | 'NVMe Gen5' | 'SATA' | null;
  capacity_gb: number | null;           // 500, 1000, 2000, 4000, 8000
  has_dram: boolean | null;             // DRAM-less is a meaningful "avoid" signal
  seq_read_mbs: number | null;
  seq_write_mbs: number | null;
  endurance_tbw: number | null;
}
```

Filters: form factor, interface, capacity, DRAM-y/n, brand, sequential perf range, TBW.

Deal metric: $/TB (and also $/TBW for endurance-conscious).

### 5.4 Extraction reliability

The reviewer flagged this as a real risk. Mitigation:

1. Per-category extractor runs against a held-out fixture set in tests (`tests/extractors/{gpu,ram,ssd}.test.ts`) with ≥30 real Amazon titles each. Each test asserts specific attrs extracted. Failing test = extractor regression.
2. Admin endpoint `GET /v1/products/:asin/extractor-debug` returns the raw Keepa fields, regex matches, and final attrs — for debugging individual SKUs without log-diving.
3. UI shows "Unknown" filter chip with count — high counts mean the extractor needs work, not that the listings should be hidden.

---

## 6. Deal scoring

Each `(asin, condition)` pair has a **current_view** row computed at cron time:

```typescript
interface CurrentView {
  asin: string;
  condition: GpuCondition;
  current_price_usd: number;
  median_30d: number | null;
  median_90d: number | null;
  msrp_baseline: number | null;
  // Derived metrics — all stored to avoid recomputation on every read.
  pct_below_median_30d: number | null;
  pct_below_median_90d: number | null;
  pct_off_msrp: number | null;
  composite_score: number;
  // Per-unit metrics for non-GPU
  price_per_gb: number | null;        // RAM
  price_per_tb: number | null;        // SSD
  is_lowest_30d: boolean;
  is_lowest_90d: boolean;
  recomputed_at: number;
}
```

### Composite score formula (default)

```
composite_score = (
    0.50 * normalize(pct_below_median_30d)        // -100..+100 → 0..100
  + 0.20 * normalize(pct_below_median_90d)
  + 0.15 * seller_rating_norm                     // 0-5 → 0-100
  + 0.10 * recency_bonus                          // 100 if last_seen <30min, decays
  + 0.05 * is_lowest_30d_bonus                    // 100 if true, else 0
)
```

Score is 0-100. Default sort uses this. UI lets users switch sort/scoring metric per session.

Alert rules can use any of: `price_floor`, `pct_below_median_30d`, `pct_below_median_90d`, `pct_off_msrp`, `composite_score`. Threshold semantic varies per metric (price_floor is `≤ threshold`, percentage metrics are `≥ threshold`, composite is `≥ threshold`).

### Median computation (D1 SQLite)

SQLite doesn't have `PERCENTILE_CONT`. Use the trick:

```sql
WITH ordered AS (
  SELECT price_usd FROM snapshots
  WHERE asin=? AND condition=? AND taken_at > unixepoch('now', '-30 days') * 1000
  ORDER BY price_usd
), counted AS (SELECT COUNT(*) AS n FROM ordered)
SELECT AVG(price_usd) FROM ordered
LIMIT 2 - (SELECT n FROM counted) % 2
OFFSET (SELECT (n - 1) / 2 FROM counted);
```

Acceptable performance for 90-day windows (max ~13K rows per (asin,condition)). If too slow at scale, materialise into `current_view` only.

---

## 7. Auth & sharing model

### 7.1 PIN generation

On first visit, the SPA calls `POST /v1/users` with no body. Worker generates:

- `user_id`: ULID
- `pin`: 4 words from a 256-word dictionary, joined by `-`. Themes: robots, space, AI. Examples: `rusty-stellar-rocket-quasar`, `chrome-pulsar-galactic-mind`, `binary-nebula-sentient-android`.
  - 4 words × 8 bits/word = 32 bits ≈ 4.3B combinations. Adequate for 5 friends.
- `pin_hash`: argon2id(pin)
- Stores user, returns `{user_id, pin, session_token}`. SPA persists `(user_id, pin, session_token)` in localStorage.

### 7.2 Recovery

User on a new device types their PIN. SPA calls `POST /v1/sessions {pin}`:

1. Worker checks `pin_lookups` for IP rate-limit: deny if >5 attempts/hour (return 429).
2. Argon2-verify pin against `users.pin_hash` (slow on purpose).
3. If valid: create session_token, return `{user_id, session_token}`.
4. SPA persists.

### 7.3 Session token

- Stateless JWT signed with Worker secret. Body: `{user_id, exp: now + 30d}`.
- Bearer auth on all write endpoints.
- Short-lived (30d) so a stolen device gradually loses access.

### 7.4 Email opt-in

`/me` page shows "Tie this PIN to an email for cross-device recovery + email alerts." On submit:

1. POST `/v1/email/subscribe {email}` — Worker sends magic link via Resend.
2. User clicks link → `/v1/email/verify?token=...` → marks `email_verified_at`.
3. Email-recovery flow: enter email at `/`, get magic link, click → bound to existing user_id.

Email alerts as a channel: opt-in per `alert_rule` (each rule's `channels` array can include `'email'`).

### 7.5 Discord — single shared channel

Bootstrap: author sets the webhook URL via `wrangler d1 execute --command "INSERT INTO discord_config..."`. No per-user Discord config in v1.

Any alert with `'discord' in channels` posts to the shared webhook. Friends opt **out** if they don't want Discord pings (default opt-in).

### 7.6 Threat model

For 5 friends, no PII, no money:

- **PIN guessing**: rate-limited at 5/hour/IP. 32-bit space → 13 years to brute-force at limit. Adequate.
- **Discord webhook leak**: webhook URL is in D1 only, never in client code. If leaked: rotate via `wrangler d1`.
- **CORS proxy abuse**: host-allowlist (`amazon.com`, `*.amazon.com` only) + per-IP rate limit (30 req/min) + HMAC token. Not bulletproof but stops drive-by abuse.
- **Browser-side scrape consent**: opt-in toggle, off by default.

What we explicitly do not protect against:

- A friend with the URL sharing it with strangers — unbounded read access to deal data is fine.
- A friend with another friend's PIN setting/clearing their alerts — social cost, not technical.

---

## 8. UI design

### 8.1 Routes

| Route | Purpose |
|---|---|
| `/` | Card grid of categories. Hover/tap → `/{cat}`. Footer: link to `/me`. |
| `/{cat}` (e.g. `/gpu`, `/ram`, `/ssd`) | Category browser. Filter sidebar (per-category attrs), sort selector, deal grid. URL params reflect filter state for shareable URLs. |
| `/{cat}/{asin}` | Deep-link to a single product card with snapshot sparkline. |
| `/watchers` | List of user's watchers, CRUD. |
| `/watchers/new` | Watcher creation: category → search term → attr filters → conditions → alert rule. |
| `/me` | PIN display (with copy-to-clipboard), email opt-in, push toggle, browser-scrape consent toggle, Discord opt-out, Keepa token gauge. |
| `/login` | Recovery: enter PIN or email → magic link. |

### 8.2 Category page filter UX

Sticky top bar across all categories:
- Sort: Best Deal | Lowest Price | Lowest $/GB or $/TB | Recently Seen | Lowest in 30 Days
- Quick chips: "Hide new" "Used+Refurb only" "Has Warehouse" "Below median" "Below MSRP"
- Search box (filters by title contains)

Per-category sidebar (left on desktop, drawer on mobile):
- Attribute filters from §5
- Each filter shows count of matching products
- Range sliders for price, capacity, perf metrics
- Multi-select for brands

Deal cards:
- Thumbnail (Amazon `m.media-amazon.com` URL — no proxying needed)
- Title (truncated 2 lines, full on hover)
- Price + condition badge
- Composite score colored bar (green → red gradient)
- "X% below 30d median ($Y)" if data available
- Seller name + rating chip if available
- Sparkline: last 30 days of snapshots for this asin+condition
- Click → `/{cat}/{asin}` for full detail
- "Set alert" button opens alert rule modal scoped to this asin

### 8.3 Watcher creation flow

Three steps in a wizard:

1. **What are you watching?** Pick category. Type search query (with autocomplete from existing products). Optionally type a friendly name.
2. **Filters.** Pick attribute filters (per category). Conditions multiselect.
3. **Alert.** Metric (default: "% below 30d median"). Threshold (slider with sensible default). Channels (push/discord/email). Cooldown (default 30 min). Enable.

Save → redirects to `/watchers` with the new watcher visible.

### 8.4 Mobile-first

All filter/sort UI works in a bottom sheet on mobile. No horizontal scroll. Cards stack to 1 column < 600px.

---

## 9. Deployment plan

### 9.1 Cloudflare resources

| Resource | Purpose |
|---|---|
| `sortsafe-api` Worker | Single Worker, all backend logic |
| `sortsafe-db` D1 | Schema in §3 |
| `sortsafe-cache` KV | Proxy LRU cache + edge response cache for read endpoints |
| `sortsafe-alerts` Queue | Alert delivery dispatch |
| `sortsafe-backups` R2 | Nightly D1 dumps |
| `sortsafe-web` Pages | SvelteKit static build |

`wrangler.toml` snippet:

```toml
name = "sortsafe-api"
main = "src/worker.ts"
compatibility_date = "2026-05-01"

[[d1_databases]]
binding = "DB"
database_name = "sortsafe-db"
database_id = "..."

[[kv_namespaces]]
binding = "CACHE"
id = "..."

[[queues.producers]]
binding = "ALERTS"
queue = "sortsafe-alerts"

[[queues.consumers]]
queue = "sortsafe-alerts"
max_batch_size = 10

[[r2_buckets]]
binding = "BACKUPS"
bucket_name = "sortsafe-backups"

[triggers]
crons = ["*/10 * * * *", "0 3 * * *"]
# 10-min: enrich + score + alert eval
# 03:00 UTC daily: snapshot TTL prune + R2 backup
```

### 9.2 Dev workflow on Termux

- Local D1: `wrangler d1 create --local`. Migrations apply locally before deploy.
- Local Worker: `wrangler dev` — needs `bunx` wrapper (we already have this pattern).
- SPA dev: `bun run dev` (existing Vite setup).
- SPA points to local Worker via `?api=http://localhost:8787`.
- Secrets via `wrangler secret put`: `KEEPA_API_KEY`, `RESEND_API_KEY`, `VAPID_PUBLIC`, `VAPID_PRIVATE`, `SCRAPE_HMAC`, `DISCORD_WEBHOOK_URL` (initial seed; later moved into D1).

### 9.3 Production rollout

1. Provision CF resources (one-time via wrangler).
2. Apply schema migrations: `wrangler d1 migrations apply sortsafe-db --remote`.
3. Seed `categories` and `discord_config` rows.
4. Deploy Worker: `wrangler deploy`.
5. Deploy Pages: `wrangler pages deploy ./build`.
6. Smoke test: visit deployed URL, verify SPA hydrates from `/v1/offers`, verify cron logs in Worker dashboard.
7. Update Termux crontab to remove the local `gpus-keepa-cron.sh` entry (redundant once cloud cron runs).
8. Monitor for 48h via Workers Analytics Engine + Discord alerts before announcing to friends.

### 9.4 Backup / disaster recovery

Daily 03:00 UTC cron runs:

```bash
wrangler d1 export sortsafe-db --remote --output /tmp/backup.sql
# (Done from inside the Worker via fetch to D1 export API; piped to R2 via PUT)
```

7-day retention in R2. Restore: `wrangler d1 execute --file backup.sql`.

---

## 10. Phasing

### Phase 1 — "lift cron + add alerts" (target: 1 weekend)

- D1 schema (full schema upfront — cheap to migrate)
- Worker with: cron handler, /v1/offers GET, /v1/products GET, /proxy/amazon GET, queue consumer
- Discord webhook dispatch (single shared channel, alerts on % below 30d median ≥ 15 for any used/refurb/warehouse offer on tracked SKUs — hardcoded rule, no per-user UI yet)
- Pages deployment of existing /gpus SPA, hydrating from /v1/offers instead of static seed
- Bot-wall detector at every parse boundary
- R2 backup cron
- Workers Analytics Engine for token cost + alert dispatch + bot-wall detection events
- Migration: cut over Termux cron → cloud cron, archive `static/gpus-seed.json`

**Success criteria:** site is reachable from a friend's phone; Discord channel gets a message when 5090 used drops 15%+ below 30d median; cron has run for 24h without error.

### Phase 2 — "personalization + RAM/SSD" (target: 1 weekend)

- PIN auth (4-word generation, argon2id hash, recovery flow)
- Web Push: VAPID setup, opt-in flow, push consumer in queue
- `/me` page (PIN display, push toggle, opt-out toggles, Keepa token gauge)
- Add RAM and SSD categories with extractors + filter UIs
- `/{cat}` route is a parametric page now, not /gpu hardcoded
- Per-category attribute filter sidebar
- Composite score formula + sparkline component

**Success criteria:** friends each have their own PIN, can opt into push, see RAM/SSD pages, and at least one friend has set up a per-asin alert that fires.

### Phase 3 — "watchers + browser scrape" (target: 1 weekend)

- Custom watchers CRUD UI + watchers cron path (every 10 min check each watcher's search results)
- Email opt-in + Resend integration + verification
- Distributed browser scrape: scrape_tasks table + claim/done endpoints + Service Worker logic + opt-in toggle in /me
- `bunx sortsafe-scrape` CLI

**Success criteria:** at least one friend has a custom watcher (e.g. "DDR5 32GB 6000 CL30 below $90") that has fired an alert; browser-scrape opt-in is visible and at least one friend has enabled it; CLI works on the author's machine.

### Phase 4+ (future, not committed)

- Per-user Discord channels
- Watcher templates ("Best mid-range 4090", "Cheapest 4TB Gen4 NVMe")
- Cross-category bundles ("PC build snapshot")
- Historical trend page per ASIN with full annotated chart
- Web Push action buttons ("Snooze 1h", "Disable rule")

---

## 11. Open questions / risks

| # | Risk | Mitigation | Owner |
|---|---|---|---|
| 1 | Browser-side scrape silently ingests bot-wall HTML | `detectBotWall()` at every parse boundary, Analytics event on detection, fail closed (no DB write) | Phase 1 |
| 2 | Keepa structured fields don't extract enough RAM/SSD attrs | Test fixture suite per category (≥30 titles), "Unknown" filter bucket, manual curation override file for top SKUs | Phase 2 |
| 3 | Discovery rotation never finds genuinely new SKUs (Keepa /search returns same top 20) | Phase 4 candidate: subreddit RSS (r/buildapcsales, r/hardwareswap) → ASIN extraction → seed | future |
| 4 | D1 row-level locking under cron + queue + writes | Coalesce writes inside cron handler into batched transactions (`db.batch`); queue consumer processes one at a time | Phase 1 |
| 5 | Alert spam (single deal triggers 5 friends × 3 channels = 15 messages) | Cooldown per rule + payload_hash dedupe + sensible defaults (cooldown 30 min) | Phase 1 |
| 6 | Browser scrape opt-in is poorly understood by friends | One-time modal with clear copy: "Your browser will fetch up to 1 Amazon page per visit to help refresh prices. You can turn this off anytime." | Phase 3 |
| 7 | PIN dictionary clusters on common words → guessability | 256-word curated dictionary, no obscenity, no two adjacent words from same theme | Phase 2 |
| 8 | Wrangler local dev on Termux may have issues we haven't seen | Spike during Phase 1; fallback to deploying every change to a `sortsafe-api-dev` Worker | Phase 1 |

---

## 12. Migration from current state

Current state lives at `~/git/sortsafe`:

- `static/gpus-seed.json` is the source of truth for the SPA.
- `scripts/gpus-seed-keepa.ts` is the Keepa pipeline.
- Local cron at `*/30 * * * *` keeps the seed fresh.

Migration steps:

1. **Phase 1 deploy completes** (Worker + D1 + Pages live).
2. One-time backfill script: read `static/gpus-seed.json`, POST each product/offer/snapshot into D1 (via a temporary `/v1/admin/import` endpoint guarded by HMAC).
3. Update SPA `hydrateFromSeed()` → `hydrateFromApi()` calling `/v1/offers`.
4. Verify SPA on production for a day with both Termux cron AND cloud cron running (data should be identical).
5. Disable Termux cron (`crontab -e`, comment line).
6. Archive `static/gpus-seed.json` (keep in repo for reference; delete after Phase 2 ships).

No data loss — snapshots are preserved on either side; offers reconcile by `offer_id`.

---

## 13. Out of scope (intentionally)

- Server-side rendering of category pages (SPA + edge cache is enough)
- Multi-region (Amazon US only)
- Webhooks for non-Discord chat platforms (Slack, Telegram, etc.) — easy to add later if asked
- Native mobile apps
- Public discovery / SEO (no sitemap, no Open Graph cards beyond defaults)
- Affiliate links / monetisation
- Rate limiting based on user identity (only IP-based for now)

---

*End of design.*
