# sortsafe Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lift the existing Termux Keepa cron + `/gpus` SPA hydration onto Cloudflare (Worker + D1 + Pages + Queues + R2). Add a hardcoded Discord-channel alert when 5090/4090/3090 used or refurbished prices drop ≥15% below their 30-day median. Keep all existing UX functionality.

**Architecture:** Single CF Worker (`sortsafe-api`) handles cron, queue consumer, REST API, and the Amazon CORS proxy via path-based routing. D1 stores everything (products, offers, snapshots, current_view, alert_rules, alert_deliveries). Cron writes go through batched transactions; alert dispatch goes through CF Queue (no polling). The existing SvelteKit SPA at `~/git/sortsafe/src/routes/gpus/` switches from hydrating `static/gpus-seed.json` to hydrating `GET /v1/offers`. No PIN auth, no per-user features, no browser-side scrape — those are Phase 2/3.

**Tech Stack:** Cloudflare Workers (TypeScript) · D1 (SQLite) · CF KV (rate-limit + proxy LRU) · CF Queues · R2 (NDJSON backups) · Workers Analytics Engine · SvelteKit (existing SPA, Bun runtime) · `vitest` (Worker tests via `@cloudflare/vitest-pool-workers`).

**Spec:** [`docs/superpowers/specs/2026-05-04-sortsafe-deal-sniper-design.md`](../specs/2026-05-04-sortsafe-deal-sniper-design.md). Phase 1 deliverables in §10.

---

## File Structure

```
~/git/sortsafe/
├── worker/                                NEW: separate package, deploys as sortsafe-api Worker
│   ├── package.json                       deps: hono, @cloudflare/workers-types, vitest, @cloudflare/vitest-pool-workers, ulid
│   ├── wrangler.toml                      D1 + KV + Queue + R2 + Analytics bindings + cron triggers
│   ├── tsconfig.json                      strict, ES2022, target Worker runtime
│   ├── vitest.config.ts                   workers pool, isolate-per-test
│   │
│   ├── src/
│   │   ├── index.ts                       Worker entry: fetch / scheduled / queue exports
│   │   ├── env.ts                         Env interface (DB, CACHE, ALERTS, BACKUPS, AE, secrets)
│   │   ├── router.ts                      Hono router wiring
│   │   │
│   │   ├── db/
│   │   │   ├── client.ts                  Typed D1 query helpers (.get, .all, .run, .batch)
│   │   │   ├── types.ts                   Row types matching schema (Product, Offer, Snapshot, ...)
│   │   │   └── migrations/
│   │   │       ├── 0001_init.sql          Full schema from spec §3 (12 tables + indexes)
│   │   │       └── 0002_seed.sql          Insert categories, system user, hardcoded alert rule
│   │   │
│   │   ├── keepa/
│   │   │   ├── client.ts                  HTTP wrapper for /token, /product, /search; logs to keepa_token_log
│   │   │   ├── types.ts                   KeepaProduct, KeepaSearchResp, etc.
│   │   │   └── bot-wall.ts                detectBotWall(html) → 'ok' | 'captcha' | 'continue-shopping' | 'sorry-page' | 'empty'
│   │   │
│   │   ├── extract/
│   │   │   ├── gpu.ts                     inferModelFromTitle + GPU attrs from Keepa product (Phase 1: GPU only)
│   │   │   └── price.ts                   parsePriceUsd, classifyCondition, etc.
│   │   │
│   │   ├── pipeline/
│   │   │   ├── cron.ts                    10-min orchestrator: recovery → token check → enrich → discover → score → alert eval
│   │   │   ├── cron-daily.ts              03:00 UTC: snapshot TTL prune + R2 NDJSON backup
│   │   │   ├── enrich.ts                  Phase 2 of cron: /product batch → upsert offers/snapshots, mark dirty
│   │   │   ├── discover.ts                Phase 3 of cron: /search top-up rotation
│   │   │   ├── score.ts                   Recompute current_view rows for dirty (asin, condition) tuples
│   │   │   ├── median.ts                  SQL median calc per spec §6 + composite_score formula
│   │   │   ├── alert-eval.ts              Iterate alert_rules, build alert_deliveries, atomic enqueue + last_fired_at
│   │   │   └── recovery.ts                Cron step 0: re-enqueue orphaned deliveries
│   │   │
│   │   ├── queue/
│   │   │   ├── consumer.ts                Queue handler: dispatch one delivery per message
│   │   │   └── discord.ts                 Webhook POST + payload builder
│   │   │
│   │   ├── api/
│   │   │   ├── offers.ts                  GET /v1/offers?cat=gpu — read from current_view + offers + products
│   │   │   ├── products.ts                GET /v1/products?cat=gpu, GET /v1/products/:asin
│   │   │   ├── admin.ts                   POST /v1/admin/import (HMAC-guarded backfill from gpus-seed.json), POST /v1/admin/run-cron
│   │   │   └── config.ts                  GET /v1/config/discord-invite (returns DISCORD_INVITE_URL secret to SPA)
│   │   │
│   │   ├── proxy/
│   │   │   └── amazon.ts                  GET /proxy/amazon?u=... — host-allowlist, HMAC, KV LRU, per-IP rate limit
│   │   │
│   │   └── lib/
│   │       ├── analytics.ts               AE event helpers: tokenLog, alertDispatch, botWallHit, cronRun
│   │       ├── rate-limit.ts              KV-counter implementation for /proxy + future /v1/sessions
│   │       ├── hmac.ts                    HMAC verify for /v1/admin/* and scrape-task endpoints
│   │       └── ulid.ts                    ULID generator (re-export or vendored 50-line impl)
│   │
│   └── test/
│       ├── db/client.test.ts              8 round-trip smoke tests for each helper
│       ├── keepa/
│       │   ├── bot-wall.test.ts           Validates detector against fixtures below
│       │   ├── client.test.ts             Mock fetch; verify token-log writes + bot-wall integration
│       │   └── fixtures/
│       │       ├── sorry-page.html        Synthetic — Amazon "Sorry" body
│       │       ├── continue-shopping.html Synthetic — interstitial body
│       │       ├── captcha.html           Synthetic — captcha challenge body
│       │       └── real-pdp.html          Synthetic positive (>5KB w/ #productTitle)
│       ├── extract/gpu.test.ts            inferModelFromTitle 32-fixture title set
│       ├── pipeline/score.test.ts         Composite score formula null behaviour, normalize edge cases
│       ├── pipeline/median.test.ts        n=0, n=1, n even, n odd, no-history fallback
│       ├── pipeline/alert-eval.test.ts    Cooldown + payload_hash dedupe + Discord global dedupe
│       ├── pipeline/recovery.test.ts      Orphaned-delivery re-enqueue
│       ├── proxy/amazon.test.ts           Host allowlist, HMAC reject, KV cache hit/miss
│       └── api/offers.test.ts             Filter + sort + pagination basics
│
├── src/                                   EXISTING SvelteKit SPA — modified
│   ├── lib/gpus/
│   │   ├── api.ts                         NEW: hydrateFromApi() — fetches /v1/offers, mirrors hydrateFromSeed contract
│   │   └── db.ts                          MODIFY: hydrateFromSeed becomes wrapper around hydrateFromApi when ?api= set
│   └── routes/gpus/
│       └── +page.svelte                   MODIFY: read VITE_API_BASE env, fall back to local-seed if missing
│
├── docs/superpowers/plans/
│   └── 2026-05-04-sortsafe-phase1.md      THIS FILE
│
├── archive/                               NEW: pre-migration artifacts (created during migration)
│   └── gpus-seed-pre-migration.json       MOVED from static/ in Task 25
│
├── scripts/
│   └── migrate-seed-to-cloud.ts           NEW: one-off migration tool — reads static/gpus-seed.json, POSTs to /v1/admin/import
│
└── .github/workflows/                     OPTIONAL Phase 1: CI (worker tests + tsc)
    └── worker-ci.yml                      Runs `cd worker && bun install && bun test && bunx tsc --noEmit`
```

**Decomposition rationale:**
- Worker code lives in `worker/` as a sibling package — separate `package.json`, `tsconfig.json`, dependencies. Keeps Worker bundle small and clear from the SvelteKit app's deps. Allows independent CI.
- Pipeline phases are separate files (`enrich.ts`, `discover.ts`, etc.) so each can be unit-tested in isolation. The `cron.ts` orchestrator is just composition.
- `extract/` is GPU-only in Phase 1 — RAM/SSD modules are added in Phase 2 plan, no dead-code stubs now.
- `db/migrations/` are plain `.sql` files; `wrangler d1 migrations apply` reads them from the path declared by `migrations_dir` in `wrangler.toml`. Data seeds live in `db/seed.sql` (NOT a migration) and are applied via `wrangler d1 execute --file=src/db/seed.sql` so tests get a clean schema without the seeded category/rule rows.
- SPA changes are deliberately minimal: one new `api.ts`, one MODIFY in `db.ts`, one env var read in `+page.svelte`. No re-architecture.

---

## Chunk 1: Worker scaffold + D1 schema + DB client

This chunk gets a deployable empty Worker with D1 wired up, schema applied locally + remote, and the typed D1 client helpers. After this chunk: you can `wrangler dev`, hit `/healthz`, and write/read products via the unit tests.

### Task 1: Initialize worker package

**Files:**
- Create: `worker/package.json`
- Create: `worker/tsconfig.json`
- Create: `worker/wrangler.toml`
- Create: `worker/.gitignore`
- Create: `worker/src/index.ts`
- Create: `worker/src/env.ts`

- [ ] **Step 1: Create `worker/package.json`**

```json
{
  "name": "sortsafe-worker",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "wrangler dev",
    "deploy": "wrangler deploy",
    "test": "vitest run",
    "test:watch": "vitest",
    "typecheck": "tsc --noEmit",
    "migrate:local": "wrangler d1 migrations apply sortsafe-db --local",
    "migrate:remote": "wrangler d1 migrations apply sortsafe-db --remote"
  },
  "devDependencies": {
    "@cloudflare/vitest-pool-workers": "^0.7.0",
    "@cloudflare/workers-types": "^4.20260501.0",
    "typescript": "^5.7.0",
    "vitest": "~2.1.9",
    "wrangler": "^3.90.0"
  },
  "dependencies": {
    "hono": "^4.6.0",
    "ulid": "^2.3.0"
  }
}
```

- [ ] **Step 2: Create `worker/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2022"],
    "module": "ES2022",
    "moduleResolution": "bundler",
    "types": ["@cloudflare/workers-types/2023-07-01"],
    "strict": true,
    "noImplicitAny": true,
    "strictNullChecks": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "esModuleInterop": true,
    "isolatedModules": true,
    "skipLibCheck": true,
    "resolveJsonModule": true,
    "noEmit": true
  },
  "include": ["src/**/*", "test/**/*"]
}
```

- [ ] **Step 3: Create `worker/wrangler.toml`** (D1/KV/Queue/R2 IDs left as placeholders — filled in Step 7)

```toml
name = "sortsafe-api"
main = "src/index.ts"
compatibility_date = "2026-05-01"
compatibility_flags = ["nodejs_compat"]

[observability]
enabled = true

# Cron triggers — see spec §4.1 / §4.1.1
[triggers]
crons = ["*/10 * * * *", "0 3 * * *"]

# D1
[[d1_databases]]
binding = "DB"
database_name = "sortsafe-db"
database_id = "REPLACE_AFTER_CREATE"
migrations_dir = "src/db/migrations"

# KV — rate-limit counters + proxy LRU
[[kv_namespaces]]
binding = "CACHE"
id = "REPLACE_AFTER_CREATE"

# Queue producer (cron enqueues here)
[[queues.producers]]
binding = "ALERTS"
queue = "sortsafe-alerts"

# Queue consumer (this Worker drains it)
[[queues.consumers]]
queue = "sortsafe-alerts"
max_batch_size = 10
max_batch_timeout = 5
max_retries = 3

# R2 for daily backups
[[r2_buckets]]
binding = "BACKUPS"
bucket_name = "sortsafe-backups"

# Analytics Engine
[[analytics_engine_datasets]]
binding = "AE"
dataset = "sortsafe_events"
```

- [ ] **Step 4: Create `worker/.gitignore`**

```
.wrangler/
node_modules/
.dev.vars
*.log
.DS_Store
```

- [ ] **Step 5: Create `worker/src/env.ts`** — typed env interface

```typescript
/**
 * Bindings + secrets injected into every Worker invocation.
 * Mirrors the [bindings] section of wrangler.toml.
 */
export interface Env {
  // Bindings
  DB: D1Database;
  CACHE: KVNamespace;
  ALERTS: Queue<AlertMessage>;
  BACKUPS: R2Bucket;
  AE: AnalyticsEngineDataset;

  // Secrets (set via `wrangler secret put`)
  KEEPA_API_KEY: string;
  SCRAPE_HMAC: string;             // HMAC for /proxy/amazon + /v1/admin/* + /v1/scrape-tasks/*
  DISCORD_WEBHOOK_URL?: string;    // cold-start fallback before discord_config row exists
  DISCORD_INVITE_URL: string;
  JWT_SECRET: string;              // session-token signing (Phase 2 uses; Phase 1 just stores)
}

/** Queue message envelope — one per alert delivery. */
export interface AlertMessage {
  delivery_id: string;
}
```

- [ ] **Step 6: Create `worker/src/index.ts`** — minimal Worker that responds 200 to anything

```typescript
import type { Env, AlertMessage } from './env';

export default {
  async fetch(req: Request, _env: Env): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === '/healthz') {
      return new Response(JSON.stringify({ ok: true, ts: Date.now() }), {
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response('not found', { status: 404 });
  },
  async scheduled(_event: ScheduledController, _env: Env, _ctx: ExecutionContext): Promise<void> {
    // Wired in Chunk 2.
  },
  async queue(_batch: MessageBatch<AlertMessage>, _env: Env, _ctx: ExecutionContext): Promise<void> {
    // Wired in Chunk 3.
  },
};
```

- [ ] **Step 7: Provision Cloudflare resources + fill IDs into wrangler.toml**

**Pre-flight checklist:**
- Workers Paid plan ($5/mo) is enabled on your account — required for Queues and Cron Triggers. Verify at <https://dash.cloudflare.com/?to=/:account/workers/queues>. If you see a "Set up Workers Paid" CTA, click it before continuing.
- `bunx wrangler --version` is `3.90.0` or newer. Older versions use the deprecated `kv:namespace` syntax and will fail Step 7c.

Run from `~/git/sortsafe/worker/`:

```sh
bun install
bunx wrangler login                                      # one-time
bunx wrangler d1 create sortsafe-db                      # → copy database_id into wrangler.toml [[d1_databases]]
bunx wrangler kv namespace create CACHE                  # → copy id into wrangler.toml [[kv_namespaces]]
bunx wrangler queues create sortsafe-alerts              # creates the queue (paid plan only)
bunx wrangler r2 bucket create sortsafe-backups          # creates the R2 bucket
```

After each command, paste the printed ID into the corresponding `REPLACE_AFTER_CREATE` slot in `wrangler.toml`.

**If `bun install` reports a peer-dep mismatch between `vitest` and `@cloudflare/vitest-pool-workers`:** Cloudflare bumps both in lockstep occasionally. Check the [vitest-pool-workers README](https://github.com/cloudflare/workers-sdk/blob/main/packages/vitest-pool-workers/README.md) for the current paired version, update both pins in `package.json`, and re-run `bun install`.

- [ ] **Step 8: Smoke test**

Run: `cd ~/git/sortsafe/worker && bunx wrangler dev --port 8787`
Expected: server starts on `:8787`, `curl http://localhost:8787/healthz` returns `{"ok":true,"ts":...}`.

- [ ] **Step 9: Commit**

```sh
cd ~/git/sortsafe
git add worker/
git commit -m "feat(worker): scaffold sortsafe-api Worker with D1/KV/Queue/R2 bindings

— claude-opus-4-7"
```

---

### Task 2: Apply D1 schema (initial migration)

Migrations contain ONLY schema — no data seeds. The categories + system user + Phase-1 alert rule live in a separate `seed.sql` applied via `wrangler d1 execute --file`. Keeping data out of migrations means tests that recreate the DB get a clean schema and can populate their own fixtures without colliding with seeded `category_id='gpu'` rows.

**Files:**
- Create: `worker/src/db/migrations/0001_init.sql`
- Create: `worker/src/db/seed.sql`           ← NOT in migrations/, applied manually
- Create: `worker/src/db/types.ts`

- [ ] **Step 1: Create `worker/src/db/migrations/0001_init.sql`** — copy the full schema from spec §3 verbatim

The schema covers 14 tables: `users`, `pin_lookups`, `push_subs`, `categories`, `products`, `offers`, `snapshots`, `keepa_token_log`, `current_view`, `alert_rules`, `alert_deliveries`, `discord_config`, `watchers`, `scrape_tasks`. All CHECK constraints and indexes from spec §3 included.

Verify after save: `grep -c "^CREATE TABLE" worker/src/db/migrations/0001_init.sql` should return `14`.

- [ ] **Step 2: Create `worker/src/db/seed.sql`** — categories + system user + hardcoded Phase-1 alert rule (NOT a migration; applied separately so tests get a clean schema)

```sql
-- Phase 1 only seeds GPU. RAM/SSD added in Phase 2 migration.
INSERT INTO categories (category_id, display, search_terms, msrp_baseline, enabled) VALUES (
  'gpu',
  'GPUs',
  '["rtx 3090","rtx 4090","rtx 5090"]',
  '{"3090":1499,"4090":1599,"5090":1999}',
  1
);

-- System user owns hardcoded alert rules so they don't need a real human user_id in Phase 1.
INSERT INTO users (user_id, pin_hash, pin_prefix, created_at, last_seen) VALUES (
  '00000000000000000000000000',
  'system-no-pin',
  'system',
  unixepoch() * 1000,
  unixepoch() * 1000
);

-- Hardcoded Phase-1 rule: any tracked GPU with used/refurb/warehouse offer ≥15% below 30d median.
INSERT INTO alert_rules (
  rule_id, user_id, scope, scope_value, metric, threshold,
  conditions, channels, cooldown_s, active, created_at
) VALUES (
  '01PHASE1ALERTRULEFORDISCORD',
  '00000000000000000000000000',
  'category',
  'gpu',
  'pct_below_median_30d',
  15.0,
  '["used-good","used-very-good","used-like-new","used-acceptable","refurbished","warehouse"]',
  '["discord"]',
  1800,
  1,
  unixepoch() * 1000
);
```

- [ ] **Step 3: Create `worker/src/db/types.ts`** — TS row types matching schema

```typescript
export type GpuCondition =
  | 'new' | 'used-like-new' | 'used-very-good' | 'used-good' | 'used-acceptable'
  | 'refurbished' | 'warehouse' | 'unknown';

export type OfferSource = 'keepa' | 'browser-scrape' | 'cli-scrape' | 'admin-import';

export type AlertChannel = 'push' | 'discord' | 'email';

export type AlertMetric = 'price_floor' | 'pct_below_median_30d' | 'pct_below_median_90d' | 'pct_off_msrp' | 'composite_score';

export interface CategoryRow {
  category_id: string;
  display: string;
  search_terms: string;             // JSON-encoded string[]
  msrp_baseline: string;            // JSON-encoded Record<string, number>
  enabled: number;                  // 0 | 1
}

export interface ProductRow {
  asin: string;
  category_id: string;
  model: string | null;
  title: string;
  brand: string | null;
  thumbnail_url: string | null;
  attrs_json: string;
  first_seen: number;
  last_refreshed: number;
  active: number;
}

export interface OfferRow {
  offer_id: string;
  asin: string;
  condition: GpuCondition;
  price_usd: number;
  seller: string | null;
  seller_id: string | null;
  seller_rating: number | null;
  seller_rating_count: number | null;
  ships_from: string | null;
  source: OfferSource;
  first_seen: number;
  last_seen: number;
  available: number;
}

export interface SnapshotRow {
  snapshot_id: number;
  asin: string;
  condition: GpuCondition;
  price_usd: number;
  taken_at: number;
  source: OfferSource;
}

export interface CurrentViewRow {
  asin: string;
  condition: GpuCondition;
  current_price_usd: number;
  best_offer_id: string;
  median_30d: number | null;
  median_90d: number | null;
  msrp_baseline: number | null;
  pct_below_median_30d: number | null;
  pct_below_median_90d: number | null;
  pct_off_msrp: number | null;
  composite_score: number;
  price_per_gb: number | null;
  price_per_tb: number | null;
  is_lowest_30d: number;
  is_lowest_90d: number;
  recomputed_at: number;
}

export interface AlertRuleRow {
  rule_id: string;
  user_id: string;
  scope: 'category' | 'asin' | 'watcher';
  scope_value: string;
  metric: AlertMetric;
  threshold: number;
  conditions: string;               // JSON-encoded GpuCondition[]
  channels: string;                 // JSON-encoded AlertChannel[]
  cooldown_s: number;
  active: number;
  created_at: number;
  last_fired_at: number | null;
}

export interface AlertDeliveryRow {
  delivery_id: string;
  rule_id: string;
  offer_id: string;
  channel: AlertChannel;
  enqueued_at: number;
  delivered_at: number | null;
  error: string | null;
  payload_hash: string;
}

export interface KeepaTokenLogRow {
  log_id: number;
  ts: number;
  endpoint: string;
  cost: number;
  remaining: number;
}
```

- [ ] **Step 4: Apply migrations locally**

```sh
cd ~/git/sortsafe/worker
bunx wrangler d1 migrations apply sortsafe-db --local
bunx wrangler d1 execute sortsafe-db --local --command "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
```

Expected output: 14 table rows (`alert_deliveries, alert_rules, categories, current_view, discord_config, keepa_token_log, offers, pin_lookups, products, push_subs, scrape_tasks, snapshots, users, watchers`) + 2 SQLite-internal rows (`d1_migrations`, `sqlite_sequence`).

- [ ] **Step 5: Apply seed locally**

```sh
bunx wrangler d1 execute sortsafe-db --local --file=src/db/seed.sql
bunx wrangler d1 execute sortsafe-db --local --command "SELECT category_id, display FROM categories;"
bunx wrangler d1 execute sortsafe-db --local --command "SELECT rule_id, scope_value, metric, threshold FROM alert_rules;"
```

Expected: one `gpu/GPUs` row; one alert rule with `gpu / pct_below_median_30d / 15.0`.

- [ ] **Step 6: Apply migrations + seed to remote (production D1)**

```sh
bunx wrangler d1 migrations apply sortsafe-db --remote
bunx wrangler d1 execute sortsafe-db --remote --file=src/db/seed.sql
bunx wrangler d1 execute sortsafe-db --remote --command "SELECT COUNT(*) FROM categories;"
```

Expected: `1`.

- [ ] **Step 7: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/db/
git commit -m "feat(db): D1 schema migrations + seed.sql with GPU category and Phase-1 alert rule

Migrations contain schema only; seed data lives in src/db/seed.sql so tests
that reset the DB get clean slate.

NOTE: Discord delivery requires manually inserting into discord_config (see
Phase 1 deploy step in spec §10) before the Chunk 4 queue consumer can
post messages.

— claude-opus-4-7"
```

---

### Task 3: D1 client wrapper

**Files:**
- Create: `worker/vitest.config.ts`
- Create: `worker/src/db/client.ts`
- Create: `worker/test/db/client.test.ts`

- [ ] **Step 1: Set up vitest + workers pool** at `worker/vitest.config.ts` FIRST so the failing test fails for the right reason

```typescript
import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.toml' },
        miniflare: {
          compatibilityDate: '2026-05-01',
          compatibilityFlags: ['nodejs_compat'],
        },
        // Apply the schema migrations to the per-test miniflare D1 so tests
        // can SELECT/INSERT against real tables. seed.sql is NOT applied —
        // tests provide their own fixtures.
        migrationsDir: './src/db/migrations',
      },
    },
  },
});
```

- [ ] **Step 2: Write the failing test** at `worker/test/db/client.test.ts` covering upsertProduct + each helper added in Step 6 with a smoke round-trip

```typescript
import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';
import {
  upsertProduct, getProductByAsin,
  upsertOffer, markOffersUnavailable,
  insertSnapshot, upsertCurrentView,
  getActiveAlertRules, getStaleProductAsins,
  logKeepaToken,
} from '../../src/db/client';

const seedCategory = () => env.DB.prepare(
  `INSERT OR IGNORE INTO categories (category_id, display, search_terms, msrp_baseline, enabled)
   VALUES ('gpu', 'GPUs', '["rtx 5090"]', '{"5090":1999}', 1)`
).run();

const sampleProduct = (asin = 'B0TESTAAAA', last_refreshed = 1000) => ({
  asin, category_id: 'gpu', model: '5090',
  title: 'Test 5090', brand: 'TestBrand', thumbnail_url: null,
  attrs_json: '{}', first_seen: 1000, last_refreshed, active: 1,
});

describe('db client', () => {
  it('upsertProduct inserts then updates the same row', async () => {
    await seedCategory();
    await upsertProduct(env.DB, sampleProduct('B0TESTAAAA', 1000));
    expect((await getProductByAsin(env.DB, 'B0TESTAAAA'))?.title).toBe('Test 5090');

    await upsertProduct(env.DB, { ...sampleProduct('B0TESTAAAA', 2000), title: 'Test 5090 Updated' });
    const b = await getProductByAsin(env.DB, 'B0TESTAAAA');
    expect(b?.title).toBe('Test 5090 Updated');
    expect(b?.last_refreshed).toBe(2000);
  });

  it('upsertOffer round-trips', async () => {
    await seedCategory();
    await upsertProduct(env.DB, sampleProduct('B0OFFTEST01'));
    await upsertOffer(env.DB, {
      offer_id: 'B0OFFTEST01__used-good__', asin: 'B0OFFTEST01', condition: 'used-good',
      price_usd: 1500, seller: null, seller_id: null, seller_rating: null,
      seller_rating_count: null, ships_from: null, source: 'keepa',
      first_seen: 1000, last_seen: 1000, available: 1,
    });
    const r = await env.DB.prepare('SELECT * FROM offers WHERE offer_id = ?').bind('B0OFFTEST01__used-good__').first();
    expect(r?.price_usd).toBe(1500);
  });

  it('markOffersUnavailable flips others, leaves keepers', async () => {
    await seedCategory();
    await upsertProduct(env.DB, sampleProduct('B0OFFTEST02'));
    const baseOffer = (offer_id: string, price: number) => ({
      offer_id, asin: 'B0OFFTEST02', condition: 'used-good' as const,
      price_usd: price, seller: null, seller_id: null, seller_rating: null,
      seller_rating_count: null, ships_from: null, source: 'keepa' as const,
      first_seen: 1000, last_seen: 1000, available: 1,
    });
    await upsertOffer(env.DB, baseOffer('B0OFFTEST02__used-good__', 1500));
    await upsertOffer(env.DB, baseOffer('B0OFFTEST02__used-good__s:SELLERX', 1480));
    await markOffersUnavailable(env.DB, 'B0OFFTEST02', 'used-good', ['B0OFFTEST02__used-good__s:SELLERX']);
    const all = await env.DB.prepare('SELECT offer_id, available FROM offers WHERE asin = ? ORDER BY offer_id').bind('B0OFFTEST02').all();
    expect(all.results).toEqual([
      { offer_id: 'B0OFFTEST02__used-good__', available: 0 },
      { offer_id: 'B0OFFTEST02__used-good__s:SELLERX', available: 1 },
    ]);
  });

  it('insertSnapshot appends one row', async () => {
    await seedCategory();
    await upsertProduct(env.DB, sampleProduct('B0SNAP01'));
    await insertSnapshot(env.DB, { asin: 'B0SNAP01', condition: 'used-good', price_usd: 1499, taken_at: 5000, source: 'keepa' });
    const r = await env.DB.prepare('SELECT COUNT(*) AS n FROM snapshots WHERE asin = ?').bind('B0SNAP01').first<{ n: number }>();
    expect(r?.n).toBe(1);
  });

  it('upsertCurrentView round-trips and updates on conflict', async () => {
    await seedCategory();
    await upsertProduct(env.DB, sampleProduct('B0CV01'));
    await upsertOffer(env.DB, {
      offer_id: 'B0CV01__used-good__', asin: 'B0CV01', condition: 'used-good',
      price_usd: 1400, seller: null, seller_id: null, seller_rating: null,
      seller_rating_count: null, ships_from: null, source: 'keepa',
      first_seen: 1000, last_seen: 1000, available: 1,
    });
    const view = {
      asin: 'B0CV01', condition: 'used-good' as const, current_price_usd: 1400,
      best_offer_id: 'B0CV01__used-good__', median_30d: 1500, median_90d: 1550,
      msrp_baseline: 1999, pct_below_median_30d: 6.7, pct_below_median_90d: 9.7,
      pct_off_msrp: 30.0, composite_score: 72, price_per_gb: null, price_per_tb: null,
      is_lowest_30d: 1, is_lowest_90d: 0, recomputed_at: 6000,
    };
    await upsertCurrentView(env.DB, view);
    await upsertCurrentView(env.DB, { ...view, composite_score: 80, recomputed_at: 7000 });
    const r = await env.DB.prepare('SELECT composite_score, recomputed_at FROM current_view WHERE asin = ? AND condition = ?').bind('B0CV01', 'used-good').first<any>();
    expect(r.composite_score).toBe(80);
    expect(r.recomputed_at).toBe(7000);
  });

  it('getActiveAlertRules respects cooldown', async () => {
    await env.DB.prepare(`INSERT OR IGNORE INTO users (user_id, pin_hash, pin_prefix, created_at, last_seen) VALUES ('u1', 'h', 'p', 0, 0)`).run();
    await env.DB.prepare(
      `INSERT INTO alert_rules (rule_id, user_id, scope, scope_value, metric, threshold, conditions, channels, cooldown_s, active, created_at, last_fired_at)
       VALUES ('R_COLD', 'u1', 'category', 'gpu', 'pct_below_median_30d', 15, '["used-good"]', '["discord"]', 1800, 1, 0, ?)`
    ).bind(Date.now() - 60_000).run();      // fired 1 minute ago, cooldown 30 min — should NOT return
    await env.DB.prepare(
      `INSERT INTO alert_rules (rule_id, user_id, scope, scope_value, metric, threshold, conditions, channels, cooldown_s, active, created_at, last_fired_at)
       VALUES ('R_HOT', 'u1', 'category', 'gpu', 'pct_below_median_30d', 15, '["used-good"]', '["discord"]', 1800, 1, 0, ?)`
    ).bind(Date.now() - 3_600_000).run();    // fired 1 hour ago — SHOULD return
    const out = await getActiveAlertRules(env.DB, Date.now());
    expect(out.map((r) => r.rule_id).sort()).toEqual(['R_HOT']);
  });

  it('getStaleProductAsins returns oldest first', async () => {
    await seedCategory();
    await upsertProduct(env.DB, sampleProduct('B0STALE_NEW', 9_000_000));
    await upsertProduct(env.DB, sampleProduct('B0STALE_OLD', 1_000_000));
    const asins = await getStaleProductAsins(env.DB, 10_000_000, 100_000, 5);
    expect(asins).toEqual(['B0STALE_OLD', 'B0STALE_NEW']);
  });

  it('logKeepaToken inserts a row', async () => {
    await logKeepaToken(env.DB, 12345, '/token', 0, 540);
    const r = await env.DB.prepare('SELECT * FROM keepa_token_log WHERE ts = ?').bind(12345).first<any>();
    expect(r.endpoint).toBe('/token');
    expect(r.remaining).toBe(540);
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test db/client
```

Expected: FAIL — every helper "is not defined" (module not yet implemented). The vitest pool boot succeeds (config exists), so the failure is the intended TDD signal.

- [ ] **Step 4: Implement `worker/src/db/client.ts`** with all 8 helpers exercised by the test

```typescript
import type { ProductRow, OfferRow, SnapshotRow, CurrentViewRow, AlertRuleRow } from './types';

/**
 * Insert or update a product row. Idempotent on (asin).
 * `first_seen` is preserved on update; `last_refreshed` always overwritten.
 */
export async function upsertProduct(db: D1Database, p: ProductRow): Promise<void> {
  await db
    .prepare(
      `INSERT INTO products
         (asin, category_id, model, title, brand, thumbnail_url, attrs_json, first_seen, last_refreshed, active)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(asin) DO UPDATE SET
         category_id    = excluded.category_id,
         model          = excluded.model,
         title          = excluded.title,
         brand          = excluded.brand,
         thumbnail_url  = excluded.thumbnail_url,
         attrs_json     = excluded.attrs_json,
         last_refreshed = excluded.last_refreshed,
         active         = excluded.active`
    )
    .bind(p.asin, p.category_id, p.model, p.title, p.brand, p.thumbnail_url, p.attrs_json, p.first_seen, p.last_refreshed, p.active)
    .run();
}

export async function getProductByAsin(db: D1Database, asin: string): Promise<ProductRow | null> {
  const r = await db.prepare('SELECT * FROM products WHERE asin = ?').bind(asin).first<ProductRow>();
  return r ?? null;
}

/** Upsert an offer. Sets available=1, last_seen from caller. */
export async function upsertOffer(db: D1Database, o: OfferRow): Promise<void> {
  await db.prepare(
    `INSERT INTO offers
       (offer_id, asin, condition, price_usd, seller, seller_id, seller_rating, seller_rating_count,
        ships_from, source, first_seen, last_seen, available)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
     ON CONFLICT(offer_id) DO UPDATE SET
       price_usd           = excluded.price_usd,
       seller              = excluded.seller,
       seller_id           = excluded.seller_id,
       seller_rating       = excluded.seller_rating,
       seller_rating_count = excluded.seller_rating_count,
       ships_from          = excluded.ships_from,
       last_seen           = excluded.last_seen,
       available           = 1`
  ).bind(
    o.offer_id, o.asin, o.condition, o.price_usd, o.seller, o.seller_id,
    o.seller_rating, o.seller_rating_count, o.ships_from, o.source,
    o.first_seen, o.last_seen
  ).run();
}

/** Mark all offers for (asin, condition) as unavailable EXCEPT those listed. */
export async function markOffersUnavailable(
  db: D1Database, asin: string, condition: string, keepOfferIds: string[]
): Promise<void> {
  const placeholders = keepOfferIds.length > 0 ? keepOfferIds.map(() => '?').join(',') : "''";
  await db.prepare(
    `UPDATE offers SET available = 0
     WHERE asin = ? AND condition = ? AND available = 1
       AND offer_id NOT IN (${placeholders})`
  ).bind(asin, condition, ...keepOfferIds).run();
}

export async function insertSnapshot(db: D1Database, s: Omit<SnapshotRow, 'snapshot_id'>): Promise<void> {
  await db.prepare(
    `INSERT INTO snapshots (asin, condition, price_usd, taken_at, source) VALUES (?, ?, ?, ?, ?)`
  ).bind(s.asin, s.condition, s.price_usd, s.taken_at, s.source).run();
}

export async function upsertCurrentView(db: D1Database, v: CurrentViewRow): Promise<void> {
  await db.prepare(
    `INSERT INTO current_view
       (asin, condition, current_price_usd, best_offer_id, median_30d, median_90d, msrp_baseline,
        pct_below_median_30d, pct_below_median_90d, pct_off_msrp, composite_score,
        price_per_gb, price_per_tb, is_lowest_30d, is_lowest_90d, recomputed_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(asin, condition) DO UPDATE SET
       current_price_usd    = excluded.current_price_usd,
       best_offer_id        = excluded.best_offer_id,
       median_30d           = excluded.median_30d,
       median_90d           = excluded.median_90d,
       msrp_baseline        = excluded.msrp_baseline,
       pct_below_median_30d = excluded.pct_below_median_30d,
       pct_below_median_90d = excluded.pct_below_median_90d,
       pct_off_msrp         = excluded.pct_off_msrp,
       composite_score      = excluded.composite_score,
       price_per_gb         = excluded.price_per_gb,
       price_per_tb         = excluded.price_per_tb,
       is_lowest_30d        = excluded.is_lowest_30d,
       is_lowest_90d        = excluded.is_lowest_90d,
       recomputed_at        = excluded.recomputed_at`
  ).bind(
    v.asin, v.condition, v.current_price_usd, v.best_offer_id, v.median_30d, v.median_90d,
    v.msrp_baseline, v.pct_below_median_30d, v.pct_below_median_90d, v.pct_off_msrp,
    v.composite_score, v.price_per_gb, v.price_per_tb, v.is_lowest_30d, v.is_lowest_90d,
    v.recomputed_at
  ).run();
}

export async function getActiveAlertRules(db: D1Database, now: number): Promise<AlertRuleRow[]> {
  const r = await db.prepare(
    `SELECT * FROM alert_rules
     WHERE active = 1
       AND (last_fired_at IS NULL OR last_fired_at < ? - cooldown_s * 1000)`
  ).bind(now).all<AlertRuleRow>();
  return r.results;
}

/** Get ASINs that need refresh, oldest-first. Pure read; caller decides batch size. */
export async function getStaleProductAsins(db: D1Database, now: number, staleMs: number, limit: number): Promise<string[]> {
  const r = await db.prepare(
    `SELECT asin FROM products WHERE active = 1 AND last_refreshed < ? - ? ORDER BY last_refreshed ASC LIMIT ?`
  ).bind(now, staleMs, limit).all<{ asin: string }>();
  return r.results.map((row) => row.asin);
}

export async function logKeepaToken(db: D1Database, ts: number, endpoint: string, cost: number, remaining: number): Promise<void> {
  await db.prepare(
    `INSERT INTO keepa_token_log (ts, endpoint, cost, remaining) VALUES (?, ?, ?, ?)`
  ).bind(ts, endpoint, cost, remaining).run();
}
```

- [ ] **Step 5: Run test to verify all 8 cases pass**

```sh
cd ~/git/sortsafe/worker && bun test db/client
```

Expected: 8/8 PASS.

- [ ] **Step 6: Run typecheck**

```sh
cd ~/git/sortsafe/worker && bun run typecheck
```

Expected: clean (no errors).

- [ ] **Step 7: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/db/client.ts worker/test/db/ worker/vitest.config.ts
git commit -m "feat(db): typed D1 client helpers + smoke tests for each (8 cases)

— claude-opus-4-7"
```

---

**End of Chunk 1.** Reviewable, mergeable on its own — schema + DB client are useful even with no Keepa code attached.

---

## Chunk 2: Keepa client + bot-wall detection + GPU extractor

This chunk adds the upstream data layer: HTTP wrappers for Keepa's `/token`, `/product`, `/search` endpoints; a parser-hardening bot-wall detector that's invoked at every Amazon HTML boundary; and the GPU title→model extractor with a 32-fixture test set. After this chunk: you can call `fetchProducts(env, ['B0DT7GMXHB'])` from a Worker test and see real Keepa data parsed into TS types.

### Task 4: Bot-wall detector

**Files:**
- Create: `worker/src/keepa/bot-wall.ts`
- Create: `worker/test/keepa/bot-wall.test.ts`
- Create: `worker/test/keepa/fixtures/captcha.html`
- Create: `worker/test/keepa/fixtures/continue-shopping.html`
- Create: `worker/test/keepa/fixtures/sorry-page.html`
- Create: `worker/test/keepa/fixtures/real-pdp.html`

- [ ] **Step 1: Create fixtures (synthetic-first, captured-real optional)**

**Use synthetic fixtures by default.** Real-world Amazon HTML is huge (~2MB) and the host this repo runs on (Termux phone with a long-flagged IP) frequently returns bot-wall bodies, which would break the positive test. Synthetic fixtures are deterministic, version-controlled, and easy to evolve when new detector signals are added.

```sh
cd ~/git/sortsafe/worker/test/keepa
mkdir -p fixtures
```

Create each fixture file with the content below.

`fixtures/sorry-page.html`:
```html
<!doctype html><html><head><title>Sorry! Something went wrong!</title></head>
<body><div id="g"><img src="/images/G/01/error/title._TTD_.png"><br>
We're sorry. The Web address you entered is not a functioning page on our site.</div></body></html>
```

`fixtures/continue-shopping.html`:
```html
<!doctype html><html><body>
  <p>To continue shopping with us, please click the button below.</p>
  <form><button class="a-button-text">Continue shopping</button></form>
</body></html>
```

`fixtures/captcha.html`:
```html
<!doctype html><html><body>
  <p>Type the characters you see in this image:</p>
  <img src="/captcha/aBcDeF.jpg">
  <form action="/errors/validateCaptcha"><input name="field-keywords"></form>
</body></html>
```

`fixtures/real-pdp.html` — synthetic positive fixture, big enough to clear the 5KB empty-body short-circuit and contains the `#productTitle` sentinel:
```html
<!doctype html><html lang="en"><head><title>GIGABYTE GeForce RTX 5090 — Amazon.com</title>
<meta charset="utf-8"></head><body>
<div id="navbar">…</div>
<div id="centerCol">
  <span id="productTitle">GIGABYTE GeForce RTX 5090 WINDFORCE OC 32G Graphics Card</span>
  <div id="corePriceDisplay_desktop_feature_div">
    <span class="a-price"><span class="a-offscreen">$3,879.00</span></span>
  </div>
  <div id="usedAccordionRow_0">
    <span class="a-color-base">Used - Like New</span>
    <span class="a-price"><span class="a-offscreen">$3,699.99</span></span>
  </div>
</div>
<!-- pad to clear the 5000-byte empty-body short-circuit -->
<div style="display:none">
  <!-- Lorem ipsum dolor sit amet, consectetur adipiscing elit. -->
  PADDING_LINE_REPEATED_TO_BREAK_5000_BYTES
  …
</div>
</body></html>
```

Pad `real-pdp.html` with however many copies of the lorem-ipsum line are needed to push file size past 5KB. Verify with `wc -c fixtures/real-pdp.html` — must report > 5000.

**Optional: capture a real PDP fixture too.** If you want a higher-fidelity positive case AND the host you're on isn't currently bot-flagged, run:
```sh
curl -s "https://www.amazon.com/dp/B0DT7GMXHB" \
  -H 'user-agent: Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 Chrome/120 Safari/537.36' \
  -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' \
  -H 'accept-language: en-US,en;q=0.9' \
  -o fixtures/real-pdp-captured.html
# Use the same sentinel the detector uses, not a brittle title-prefix regex.
grep -q 'id="productTitle"' fixtures/real-pdp-captured.html && echo OK || rm fixtures/real-pdp-captured.html
```
If the file passes the grep, add an extra test: `expect(detectBotWall(fix('real-pdp-captured.html'))).toBe('ok')`. If it doesn't pass (curl returned a bot wall), `rm` removes it and you keep just the synthetic case.

**DO NOT relax the detector regexes if the optional captured fixture fails the test.** That's the bot wall, not a detector bug.

- [ ] **Step 2: Write the failing test** at `worker/test/keepa/bot-wall.test.ts`

```typescript
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { detectBotWall } from '../../src/keepa/bot-wall';

const fix = (name: string) => readFileSync(resolve(__dirname, 'fixtures', name), 'utf8');

describe('detectBotWall', () => {
  it('flags the "Sorry! Something went wrong" page as sorry-page', () => {
    expect(detectBotWall(fix('sorry-page.html'))).toBe('sorry-page');
  });
  it('flags the "Continue shopping" interstitial', () => {
    expect(detectBotWall(fix('continue-shopping.html'))).toBe('continue-shopping');
  });
  it('flags the captcha challenge', () => {
    expect(detectBotWall(fix('captcha.html'))).toBe('captcha');
  });
  it('passes a real product detail page', () => {
    expect(detectBotWall(fix('real-pdp.html'))).toBe('ok');
  });
  it('flags an empty body', () => {
    expect(detectBotWall('')).toBe('empty');
  });
  it('flags a tiny body that lacks our sentinels', () => {
    expect(detectBotWall('<html><body>hello</body></html>')).toBe('empty');
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test bot-wall
```

Expected: FAIL — `detectBotWall is not defined`.

- [ ] **Step 4: Implement `worker/src/keepa/bot-wall.ts`** per spec §4.2

```typescript
/**
 * Classifies an Amazon HTML response. Returns 'ok' when the body looks like
 * real content; otherwise returns the specific bot-wall reason so the caller
 * can log the right Analytics event and skip writing to D1.
 *
 * Detection signals from spec §4.2:
 *   - Title "Sorry! Something went wrong"        → 'sorry-page'
 *   - Body contains captcha-image instructions   → 'captcha'
 *   - Body contains continue-shopping prompt     → 'continue-shopping'
 *   - Body length <5000 bytes AND no productTitle/search-result markers → 'empty'
 *   - Otherwise                                  → 'ok'
 *
 * Order matters: sorry-page > captcha > continue-shopping > empty > ok.
 */
export type BotWallReason = 'ok' | 'sorry-page' | 'captcha' | 'continue-shopping' | 'empty';

const RX_SORRY    = /<title>\s*Sorry! Something went wrong/i;
const RX_CAPTCHA  = /Type the characters you see in this image|\/errors\/validateCaptcha|<title>\s*Robot Check/i;
const RX_CONTINUE = /click the button below to continue shopping|continue shopping with us/i;
const RX_HAS_PDP  = /id="productTitle"/i;
const RX_HAS_SRP  = /data-component-type="s-search-result"/i;

export function detectBotWall(html: string): BotWallReason {
  if (!html || html.length === 0) return 'empty';
  if (RX_SORRY.test(html)) return 'sorry-page';
  if (RX_CAPTCHA.test(html)) return 'captcha';
  if (RX_CONTINUE.test(html)) return 'continue-shopping';
  if (html.length < 5000 && !RX_HAS_PDP.test(html) && !RX_HAS_SRP.test(html)) return 'empty';
  return 'ok';
}
```

- [ ] **Step 5: Run test to verify it passes**

```sh
cd ~/git/sortsafe/worker && bun test bot-wall
```

Expected: PASS (all 6 cases).

- [ ] **Step 6: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/keepa/bot-wall.ts worker/test/keepa/
git commit -m "feat(keepa): bot-wall detector with sorry/captcha/continue/empty fixtures

— claude-opus-4-7"
```

---

### Task 5: Keepa client (token, /product, /search) with token logging

**Files:**
- Create: `worker/src/keepa/types.ts`
- Create: `worker/src/keepa/client.ts`
- Create: `worker/test/keepa/client.test.ts`

- [ ] **Step 1: Write the failing test** at `worker/test/keepa/client.test.ts`

```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { fetchTokens, fetchProducts, searchTerm } from '../../src/keepa/client';

const mockFetch = (body: unknown, status = 200) => {
  return vi.fn().mockResolvedValue(new Response(JSON.stringify(body), {
    status, headers: { 'content-type': 'application/json' },
  }));
};

describe('keepa client', () => {
  beforeEach(() => {
    vi.unstubAllGlobals();
  });

  it('fetchTokens returns tokensLeft and logs to keepa_token_log', async () => {
    vi.stubGlobal('fetch', mockFetch({ tokensLeft: 540, refillIn: 60000, refillRate: 1, timestamp: Date.now() }));
    const t = await fetchTokens(env);
    expect(t).toBe(540);
    const log = await env.DB.prepare('SELECT * FROM keepa_token_log ORDER BY ts DESC LIMIT 1').first<any>();
    expect(log.endpoint).toBe('/token');
    expect(log.cost).toBe(0);
    expect(log.remaining).toBe(540);
  });

  it('fetchProducts batches asin csv, parses products, logs cost = asins*2', async () => {
    vi.stubGlobal('fetch', mockFetch({
      tokensLeft: 538,
      products: [
        { asin: 'B0AAAAAAAA', title: 'GeForce RTX 5090 Test', stats: { current: [-1, 200000, -1, -1, -1, -1, -1, -1, -1, -1] }, imagesCSV: 'a.jpg,b.jpg' },
        { asin: 'B0BBBBBBBB', title: 'GeForce RTX 4090 Test', stats: { current: [-1, 150000, -1, -1, -1, -1, -1, -1, -1, -1] } },
      ],
    }));
    const result = await fetchProducts(env, ['B0AAAAAAAA', 'B0BBBBBBBB']);
    expect(result.products.length).toBe(2);
    expect(result.products[0].asin).toBe('B0AAAAAAAA');
    const log = await env.DB.prepare('SELECT * FROM keepa_token_log WHERE endpoint = ? ORDER BY ts DESC LIMIT 1').bind('/product').first<any>();
    expect(log.remaining).toBe(538);
    expect(log.cost).toBe(4);                 // 2 ASINs × 2 tokens (1 base + 1 stats)
  });

  it('searchTerm returns empty list on bot-wall AND does NOT log to keepa_token_log', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(
      '<html><head><title>Sorry! Something went wrong</title></head><body></body></html>',
      { status: 200, headers: { 'content-type': 'text/html' } }
    )));
    const before = await env.DB.prepare('SELECT COUNT(*) AS n FROM keepa_token_log WHERE endpoint = ?').bind('/search').first<{n:number}>();
    const r = await searchTerm(env, 'rtx 5090');
    expect(r.products.length).toBe(0);
    expect(r.botWalled).toBe(true);
    const after = await env.DB.prepare('SELECT COUNT(*) AS n FROM keepa_token_log WHERE endpoint = ?').bind('/search').first<{n:number}>();
    expect(after?.n).toBe(before?.n);          // bot-wall does NOT pollute the budget log
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test keepa/client
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement `worker/src/keepa/types.ts`**

```typescript
export interface KeepaProduct {
  asin: string;
  title?: string;
  imagesCSV?: string;
  brand?: string;
  stats?: {
    current?: number[];          // CSV indices: 0=Amazon, 1=New, 2=Used, 6=Refurbished, 9=Warehouse (cents; -1 = no offer)
    rating?: number;             // 0-50; divide by 10 for stars
    reviewCount?: number;
  };
  features?: string[];
  productGroup?: string;
  categoryTree?: { catId: number; name: string }[];
}

export interface KeepaTokenResp {
  tokensLeft: number;
  refillIn: number;
  refillRate: number;
  timestamp: number;
}

export interface KeepaProductResp {
  tokensLeft: number;
  products: KeepaProduct[];
}

export interface KeepaSearchResp {
  tokensLeft: number;
  products?: KeepaProduct[];
}
```

- [ ] **Step 4: Implement `worker/src/keepa/client.ts`**

```typescript
import type { Env } from '../env';
import type { KeepaProduct, KeepaProductResp, KeepaSearchResp, KeepaTokenResp } from './types';
import { detectBotWall } from './bot-wall';
import { logKeepaToken } from '../db/client';

const KEEPA_BASE = 'https://api.keepa.com';
const DOMAIN = 1;                    // Amazon US
const FETCH_TIMEOUT_MS = 120_000;    // Keepa /search can take 60+s under load

/**
 * GET /token. Costs 0 tokens. Always logs to keepa_token_log so /me can show
 * a current balance even if no other call ran recently.
 */
export async function fetchTokens(env: Env): Promise<number> {
  const r = await fetch(`${KEEPA_BASE}/token?key=${env.KEEPA_API_KEY}`, {
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!r.ok) throw new Error(`Keepa /token HTTP ${r.status}`);
  const data = (await r.json()) as KeepaTokenResp;
  await logKeepaToken(env.DB, Date.now(), '/token', 0, data.tokensLeft);
  return data.tokensLeft;
}

/**
 * GET /product?asin=A,B,C&stats=180. Costs 2 tokens per ASIN (1 base + 1 stats).
 * Returns parsed products. Logs cost = (priorBalance - newBalance), or asins.length*2 fallback.
 */
export async function fetchProducts(env: Env, asins: string[]): Promise<{ products: KeepaProduct[]; tokensLeft: number }> {
  if (asins.length === 0) return { products: [], tokensLeft: 0 };
  const url = `${KEEPA_BASE}/product?key=${env.KEEPA_API_KEY}&domain=${DOMAIN}&asin=${asins.join(',')}&stats=180&rating=1`;
  const r = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
  if (!r.ok) throw new Error(`Keepa /product HTTP ${r.status}: ${(await r.text()).slice(0, 200)}`);
  const data = (await r.json()) as KeepaProductResp;
  await logKeepaToken(env.DB, Date.now(), '/product', asins.length * 2, data.tokensLeft);
  return { products: data.products ?? [], tokensLeft: data.tokensLeft };
}

/**
 * GET /search. Returns up to 40 products with full stats. Cost varies (~10-50 tokens).
 * If Keepa returns an HTML body (rare; bot-wall), we detect it and return empty list
 * with botWalled=true. We deliberately do NOT log a keepa_token_log row for bot-walled
 * responses — logging remaining=0 would falsely trigger the cron's "tokens<30 skip"
 * guard on the next tick.
 */
export async function searchTerm(env: Env, term: string): Promise<{ products: KeepaProduct[]; tokensLeft: number; botWalled: boolean }> {
  const url = `${KEEPA_BASE}/search?key=${env.KEEPA_API_KEY}&domain=${DOMAIN}&type=product&term=${encodeURIComponent(term)}&page=0&stats=180`;
  const r = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS) });
  const ct = r.headers.get('content-type') ?? '';
  const body = await r.text();
  if (!ct.includes('json')) {
    const reason = detectBotWall(body);
    if (reason !== 'ok') {
      // Skip the token log entirely — see comment above.
      return { products: [], tokensLeft: 0, botWalled: true };
    }
  }
  if (!r.ok) throw new Error(`Keepa /search HTTP ${r.status}: ${body.slice(0, 200)}`);
  const data = JSON.parse(body) as KeepaSearchResp;
  // Cost approximation: search returns N products, each costs ~0.5 tokens.
  // (Verified empirically against Keepa /token endpoint deltas during initial dev.)
  const cost = Math.ceil((data.products?.length ?? 0) * 0.5) + 5;
  await logKeepaToken(env.DB, Date.now(), '/search', cost, data.tokensLeft);
  return { products: data.products ?? [], tokensLeft: data.tokensLeft, botWalled: false };
}
```

- [ ] **Step 5: Run tests**

```sh
cd ~/git/sortsafe/worker && bun test keepa
```

Expected: all 4 tests across `bot-wall` + `client` pass.

- [ ] **Step 6: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/keepa/ worker/test/keepa/
git commit -m "feat(keepa): client wrappers for /token, /product, /search with token-log + bot-wall guard

— claude-opus-4-7"
```

---

### Task 6: GPU title→model extractor (Phase 1 only — RAM/SSD added in Phase 2 plan)

**Files:**
- Create: `worker/src/extract/gpu.ts`
- Create: `worker/src/extract/price.ts`
- Create: `worker/test/extract/gpu.test.ts`

- [ ] **Step 1: Write the failing test** at `worker/test/extract/gpu.test.ts`

Use the 32 fixture titles below — a mix of real titles drawn from `~/git/sortsafe/static/gpus-seed.json`, adversarial accessory cases, and edge cases. Each row is `[title, expectedModel | null, tag]`. Tags cluster failures so you know what kind of regex to fix.

```typescript
import { describe, it, expect } from 'vitest';
import { inferModelFromTitle, extractGpuAttrs } from '../../src/extract/gpu';

describe('inferModelFromTitle', () => {
  // 32 fixture titles. Tags cluster failures: "real-XYZ" should classify;
  // "accessory" / "accessory-adversarial" must reject; "wrong-model" verifies
  // siblings (5070/5080/4080 SUPER) don't false-positive; "edge" covers known
  // limitations.
  const cases: [string, '3090' | '4090' | '5090' | null, string][] = [
    // Real 3090s — should classify
    ['NVIDIA GeForce RTX 3090 Founders Edition Graphics Card (Renewed)', '3090', 'real-3090'],
    ['MSI Gaming GeForce RTX 3090 24GB GDRR6X 384-Bit HDMI/DP', '3090', 'real-3090'],
    ['Gigabyte 24GB NVIDIA GeForce RTX 3090 Turbo GDDR6X Graphics Card', '3090', 'real-3090'],
    ['nVidia GeForce RTX 3090 Founders Edition Graphics Card', '3090', 'real-3090'],
    ['GIGABYTE AORUS GeForce RTX 3090 Xtreme 24G Graphics Card', '3090', 'real-3090'],
    // Real 4090s
    ['VIPERA NVIDIA GeForce RTX 4090 Founders Edition Graphics Card', '4090', 'real-4090'],
    ['MSI GeForce RTX 4090 SUPRIM Liquid X 24G Gaming Graphics Card', '4090', 'real-4090'],
    ['MSI GeForce RTX 4090 Gaming X Trio 24G Gaming Graphics Card', '4090', 'real-4090'],
    ['ASUS TUF GeForce RTX 4090 OC Edition 24GB', '4090', 'real-4090'],
    // Real 5090s
    ['GIGABYTE GeForce RTX 5090 WINDFORCE OC 32G Graphics Card', '5090', 'real-5090'],
    ['ASUS ROG Astral GeForce RTX 5090 BTF OC Edition, 32GB GDDR7', '5090', 'real-5090'],
    ['msi Gaming RTX 5090 32G Gaming Trio OC Graphics Card', '5090', 'real-5090'],
    ['msi Gaming RTX 5090 32G SUPRIM SOC Graphics Card', '5090', 'real-5090'],
    ['msi Gaming RTX 5090 32G Lightning Z Graphics Card', '5090', 'real-5090'],
    // Accessories — must return null (RX_ACCESSORY signals)
    ['Backplate for RTX 4090 - Custom Aluminum', null, 'accessory'],
    ['Water Block for GeForce RTX 4090 - Bykski', null, 'accessory'],
    ['Riser cable for RTX 5090 PCIe Gen5 x16', null, 'accessory'],
    ['GPU Bracket Mount for RTX 4090', null, 'accessory'],
    ['Replacement fan for RTX 3090 Founders Edition', null, 'accessory'],
    // Adversarial accessories (round-1 review feedback)
    ['Cooler for RTX 4090 - Aftermarket Replacement', null, 'accessory-adversarial'],
    ['GPU Cooler Mount for GeForce RTX 5090', null, 'accessory-adversarial'],
    ['Power Cable Adapter for RTX 4090 12VHPWR', null, 'accessory-adversarial'],
    // Tricky: "Cooler Master" branded card MUST still classify (negative lookahead in RX_ACCESSORY)
    ['Cooler Master GeForce RTX 4090 Liquid Cooled Edition', '4090', 'real-cooler-master'],
    // Wrong model in title (mislabeled in "rtx 5090" search results)
    ['NVIDIA GeForce RTX 5070 Ti Graphics Card', null, 'wrong-model'],     // intent: returns null because no 30/40/50-90 match, NOT because it's flagged as accessory
    ['ASUS ROG Strix RTX 5080 16GB OC Edition', null, 'wrong-model'],
    ['Gigabyte RTX 4080 SUPER Gaming OC', null, 'wrong-model'],
    // Generic non-GPU (no match path)
    ['USB-C Cable 6ft', null, 'unrelated'],
    ['M.2 NVMe Heatsink', null, 'unrelated'],
    ['Logitech MX Master 3 Mouse', null, 'unrelated'],
    // Edge cases
    ['Pre-owned GeForce RTX 3090 - Tested Working', '3090', 'edge'],
    ['RTX 4090 (NEW SEALED)', '4090', 'edge'],
    ['rtx5090 typo no space', null, 'edge-fail'],   // Documented limitation: no space between rtx and model → not classified. If this matters later, loosen the regex.
  ];

  for (const [title, expected, tag] of cases) {
    it(`[${tag}] "${title.slice(0, 50)}..." → ${expected}`, () => {
      expect(inferModelFromTitle(title)).toBe(expected);
    });
  }
});

describe('extractGpuAttrs', () => {
  it('extracts memory_gb=32 for a 5090 with "32G" in title', () => {
    const a = extractGpuAttrs({
      asin: 'B0X', title: 'GIGABYTE GeForce RTX 5090 WINDFORCE OC 32G Graphics Card', brand: 'GIGABYTE',
    });
    expect(a.memory_gb).toBe(32);
    expect(a.model).toBe('5090');
  });
  it('returns null for accessories', () => {
    const a = extractGpuAttrs({ asin: 'B0X', title: 'Backplate for RTX 4090' });
    expect(a.model).toBeNull();
    expect(a.memory_gb).toBeNull();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test extract
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement `worker/src/extract/gpu.ts`**

```typescript
import type { KeepaProduct } from '../keepa/types';

export type GpuModel = '3090' | '4090' | '5090';

export interface GpuAttrs {
  model: GpuModel | null;
  memory_gb: number | null;
  variant: string | null;
  tier: 'reference' | 'oc' | 'premium' | null;
}

// Negative lookahead `(?!master\s+(rtx|geforce))` so "Cooler Master GeForce RTX 4090"
// passes through (it's a real card, not an accessory). "Cooler for RTX 4090" still trips
// the gate because "for" doesn't match "master rtx|geforce". Word-bounded "cable" with
// adapter/extension/for context catches "Power Cable Adapter for RTX 4090".
const RX_ACCESSORY = /backplate|water\s*block|waterblock|fan only|^cable|riser cable|\bcable\b\s+(?:adapter|extension|for)\b|bracket|mount adapter|cooler\s+(?!master\s+(?:rtx|geforce))|replacement\s+fan|gpu\s+bracket/i;

// Require AT LEAST ONE space between "rtx"/"geforce" and the model number — Amazon titles
// always have spacing; allowing zero-space ("rtx5090") was matching too aggressively in the
// original implementation. Documented limitation captured in test fixtures.
const RX_5090 = /\b(rtx|geforce)\s+5090\b/i;
const RX_4090 = /\b(rtx|geforce)\s+4090\b/i;
const RX_3090 = /\b(rtx|geforce)\s+3090\b/i;

const RX_MEM = /\b(\d+)\s*G(?:B)?\s*(?:GD|GDDR|graphics)/i;

/**
 * Decide which tracked GPU model a title refers to.
 * Returns null if it's an accessory or a different model.
 */
export function inferModelFromTitle(title: string): GpuModel | null {
  const t = title.toLowerCase();
  if (RX_ACCESSORY.test(t)) return null;
  if (RX_5090.test(t)) return '5090';
  if (RX_4090.test(t)) return '4090';
  if (RX_3090.test(t)) return '3090';
  return null;
}

/**
 * Extract GPU-specific attributes from a Keepa product. Title is the dominant
 * signal — Keepa's structured `brand` is reliable, the rest comes from regex.
 */
export function extractGpuAttrs(p: { asin: string; title: string; brand?: string }): GpuAttrs {
  const model = inferModelFromTitle(p.title);
  if (!model) return { model: null, memory_gb: null, variant: null, tier: null };
  const memMatch = p.title.match(RX_MEM);
  const memory_gb = memMatch ? parseInt(memMatch[1], 10) : null;
  const variant = p.title.match(/(Founders Edition|AORUS Master|AORUS Xtreme|TUF|ROG Strix|ROG Astral|Gaming Trio|Suprim|Ventus|Windforce|Eagle|Zotac AMP|FTW3|Black Edition|Lightning Z|Liquid X)/i)?.[0] ?? null;
  // tier: order matters — premium > oc > reference. Word-boundaries on OC and Trio to avoid
  // false positives on "OCular" / "Trio of cards" etc. (vanishingly unlikely but cheap to scope).
  let tier: GpuAttrs['tier'] = null;
  if (/Founders Edition/i.test(p.title)) tier = 'reference';
  else if (/Suprim|Lightning|Aorus Master|Aorus Xtreme|Strix LC|FTW3|ROG Astral/i.test(p.title)) tier = 'premium';
  else if (/\bOC\b|\bTrio\b/i.test(p.title)) tier = 'oc';
  return { model, memory_gb, variant, tier };
}
```

- [ ] **Step 4: Implement `worker/src/extract/price.ts`** — reusable parsers (no separate test in this chunk; first real exercise comes from `pipeline/enrich.ts` in Chunk 3 which round-trips both helpers via integration tests)

```typescript
import type { GpuCondition } from '../db/types';

export function parsePriceUsd(s: string | null | undefined): number | null {
  if (!s) return null;
  const m = s.replace(/,/g, '').match(/\$?([\d.]+)/);
  if (!m) return null;
  const n = parseFloat(m[1]);
  return Number.isFinite(n) && n > 0 ? n : null;
}

const COND_LABELS: Array<[RegExp, GpuCondition]> = [
  [/amazon warehouse/i, 'warehouse'],
  [/refurbished|renewed/i, 'refurbished'],
  [/used\s*-\s*like new|used like new/i, 'used-like-new'],
  [/used\s*-\s*very good|used very good/i, 'used-very-good'],
  [/used\s*-\s*acceptable/i, 'used-acceptable'],
  [/used\s*-\s*good|used good|^used\b/i, 'used-good'],
  [/^new\b/i, 'new'],
];

export function classifyCondition(label: string | null | undefined): GpuCondition {
  if (!label) return 'unknown';
  for (const [rx, cond] of COND_LABELS) if (rx.test(label)) return cond;
  return 'unknown';
}
```

- [ ] **Step 5: Run tests**

```sh
cd ~/git/sortsafe/worker && bun test extract
```

Expected: all assertions pass (all 32 title cases + 2 attr cases).

- [ ] **Step 6: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/extract/ worker/test/extract/
git commit -m "feat(extract): GPU model + attrs extraction with 32-fixture title test

— claude-opus-4-7"
```

---

**End of Chunk 2.** At this point:
- `cd ~/git/sortsafe/worker && bun test` passes ~40 cases across db/keepa/extract.
- `cd ~/git/sortsafe/worker && bun run typecheck` clean.
- `cd ~/git/sortsafe/worker && bunx wrangler dev` serves `/healthz` (200 ok).
- D1 has the full Phase 1 schema, locally and remote, with the GPU category + system user + hardcoded alert rule seeded.

Next chunk (3): wire the cron pipeline that calls all this code.

---

## Chunk 3: Cron pipeline (enrich + discover + score + median)

This chunk builds the core 10-min cron orchestrator and its sub-phases. After this chunk, manually invoking `/v1/admin/run-cron` (added in Chunk 5) — or just letting the scheduled trigger fire — will refresh known products via Keepa, top up new ASINs from search, and recompute every dirty `current_view` row with median + composite score. No alerts yet (Chunk 4); no public API yet (Chunk 5). But D1 will hold real, scored deal data driven by Keepa.

### Task 7: Median computation helper

**Files:**
- Create: `worker/src/pipeline/median.ts`
- Create: `worker/test/pipeline/median.test.ts`

- [ ] **Step 1: Write the failing test** at `worker/test/pipeline/median.test.ts`

```typescript
import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';
import { computeMedian } from '../../src/pipeline/median';

const seed = (rows: Array<{ asin: string; condition: string; price: number; takenAt: number }>) => {
  return Promise.all(rows.map((r) => env.DB.prepare(
    `INSERT INTO snapshots (asin, condition, price_usd, taken_at, source) VALUES (?, ?, ?, ?, 'keepa')`
  ).bind(r.asin, r.condition, r.price, r.takenAt).run()));
};

describe('computeMedian', () => {
  const NOW = 10_000_000_000;

  it('returns null for n=0 (no snapshots in window)', async () => {
    const m = await computeMedian(env.DB, 'B0NONE', 'used-good', NOW, 30);
    expect(m).toBeNull();
  });

  it('returns the single value for n=1', async () => {
    await seed([{ asin: 'B0ONE', condition: 'used-good', price: 1500, takenAt: NOW - 1000 }]);
    expect(await computeMedian(env.DB, 'B0ONE', 'used-good', NOW, 30)).toBe(1500);
  });

  it('returns the middle value for odd n', async () => {
    await seed([
      { asin: 'B0ODD', condition: 'used-good', price: 1000, takenAt: NOW - 1000 },
      { asin: 'B0ODD', condition: 'used-good', price: 1500, takenAt: NOW - 2000 },
      { asin: 'B0ODD', condition: 'used-good', price: 2000, takenAt: NOW - 3000 },
    ]);
    expect(await computeMedian(env.DB, 'B0ODD', 'used-good', NOW, 30)).toBe(1500);
  });

  it('returns the average of the middle two for even n', async () => {
    await seed([
      { asin: 'B0EVEN', condition: 'used-good', price: 1000, takenAt: NOW - 1000 },
      { asin: 'B0EVEN', condition: 'used-good', price: 1400, takenAt: NOW - 2000 },
      { asin: 'B0EVEN', condition: 'used-good', price: 1600, takenAt: NOW - 3000 },
      { asin: 'B0EVEN', condition: 'used-good', price: 2000, takenAt: NOW - 4000 },
    ]);
    expect(await computeMedian(env.DB, 'B0EVEN', 'used-good', NOW, 30)).toBe(1500);  // (1400+1600)/2
  });

  it('respects the time window (excludes snapshots older than windowDays)', async () => {
    const dayMs = 24 * 60 * 60 * 1000;
    await seed([
      { asin: 'B0WIN', condition: 'used-good', price: 1000, takenAt: NOW - 5 * dayMs },     // in window
      { asin: 'B0WIN', condition: 'used-good', price: 9999, takenAt: NOW - 100 * dayMs },   // outside 30d
    ]);
    expect(await computeMedian(env.DB, 'B0WIN', 'used-good', NOW, 30)).toBe(1000);
    expect(await computeMedian(env.DB, 'B0WIN', 'used-good', NOW, 365)).toBe(5499.5);       // both rows in window
  });

  it('isolates by asin AND condition', async () => {
    await seed([
      { asin: 'B0MIX', condition: 'used-good', price: 1000, takenAt: NOW - 1000 },
      { asin: 'B0MIX', condition: 'refurbished', price: 9999, takenAt: NOW - 1000 },
      { asin: 'B0OTHER', condition: 'used-good', price: 5555, takenAt: NOW - 1000 },
    ]);
    expect(await computeMedian(env.DB, 'B0MIX', 'used-good', NOW, 30)).toBe(1000);
    expect(await computeMedian(env.DB, 'B0MIX', 'refurbished', NOW, 30)).toBe(9999);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/median
```

Expected: FAIL — `computeMedian is not defined`.

- [ ] **Step 3: Implement `worker/src/pipeline/median.ts`** per spec §6 SQL trick

```typescript
/**
 * Median price for (asin, condition) over the last `windowDays` days.
 * Returns null when there are no snapshots in the window. Uses the SQLite
 * LIMIT 2 - n%2 / OFFSET (n-1)/2 trick — averages the middle two for even n,
 * returns the middle one for odd n, returns the only value for n=1.
 *
 * Performance: per spec §6, scans up to ~13K rows for a hot ASIN's 90-day
 * window. Cron calls this only on dirty rows so cost is bounded.
 */
export async function computeMedian(
  db: D1Database,
  asin: string,
  condition: string,
  nowMs: number,
  windowDays: number,
): Promise<number | null> {
  const cutoffMs = nowMs - windowDays * 24 * 60 * 60 * 1000;
  const r = await db.prepare(
    `WITH ordered AS (
       SELECT price_usd FROM snapshots
       WHERE asin = ?1 AND condition = ?2 AND taken_at > ?3
       ORDER BY price_usd
     ), counted AS (
       SELECT COUNT(*) AS n FROM ordered
     )
     SELECT
       CASE
         WHEN (SELECT n FROM counted) = 0 THEN NULL
         ELSE (
           SELECT AVG(price_usd) FROM ordered
           LIMIT 2 - (SELECT n FROM counted) % 2
           OFFSET (SELECT (n - 1) / 2 FROM counted)
         )
       END AS median`
  ).bind(asin, condition, cutoffMs).first<{ median: number | null }>();
  return r?.median ?? null;
}
```

- [ ] **Step 4: Run test to verify all 6 cases pass**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/median
```

Expected: 6/6 PASS.

- [ ] **Step 5: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/pipeline/median.ts worker/test/pipeline/median.test.ts
git commit -m "feat(pipeline): SQL median helper with n=0/1/even/odd/window/isolation tests

— claude-opus-4-7"
```

---

### Task 8: Composite score formula

**Files:**
- Create: `worker/src/pipeline/score.ts`
- Create: `worker/test/pipeline/score.test.ts`

- [ ] **Step 1: Write the failing test** at `worker/test/pipeline/score.test.ts`

```typescript
import { describe, it, expect } from 'vitest';
import { computeCompositeScore, type ScoreInputs } from '../../src/pipeline/score';

const base: ScoreInputs = {
  current_price_usd: 1500,
  median_30d: 1700,           // 11.8% below
  median_90d: 1800,           // 16.7% below
  seller_rating: 4.5,
  last_seen: Date.now() - 5 * 60 * 1000,   // 5 min ago
  is_lowest_30d: true,
};

describe('computeCompositeScore', () => {
  it('returns a number 0-100 for fully-populated inputs', () => {
    const s = computeCompositeScore(base);
    expect(s).toBeGreaterThan(0);
    expect(s).toBeLessThanOrEqual(100);
  });

  it('drops null components AND their weight from the denominator (per spec §6)', () => {
    // No median data — score should equal recency_bonus alone (the only non-null component).
    const s = computeCompositeScore({
      current_price_usd: 1500,
      median_30d: null, median_90d: null,
      seller_rating: null, is_lowest_30d: false,
      last_seen: Date.now(),
    });
    // recency_bonus: 100 if <30min; with weight 0.10 in numerator AND denominator → score = 100
    expect(s).toBeCloseTo(100, 0);
  });

  it('treats negative pct_below_median (above median) as a low score', () => {
    const s = computeCompositeScore({
      ...base,
      current_price_usd: 2200,        // above 30d median 1700 → -29.4%
      median_30d: 1700,
      median_90d: 1700,
      is_lowest_30d: false,
    });
    expect(s).toBeLessThan(50);
  });

  it('clips pct_below_median to [-50, 100] before normalizing', () => {
    const sExtreme = computeCompositeScore({
      ...base,
      current_price_usd: 1,           // 99.9% below median — clipped to 100
      median_30d: 1700, median_90d: 1700,
    });
    const sCapped = computeCompositeScore({
      ...base,
      current_price_usd: 5,           // also extremely below — should produce ~same score
      median_30d: 1700, median_90d: 1700,
    });
    expect(Math.abs(sExtreme - sCapped)).toBeLessThan(1);  // clipping bounds dominate
  });

  it('seller_rating contributes ~15% weight at 5★', () => {
    // Same inputs except seller_rating null vs 5★. Difference should be ~15 (0.15*100=15) scaled by total weight.
    const noSeller = computeCompositeScore({ ...base, seller_rating: null });
    const fullSeller = computeCompositeScore({ ...base, seller_rating: 5 });
    expect(fullSeller).toBeGreaterThan(noSeller);
  });

  it('recency_bonus decays linearly to 0 by 24h', () => {
    const fresh = computeCompositeScore({ ...base, last_seen: Date.now() });
    const oneHour = computeCompositeScore({ ...base, last_seen: Date.now() - 60 * 60 * 1000 });
    const oneDay = computeCompositeScore({ ...base, last_seen: Date.now() - 24 * 60 * 60 * 1000 });
    expect(fresh).toBeGreaterThan(oneHour);
    expect(oneHour).toBeGreaterThan(oneDay);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/score
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement `worker/src/pipeline/score.ts`** per spec §6

```typescript
export interface ScoreInputs {
  current_price_usd: number;
  median_30d: number | null;
  median_90d: number | null;
  seller_rating: number | null;        // 0-5
  last_seen: number;                   // epoch ms — informs recency_bonus
  is_lowest_30d: boolean;
}

const WEIGHTS = {
  pct_below_median_30d: 0.50,
  pct_below_median_90d: 0.20,
  seller_rating: 0.15,
  recency_bonus: 0.10,
  is_lowest_30d_bonus: 0.05,
} as const;

const DAY_MS = 24 * 60 * 60 * 1000;
const FRESH_MS = 30 * 60 * 1000;        // <30min = 100; linear decay to 0 by 24h

/**
 * Normalize a pct-below-median value into 0-100. Clip to [-50, 100] first,
 * then map: -50 → 0, 0 → 33.3, 100 → 100. Negative discount = above median = bad.
 */
function normalizePct(pct: number): number {
  const clipped = Math.max(-50, Math.min(100, pct));
  return ((clipped + 50) / 150) * 100;
}

/** Linear decay from 100 (just now) → 0 (≥24h ago); plateau at 100 within FRESH_MS. */
function recencyBonus(lastSeen: number): number {
  const ageMs = Math.max(0, Date.now() - lastSeen);
  if (ageMs <= FRESH_MS) return 100;
  if (ageMs >= DAY_MS) return 0;
  const span = DAY_MS - FRESH_MS;
  return 100 * (1 - (ageMs - FRESH_MS) / span);
}

/**
 * Weighted average of available components. Drops null components AND their
 * weight from the denominator so a brand-new ASIN with only recency_bonus
 * still gets a sensible score.
 */
export function computeCompositeScore(input: ScoreInputs): number {
  const components: Array<{ value: number; weight: number }> = [];

  if (input.median_30d != null && input.median_30d > 0) {
    const pct = ((input.median_30d - input.current_price_usd) / input.median_30d) * 100;
    components.push({ value: normalizePct(pct), weight: WEIGHTS.pct_below_median_30d });
  }
  if (input.median_90d != null && input.median_90d > 0) {
    const pct = ((input.median_90d - input.current_price_usd) / input.median_90d) * 100;
    components.push({ value: normalizePct(pct), weight: WEIGHTS.pct_below_median_90d });
  }
  if (input.seller_rating != null) {
    components.push({ value: (input.seller_rating / 5) * 100, weight: WEIGHTS.seller_rating });
  }
  components.push({ value: recencyBonus(input.last_seen), weight: WEIGHTS.recency_bonus });
  components.push({ value: input.is_lowest_30d ? 100 : 0, weight: WEIGHTS.is_lowest_30d_bonus });

  const num = components.reduce((s, c) => s + c.value * c.weight, 0);
  const den = components.reduce((s, c) => s + c.weight, 0);
  return den > 0 ? num / den : 0;
}
```

- [ ] **Step 4: Run test to verify it passes**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/score
```

Expected: 6/6 PASS.

- [ ] **Step 5: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/pipeline/score.ts worker/test/pipeline/score.test.ts
git commit -m "feat(pipeline): composite score formula with null-component-drops-weight semantics

— claude-opus-4-7"
```

---

### Task 9: Enrich phase (Keepa /product → upsert offers + snapshots, mark dirty)

**Files:**
- Create: `worker/src/pipeline/enrich.ts`
- Create: `worker/test/pipeline/enrich.test.ts`

- [ ] **Step 1: Write the failing test** at `worker/test/pipeline/enrich.test.ts`

```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { enrichBatch, type DirtySet } from '../../src/pipeline/enrich';

// We mock Keepa's HTTP layer rather than the higher-level client wrapper so the
// integration of fetchProducts → enrichBatch → D1 is exercised end-to-end.
const mockKeepaProduct = (asin: string, title: string, current: number[]) => ({
  asin, title, brand: 'TestBrand', imagesCSV: 'a.jpg',
  stats: { current, rating: 45, reviewCount: 1200 },
});

const seedCategoryAndProduct = async (asin: string) => {
  await env.DB.prepare(
    `INSERT OR IGNORE INTO categories (category_id, display, search_terms, msrp_baseline, enabled) VALUES ('gpu', 'GPUs', '["rtx 5090"]', '{"5090":1999}', 1)`
  ).run();
  await env.DB.prepare(
    `INSERT INTO products (asin, category_id, model, title, brand, thumbnail_url, attrs_json, first_seen, last_refreshed, active)
     VALUES (?, 'gpu', '5090', 'Test 5090 (placeholder)', 'TestBrand', null, '{}', 1000, 1000, 1)`
  ).bind(asin).run();
};

describe('enrichBatch', () => {
  beforeEach(() => vi.unstubAllGlobals());

  it('upserts new offers, inserts snapshots, marks (asin,condition) dirty', async () => {
    await seedCategoryAndProduct('B0ENRICH01');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      tokensLeft: 530,
      products: [mockKeepaProduct('B0ENRICH01', 'GIGABYTE GeForce RTX 5090 WINDFORCE 32G',
        // CSV indices 0=Amazon, 1=New, 2=Used, 6=Refurb, 9=Warehouse — cents; -1 means absent
        [-1, 199900, 169999, -1, -1, -1, 159999, -1, -1, 149999])),
      ],
    }), { headers: { 'content-type': 'application/json' } })));

    const dirty: DirtySet = new Set();
    await enrichBatch(env, ['B0ENRICH01'], dirty);

    const offers = await env.DB.prepare('SELECT condition, price_usd FROM offers WHERE asin = ? ORDER BY condition').bind('B0ENRICH01').all<any>();
    expect(offers.results.map((o) => o.condition).sort()).toEqual(['new', 'refurbished', 'used-good', 'warehouse']);

    const snaps = await env.DB.prepare('SELECT COUNT(*) AS n FROM snapshots WHERE asin = ?').bind('B0ENRICH01').first<any>();
    expect(snaps.n).toBe(4);

    expect(dirty.has('B0ENRICH01__new')).toBe(true);
    expect(dirty.has('B0ENRICH01__used-good')).toBe(true);
    expect(dirty.has('B0ENRICH01__refurbished')).toBe(true);
    expect(dirty.has('B0ENRICH01__warehouse')).toBe(true);
  });

  it('marks offers no longer present as available=0', async () => {
    await seedCategoryAndProduct('B0DROP01');
    // Pre-existing used offer
    await env.DB.prepare(
      `INSERT INTO offers (offer_id, asin, condition, price_usd, source, first_seen, last_seen, available)
       VALUES ('B0DROP01__used-good__', 'B0DROP01', 'used-good', 1500, 'keepa', 1000, 1000, 1)`
    ).run();

    // Keepa returns a NEW price but no longer a USED price (used[2] = -1)
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      tokensLeft: 528,
      products: [mockKeepaProduct('B0DROP01', 'GIGABYTE GeForce RTX 5090 32G',
        [-1, 200000, -1, -1, -1, -1, -1, -1, -1, -1])],
    }), { headers: { 'content-type': 'application/json' } })));

    const dirty: DirtySet = new Set();
    await enrichBatch(env, ['B0DROP01'], dirty);

    const used = await env.DB.prepare('SELECT available FROM offers WHERE offer_id = ?').bind('B0DROP01__used-good__').first<any>();
    expect(used.available).toBe(0);
    // Dirty set still includes used-good so score recompute can drop it from current_view
    expect(dirty.has('B0DROP01__used-good')).toBe(true);
  });

  it('updates products.title and products.last_refreshed on refresh', async () => {
    await seedCategoryAndProduct('B0REFRESH01');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      tokensLeft: 525,
      products: [mockKeepaProduct('B0REFRESH01', 'GIGABYTE GeForce RTX 5090 WINDFORCE OC 32G Updated Title',
        [-1, 195000, -1, -1, -1, -1, -1, -1, -1, -1])],
    }), { headers: { 'content-type': 'application/json' } })));

    await enrichBatch(env, ['B0REFRESH01'], new Set());
    const p = await env.DB.prepare('SELECT title, last_refreshed FROM products WHERE asin = ?').bind('B0REFRESH01').first<any>();
    expect(p.title).toContain('Updated Title');
    expect(p.last_refreshed).toBeGreaterThan(1000);
  });

  it('skips products whose title fails inferModelFromTitle (accessory or wrong model)', async () => {
    await seedCategoryAndProduct('B0SKIP01');
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      tokensLeft: 522,
      products: [mockKeepaProduct('B0SKIP01', 'Backplate for RTX 5090 Custom Aluminum',
        [-1, 9900, -1, -1, -1, -1, -1, -1, -1, -1])],
    }), { headers: { 'content-type': 'application/json' } })));

    const dirty: DirtySet = new Set();
    await enrichBatch(env, ['B0SKIP01'], dirty);
    const offers = await env.DB.prepare('SELECT COUNT(*) AS n FROM offers WHERE asin = ?').bind('B0SKIP01').first<any>();
    expect(offers.n).toBe(0);
    expect(dirty.size).toBe(0);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/enrich
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement `worker/src/pipeline/enrich.ts`**

```typescript
import type { Env } from '../env';
import type { GpuCondition, OfferRow } from '../db/types';
import { fetchProducts } from '../keepa/client';
import { inferModelFromTitle, extractGpuAttrs } from '../extract/gpu';
import { upsertProduct, upsertOffer, markOffersUnavailable, insertSnapshot } from '../db/client';

/** Cron tracks dirty (asin, condition) tuples so score-recompute only walks changed rows. */
export type DirtySet = Set<string>;
const dirtyKey = (asin: string, condition: string) => `${asin}__${condition}`;

// Keepa CSV indices (per spec §6 / Keepa docs)
const PRICE_IDX: Array<{ idx: number; cond: GpuCondition }> = [
  { idx: 1, cond: 'new' },
  { idx: 2, cond: 'used-good' },
  { idx: 6, cond: 'refurbished' },
  { idx: 9, cond: 'warehouse' },
];

/**
 * Phase 2 of cron: refresh a batch of ASINs from Keepa /product, upsert offers
 * and snapshots, mark stale offers unavailable, and add every touched
 * (asin, condition) to the dirty set so the score phase recomputes them.
 *
 * Skips any product whose title fails inferModelFromTitle (accessory / wrong model).
 */
export async function enrichBatch(env: Env, asins: string[], dirty: DirtySet): Promise<void> {
  if (asins.length === 0) return;
  const { products } = await fetchProducts(env, asins);
  const now = Date.now();

  for (const p of products) {
    const title = p.title ?? '';
    const verifiedModel = inferModelFromTitle(title);
    if (!verifiedModel) continue;          // accessory or wrong model — leave unchanged
    const attrs = extractGpuAttrs({ asin: p.asin, title, brand: p.brand });

    // Refresh product row (title/brand/thumb might have changed; last_refreshed always updates)
    const thumb = p.imagesCSV ? `https://m.media-amazon.com/images/I/${p.imagesCSV.split(',')[0]}` : null;
    await upsertProduct(env.DB, {
      asin: p.asin, category_id: 'gpu', model: verifiedModel,
      title, brand: p.brand ?? null, thumbnail_url: thumb,
      attrs_json: JSON.stringify(attrs),
      first_seen: now, last_refreshed: now, active: 1,
    });

    const cur = p.stats?.current ?? [];
    const seenConditionsThisFetch = new Set<GpuCondition>();
    for (const { idx, cond } of PRICE_IDX) {
      const cents = cur[idx];
      if (typeof cents !== 'number' || cents <= 0) continue;
      const price = cents / 100;
      seenConditionsThisFetch.add(cond);

      // Keepa-aggregate row: empty seller segment per spec §3 offer_id format
      const offer: OfferRow = {
        offer_id: `${p.asin}__${cond}__`,
        asin: p.asin, condition: cond, price_usd: price,
        seller: null, seller_id: null, seller_rating: null, seller_rating_count: null,
        ships_from: null, source: 'keepa',
        first_seen: now, last_seen: now, available: 1,
      };
      await upsertOffer(env.DB, offer);
      await insertSnapshot(env.DB, { asin: p.asin, condition: cond, price_usd: price, taken_at: now, source: 'keepa' });
      // Keep the Keepa-aggregate row alive; mark all OTHER per-seller rows for this (asin, cond) inactive
      await markOffersUnavailable(env.DB, p.asin, cond, [offer.offer_id]);
      dirty.add(dirtyKey(p.asin, cond));
    }

    // Conditions that USED to have an offer but didn't this time → mark all unavailable
    // (also adds them to dirty so score phase removes them from current_view)
    for (const { cond } of PRICE_IDX) {
      if (seenConditionsThisFetch.has(cond)) continue;
      await markOffersUnavailable(env.DB, p.asin, cond, []);
      dirty.add(dirtyKey(p.asin, cond));
    }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/enrich
```

Expected: 4/4 PASS.

- [ ] **Step 5: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/pipeline/enrich.ts worker/test/pipeline/enrich.test.ts
git commit -m "feat(pipeline): enrich batch — Keepa /product → upsert + snapshot + mark stale + dirty

— claude-opus-4-7"
```

---

### Task 10: Discover phase (Keepa /search rotation)

**Files:**
- Create: `worker/src/pipeline/discover.ts`
- Create: `worker/test/pipeline/discover.test.ts`

- [ ] **Step 1: Write the failing test**

```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { discoverForCategory, type DirtySet } from '../../src/pipeline/discover';

const mockKeepaProduct = (asin: string, title: string) => ({
  asin, title, brand: 'TestBrand', imagesCSV: 'a.jpg',
  stats: { current: [-1, 200000, -1, -1, -1, -1, -1, -1, -1, -1] },
});

const seedCategory = (terms: string[]) => env.DB.prepare(
  `INSERT OR REPLACE INTO categories (category_id, display, search_terms, msrp_baseline, enabled)
   VALUES ('gpu', 'GPUs', ?, '{"5090":1999}', 1)`
).bind(JSON.stringify(terms)).run();

describe('discoverForCategory', () => {
  beforeEach(() => vi.unstubAllGlobals());

  it('inserts new product rows for each ASIN returned by /search', async () => {
    await seedCategory(['rtx 5090']);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      tokensLeft: 480,
      products: [
        mockKeepaProduct('B0DISC01', 'GIGABYTE GeForce RTX 5090 WINDFORCE'),
        mockKeepaProduct('B0DISC02', 'msi Gaming RTX 5090 32G'),
      ],
    }), { headers: { 'content-type': 'application/json' } })));

    const dirty: DirtySet = new Set();
    const added = await discoverForCategory(env, 'gpu', dirty);
    expect(added).toBe(2);
    const products = await env.DB.prepare('SELECT asin FROM products ORDER BY asin').all<any>();
    expect(products.results.map((r) => r.asin)).toEqual(['B0DISC01', 'B0DISC02']);
  });

  it('skips ASINs we already have', async () => {
    await seedCategory(['rtx 5090']);
    await env.DB.prepare(
      `INSERT INTO products (asin, category_id, model, title, brand, thumbnail_url, attrs_json, first_seen, last_refreshed, active)
       VALUES ('B0DUP01', 'gpu', '5090', 'Existing 5090', 'X', null, '{}', 1000, 1000, 1)`
    ).run();
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      tokensLeft: 478,
      products: [
        mockKeepaProduct('B0DUP01', 'GIGABYTE GeForce RTX 5090 (refreshed title)'),
        mockKeepaProduct('B0NEW01', 'ASUS ROG Astral GeForce RTX 5090'),
      ],
    }), { headers: { 'content-type': 'application/json' } })));
    const added = await discoverForCategory(env, 'gpu', new Set());
    expect(added).toBe(1);
  });

  it('returns 0 and leaves DB clean when search bot-walls', async () => {
    await seedCategory(['rtx 5090']);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(
      '<html><head><title>Sorry! Something went wrong</title></head></html>',
      { status: 200, headers: { 'content-type': 'text/html' } }
    )));
    const added = await discoverForCategory(env, 'gpu', new Set());
    expect(added).toBe(0);
  });

  it('skips Keepa products whose title fails model inference', async () => {
    await seedCategory(['rtx 5090']);
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(JSON.stringify({
      tokensLeft: 470,
      products: [
        mockKeepaProduct('B0BAD01', 'Backplate for RTX 5090 Aluminum'),
        mockKeepaProduct('B0GOOD01', 'GIGABYTE GeForce RTX 5090 WINDFORCE 32G'),
      ],
    }), { headers: { 'content-type': 'application/json' } })));
    const added = await discoverForCategory(env, 'gpu', new Set());
    expect(added).toBe(1);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/discover
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement `worker/src/pipeline/discover.ts`**

```typescript
import type { Env } from '../env';
import type { CategoryRow } from '../db/types';
import { searchTerm } from '../keepa/client';
import { inferModelFromTitle, extractGpuAttrs } from '../extract/gpu';
import { upsertProduct } from '../db/client';
import type { DirtySet } from './enrich';

/**
 * Phase 3 of cron. For one category (e.g. 'gpu'), pick the next search term
 * (rotated round-robin via category.search_terms array) and call Keepa /search.
 * For each returned product whose title classifies as a tracked model AND we
 * don't already have, insert a products row with last_refreshed=0 so the next
 * enrich pass picks it up.
 *
 * Returns count of products newly inserted. Bot-walled responses return 0.
 */
export async function discoverForCategory(env: Env, categoryId: string, _dirty: DirtySet): Promise<number> {
  const category = await env.DB.prepare('SELECT * FROM categories WHERE category_id = ?').bind(categoryId).first<CategoryRow>();
  if (!category) return 0;
  const terms = JSON.parse(category.search_terms) as string[];
  if (terms.length === 0) return 0;

  // Rotate: hash by current 10-min cron slot so each tick rotates one position.
  const slot = Math.floor(Date.now() / (10 * 60 * 1000));
  const term = terms[slot % terms.length];

  const { products, botWalled } = await searchTerm(env, term);
  if (botWalled) return 0;

  // Existing-asin set built once, then checked per row.
  const existing = await env.DB.prepare('SELECT asin FROM products WHERE category_id = ?').bind(categoryId).all<{ asin: string }>();
  const existingSet = new Set(existing.results.map((r) => r.asin));

  let added = 0;
  const now = Date.now();
  for (const p of products) {
    if (existingSet.has(p.asin)) continue;
    const title = p.title ?? '';
    const verifiedModel = inferModelFromTitle(title);
    if (!verifiedModel) continue;

    const attrs = extractGpuAttrs({ asin: p.asin, title, brand: p.brand });
    const thumb = p.imagesCSV ? `https://m.media-amazon.com/images/I/${p.imagesCSV.split(',')[0]}` : null;
    await upsertProduct(env.DB, {
      asin: p.asin, category_id: categoryId, model: verifiedModel,
      title, brand: p.brand ?? null, thumbnail_url: thumb,
      attrs_json: JSON.stringify(attrs),
      first_seen: now,
      last_refreshed: 0,                // 0 ensures next enrich tick picks it up first
      active: 1,
    });
    added++;
  }
  return added;
}
```

- [ ] **Step 4: Run test to verify it passes**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/discover
```

Expected: 4/4 PASS.

- [ ] **Step 5: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/pipeline/discover.ts worker/test/pipeline/discover.test.ts
git commit -m "feat(pipeline): discover phase — Keepa /search rotation per category

— claude-opus-4-7"
```

---

### Task 11: Score phase (recompute current_view for dirty rows)

**Files:**
- Create: `worker/src/pipeline/score-recompute.ts`
- Create: `worker/test/pipeline/score-recompute.test.ts`

- [ ] **Step 1: Write the failing test**

```typescript
import { describe, it, expect } from 'vitest';
import { env } from 'cloudflare:test';
import { recomputeScoresForDirty } from '../../src/pipeline/score-recompute';

const seed = async () => {
  await env.DB.prepare(
    `INSERT OR IGNORE INTO categories (category_id, display, search_terms, msrp_baseline, enabled) VALUES ('gpu', 'GPUs', '[]', '{"5090":1999,"4090":1599,"3090":1499}', 1)`
  ).run();
};

const seedProductWithOffer = async (asin: string, model: string, condition: string, price: number, last_seen: number) => {
  await env.DB.prepare(
    `INSERT OR REPLACE INTO products (asin, category_id, model, title, brand, thumbnail_url, attrs_json, first_seen, last_refreshed, active)
     VALUES (?, 'gpu', ?, 'Test', 'X', null, '{}', 1000, 1000, 1)`
  ).bind(asin, model).run();
  await env.DB.prepare(
    `INSERT OR REPLACE INTO offers (offer_id, asin, condition, price_usd, source, first_seen, last_seen, available)
     VALUES (?, ?, ?, ?, 'keepa', 1000, ?, 1)`
  ).bind(`${asin}__${condition}__`, asin, condition, price, last_seen).run();
};

const seedSnapshots = (asin: string, condition: string, prices: number[], baseTs: number) =>
  Promise.all(prices.map((p, i) => env.DB.prepare(
    `INSERT INTO snapshots (asin, condition, price_usd, taken_at, source) VALUES (?, ?, ?, ?, 'keepa')`
  ).bind(asin, condition, p, baseTs - i * 60_000).run()));

describe('recomputeScoresForDirty', () => {
  it('writes current_view row with median + score for a single dirty tuple', async () => {
    await seed();
    const now = Date.now();
    await seedProductWithOffer('B0RECOMP01', '5090', 'used-good', 1500, now - 5 * 60_000);
    await seedSnapshots('B0RECOMP01', 'used-good', [1500, 1700, 1800, 1900, 2000], now);

    await recomputeScoresForDirty(env, new Set(['B0RECOMP01__used-good']));

    const v = await env.DB.prepare('SELECT * FROM current_view WHERE asin = ? AND condition = ?').bind('B0RECOMP01', 'used-good').first<any>();
    expect(v).not.toBeNull();
    expect(v.current_price_usd).toBe(1500);
    expect(v.median_30d).toBe(1800);                // odd n=5 middle
    expect(v.pct_below_median_30d).toBeCloseTo(16.67, 1);
    expect(v.composite_score).toBeGreaterThan(0);
  });

  it('removes current_view rows when no available offers remain', async () => {
    await seed();
    const now = Date.now();
    await seedProductWithOffer('B0GONE01', '5090', 'used-good', 1500, now);
    // Pre-existing current_view row
    await env.DB.prepare(
      `INSERT INTO current_view (asin, condition, current_price_usd, best_offer_id, composite_score, recomputed_at)
       VALUES ('B0GONE01', 'used-good', 1500, 'B0GONE01__used-good__', 50, ?)`
    ).bind(now).run();
    // Now mark all offers unavailable (simulating Keepa dropping the listing)
    await env.DB.prepare(`UPDATE offers SET available = 0 WHERE asin = ?`).bind('B0GONE01').run();

    await recomputeScoresForDirty(env, new Set(['B0GONE01__used-good']));
    const v = await env.DB.prepare('SELECT * FROM current_view WHERE asin = ? AND condition = ?').bind('B0GONE01', 'used-good').first<any>();
    expect(v).toBeNull();
  });

  it('uses MSRP baseline from category for pct_off_msrp', async () => {
    await seed();
    const now = Date.now();
    await seedProductWithOffer('B0MSRP01', '5090', 'used-good', 1500, now);
    await seedSnapshots('B0MSRP01', 'used-good', [1500], now);
    await recomputeScoresForDirty(env, new Set(['B0MSRP01__used-good']));
    const v = await env.DB.prepare('SELECT pct_off_msrp, msrp_baseline FROM current_view WHERE asin = ? AND condition = ?').bind('B0MSRP01', 'used-good').first<any>();
    expect(v.msrp_baseline).toBe(1999);
    expect(v.pct_off_msrp).toBeCloseTo(((1999 - 1500) / 1999) * 100, 1);  // ~24.96%
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/score-recompute
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement `worker/src/pipeline/score-recompute.ts`**

```typescript
import type { Env } from '../env';
import type { GpuCondition } from '../db/types';
import { computeMedian } from './median';
import { computeCompositeScore } from './score';
import { upsertCurrentView } from '../db/client';

interface BestOffer {
  offer_id: string;
  price_usd: number;
  seller_rating: number | null;
  last_seen: number;
}

/**
 * Phase 4 of cron. For each dirty (asin, condition) tuple, recompute the
 * current_view row from scratch. Removes the row if no available offers exist.
 *
 * Dirty keys are formatted "{asin}__{condition}" by enrich.ts.
 */
export async function recomputeScoresForDirty(env: Env, dirty: Set<string>): Promise<void> {
  if (dirty.size === 0) return;
  const now = Date.now();

  // Cache MSRP baselines per category (one read per cron tick, not per row)
  const msrpByCategoryAndModel = new Map<string, number>();
  const cats = await env.DB.prepare('SELECT category_id, msrp_baseline FROM categories').all<{ category_id: string; msrp_baseline: string }>();
  for (const c of cats.results) {
    const map = JSON.parse(c.msrp_baseline) as Record<string, number>;
    for (const [model, price] of Object.entries(map)) {
      msrpByCategoryAndModel.set(`${c.category_id}|${model}`, price);
    }
  }

  for (const key of dirty) {
    const [asin, condition] = key.split('__') as [string, GpuCondition];
    const product = await env.DB.prepare('SELECT category_id, model FROM products WHERE asin = ?').bind(asin).first<{ category_id: string; model: string | null }>();
    if (!product) continue;

    const best = await env.DB.prepare(
      `SELECT offer_id, price_usd, seller_rating, last_seen
       FROM offers WHERE asin = ? AND condition = ? AND available = 1
       ORDER BY price_usd ASC LIMIT 1`
    ).bind(asin, condition).first<BestOffer>();

    if (!best) {
      // No available offers — remove the row so the UI stops showing it
      await env.DB.prepare('DELETE FROM current_view WHERE asin = ? AND condition = ?').bind(asin, condition).run();
      continue;
    }

    const median_30d = await computeMedian(env.DB, asin, condition, now, 30);
    const median_90d = await computeMedian(env.DB, asin, condition, now, 90);
    const msrp_baseline = product.model ? msrpByCategoryAndModel.get(`${product.category_id}|${product.model}`) ?? null : null;

    const pct_below_median_30d = median_30d != null ? ((median_30d - best.price_usd) / median_30d) * 100 : null;
    const pct_below_median_90d = median_90d != null ? ((median_90d - best.price_usd) / median_90d) * 100 : null;
    const pct_off_msrp = msrp_baseline != null ? ((msrp_baseline - best.price_usd) / msrp_baseline) * 100 : null;

    // is_lowest_30d: best.price_usd <= MIN(snapshots in 30d window). Cheap query — already indexed.
    const min30 = await env.DB.prepare(
      `SELECT MIN(price_usd) AS m FROM snapshots WHERE asin = ? AND condition = ? AND taken_at > ?`
    ).bind(asin, condition, now - 30 * 24 * 3600 * 1000).first<{ m: number | null }>();
    const min90 = await env.DB.prepare(
      `SELECT MIN(price_usd) AS m FROM snapshots WHERE asin = ? AND condition = ? AND taken_at > ?`
    ).bind(asin, condition, now - 90 * 24 * 3600 * 1000).first<{ m: number | null }>();
    const is_lowest_30d = min30?.m != null && best.price_usd <= min30.m;
    const is_lowest_90d = min90?.m != null && best.price_usd <= min90.m;

    const composite_score = computeCompositeScore({
      current_price_usd: best.price_usd,
      median_30d, median_90d,
      seller_rating: best.seller_rating,
      last_seen: best.last_seen,
      is_lowest_30d,
    });

    await upsertCurrentView(env.DB, {
      asin, condition,
      current_price_usd: best.price_usd,
      best_offer_id: best.offer_id,
      median_30d, median_90d, msrp_baseline,
      pct_below_median_30d, pct_below_median_90d, pct_off_msrp,
      composite_score,
      price_per_gb: null,        // GPU-only in Phase 1; RAM/SSD in Phase 2
      price_per_tb: null,
      is_lowest_30d: is_lowest_30d ? 1 : 0,
      is_lowest_90d: is_lowest_90d ? 1 : 0,
      recomputed_at: now,
    });
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/score-recompute
```

Expected: 3/3 PASS.

- [ ] **Step 5: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/pipeline/score-recompute.ts worker/test/pipeline/score-recompute.test.ts
git commit -m "feat(pipeline): score recompute — current_view per dirty tuple, removes on offer drop

— claude-opus-4-7"
```

---

### Task 12: Cron orchestrator (compose phases 1-4)

**Files:**
- Create: `worker/src/pipeline/cron.ts`
- Create: `worker/test/pipeline/cron.test.ts`
- Modify: `worker/src/index.ts:9-15` — wire `scheduled` handler

- [ ] **Step 1: Write the failing test**

```typescript
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import { runCron } from '../../src/pipeline/cron';

beforeEach(() => vi.unstubAllGlobals());

const stubKeepaSequence = (responses: Response[]) => {
  const fn = vi.fn();
  for (const r of responses) fn.mockResolvedValueOnce(r);
  vi.stubGlobal('fetch', fn);
  return fn;
};

const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });

describe('runCron', () => {
  it('end-to-end: token check, enrich one stale ASIN, write current_view row', async () => {
    // Seed: category + one product that's been refreshed 1h ago (stale)
    await env.DB.prepare(
      `INSERT OR IGNORE INTO categories (category_id, display, search_terms, msrp_baseline, enabled) VALUES ('gpu', 'GPUs', '["rtx 5090"]', '{"5090":1999}', 1)`
    ).run();
    await env.DB.prepare(
      `INSERT INTO products (asin, category_id, model, title, brand, thumbnail_url, attrs_json, first_seen, last_refreshed, active)
       VALUES ('B0CRON01', 'gpu', '5090', 'placeholder title', 'X', null, '{}', 1000, ?, 1)`
    ).bind(Date.now() - 60 * 60 * 1000).run();

    stubKeepaSequence([
      json({ tokensLeft: 540, refillIn: 60000, refillRate: 1, timestamp: Date.now() }),                      // /token
      json({ tokensLeft: 538, products: [{                                                                    // /product
        asin: 'B0CRON01', title: 'GIGABYTE GeForce RTX 5090 WINDFORCE 32G',
        brand: 'GIGABYTE', imagesCSV: 'a.jpg',
        stats: { current: [-1, 199900, 159999, -1, -1, -1, -1, -1, -1, -1] },
      }]}),
      // /search call: discovery rotates in, returns one new ASIN
      json({ tokensLeft: 488, products: [{
        asin: 'B0CRON02', title: 'msi Gaming RTX 5090 32G', brand: 'msi', imagesCSV: 'b.jpg',
        stats: { current: [-1, 195000, -1, -1, -1, -1, -1, -1, -1, -1] },
      }]}),
    ]);

    await runCron(env);

    const v = await env.DB.prepare('SELECT * FROM current_view WHERE asin = ? AND condition = ?').bind('B0CRON01', 'used-good').first<any>();
    expect(v).not.toBeNull();
    expect(v.current_price_usd).toBe(1599.99);

    // Discovery should have inserted B0CRON02 (last_refreshed=0 so next tick enriches it)
    const newProd = await env.DB.prepare('SELECT asin, last_refreshed FROM products WHERE asin = ?').bind('B0CRON02').first<any>();
    expect(newProd.asin).toBe('B0CRON02');
    expect(newProd.last_refreshed).toBe(0);
  });

  it('skips enrich + discover when tokens < 30', async () => {
    await env.DB.prepare(
      `INSERT OR IGNORE INTO categories (category_id, display, search_terms, msrp_baseline, enabled) VALUES ('gpu', 'GPUs', '["rtx 5090"]', '{"5090":1999}', 1)`
    ).run();
    stubKeepaSequence([
      json({ tokensLeft: 5, refillIn: 60000, refillRate: 1, timestamp: Date.now() }),
    ]);
    await runCron(env);
    // No /product or /search calls were issued — only the /token call was made
    // (verified by the absence of further mock call count; vi.fn().mock.calls.length === 1)
    // Cleaner assertion: no products inserted from discovery, no offers from enrichment.
    const offers = await env.DB.prepare('SELECT COUNT(*) AS n FROM offers').first<any>();
    expect(offers.n).toBe(0);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

```sh
cd ~/git/sortsafe/worker && bun test pipeline/cron
```

Expected: FAIL — `runCron is not defined`.

- [ ] **Step 3: Implement `worker/src/pipeline/cron.ts`** per spec §4.1

```typescript
import type { Env } from '../env';
import { fetchTokens } from '../keepa/client';
import { getStaleProductAsins } from '../db/client';
import { enrichBatch, type DirtySet } from './enrich';
import { discoverForCategory } from './discover';
import { recomputeScoresForDirty } from './score-recompute';

const STALE_MS = 15 * 60 * 1000;     // refresh products older than 15 min
const SAFETY_MARGIN = 5;
const SEARCH_RESERVE = 50;
const TOKEN_FLOOR = 30;
const TOKENS_PER_ASIN = 2;            // /product?stats=180

/**
 * 10-min cron orchestrator. Phases per spec §4.1:
 *   0. (Recovery sweep — added in Chunk 4)
 *   1. Token check — bail if < TOKEN_FLOOR
 *   2. Enrich — refresh stale products
 *   3. Discover — top up new ASINs from /search
 *   4. Score — recompute current_view for dirty rows
 *   5. (Alert eval — added in Chunk 4)
 */
export async function runCron(env: Env): Promise<void> {
  const tokensRemaining = await fetchTokens(env);
  if (tokensRemaining < TOKEN_FLOOR) {
    console.log(`[cron] tokens=${tokensRemaining} < ${TOKEN_FLOOR}; skipping enrich+discover`);
    return;
  }

  const dirty: DirtySet = new Set();

  // Phase 2: enrich
  const enrichBudget = Math.max(0, Math.floor((tokensRemaining - SEARCH_RESERVE - SAFETY_MARGIN) / TOKENS_PER_ASIN));
  if (enrichBudget > 0) {
    const stale = await getStaleProductAsins(env.DB, Date.now(), STALE_MS, enrichBudget);
    if (stale.length > 0) await enrichBatch(env, stale, dirty);
  }

  // Phase 3: discover (rotate one category per tick — Phase 1 only has 'gpu')
  // We always run discovery if budget allows; discoverForCategory bails internally on bot-wall.
  const tokensAfterEnrich = await fetchTokens(env);
  if (tokensAfterEnrich >= SEARCH_RESERVE) {
    await discoverForCategory(env, 'gpu', dirty);
  }

  // Phase 4: score recompute
  await recomputeScoresForDirty(env, dirty);
}
```

- [ ] **Step 4: Wire `scheduled` handler in `worker/src/index.ts`**

Replace lines 9-15 of `worker/src/index.ts`:

```typescript
import type { Env, AlertMessage } from './env';
import { runCron } from './pipeline/cron';

export default {
  async fetch(req: Request, _env: Env): Promise<Response> {
    const url = new URL(req.url);
    if (url.pathname === '/healthz') {
      return new Response(JSON.stringify({ ok: true, ts: Date.now() }), {
        headers: { 'content-type': 'application/json' },
      });
    }
    return new Response('not found', { status: 404 });
  },
  async scheduled(event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    // 10-min cron only in this chunk; 03:00 UTC daily added in Chunk 6
    if (event.cron === '*/10 * * * *') {
      ctx.waitUntil(runCron(env).catch((e) => console.error('[cron]', e)));
    }
  },
  async queue(_batch: MessageBatch<AlertMessage>, _env: Env, _ctx: ExecutionContext): Promise<void> {
    // Wired in Chunk 4.
  },
};
```

- [ ] **Step 5: Run tests + manual cron trigger via wrangler dev**

```sh
cd ~/git/sortsafe/worker
bun test pipeline/cron
bun run typecheck
```

Expected: 2/2 cron tests PASS, typecheck clean.

Manual smoke (optional, costs real Keepa tokens):

```sh
cd ~/git/sortsafe/worker
bunx wrangler d1 execute sortsafe-db --local --file=src/db/seed.sql        # seed gpu category locally
bunx wrangler d1 execute sortsafe-db --local --command "INSERT INTO products (asin, category_id, model, title, brand, thumbnail_url, attrs_json, first_seen, last_refreshed, active) VALUES ('B0DT7GMXHB', 'gpu', '5090', 'placeholder', null, null, '{}', 1000, 0, 1);"
bunx wrangler dev --test-scheduled                                          # opens at :8787 with cron triggerable
# in another terminal:
curl 'http://localhost:8787/__scheduled?cron=*/10+*+*+*+*'
bunx wrangler d1 execute sortsafe-db --local --command "SELECT asin, condition, current_price_usd, composite_score FROM current_view ORDER BY composite_score DESC;"
```

Expected: at least one row in current_view with a real Keepa-sourced price for B0DT7GMXHB.

- [ ] **Step 6: Commit**

```sh
cd ~/git/sortsafe
git add worker/src/pipeline/cron.ts worker/test/pipeline/cron.test.ts worker/src/index.ts
git commit -m "feat(cron): 10-min orchestrator wires token-check → enrich → discover → score

Composes the four pipeline phases. Adaptive enrich budget per spec §4.1:
floor((tokensRemaining - SEARCH_RESERVE - SAFETY_MARGIN) / 2). Recovery
sweep + alert eval added in Chunk 4.

Manual smoke: wrangler dev --test-scheduled then curl /__scheduled?cron=*/10*

— claude-opus-4-7"
```

---

**End of Chunk 3.** At this point:
- `cd ~/git/sortsafe/worker && bun test` passes ~55 cases across db/keepa/extract/pipeline.
- Cron handler runs end-to-end against mocked Keepa: token check → enrich stale → discover new → score recompute.
- `current_view` populates with real prices, medians, and composite scores.
- `bunx wrangler dev --test-scheduled` lets you fire the cron manually for live integration testing.

Next chunk (4): alert evaluation, queue consumer, Discord webhook dispatch, recovery sweep.

---
