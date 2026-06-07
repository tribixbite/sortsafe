# Claude Code config — sortsafe

## Project purpose
Multi-category Amazon deal sniper (GPUs, RAM, SSDs). Migrated from `~/git/torch`'s `/gpus` subroute on 2026-05-04 so non-flashlight infrastructure can grow independently.

## Data integrity rules (inherited)
NEVER fabricate, infer, estimate, guess, or default data values. The seed file's authority is Keepa's API responses. Empty fields must stay empty.

## Tech stack
- SvelteKit + Tailwind v4 (CSS-first), `adapter-static` (SPA)
- Bun runtime; Termux-on-Android dev environment
- IndexedDB for browser-side state
- Keepa API (Power Plan, 600 tok/hr) for server-side seeding
- Planned: Cloudflare Worker as CORS proxy + Web Push, Discord webhook for alerts

## Termux specifics
- `bun run dev` goes through `scripts/vite-cli.ts` (avoids `node` shebang — Termux's bionic-linked node breaks native binary resolution)
- `postinstall.sh` symlinks `@esbuild/linux-arm64` → `android-arm64` and patches rollup
- `bun run check` is broken on Termux (esbuild EPIPE) — use `bunx tsc --noEmit --skipLibCheck`

## Cron
- `scripts/gpus-keepa-cron.sh` runs every 30 min via `crontab -e`
- Logs to `~/.cache/sortsafe-gpus-keepa.log`
- Lockfile prevents overlapping runs

## Catalog architecture (multi-category)
- Four categories — **GPU / RAM / SSD / CPU** — are driven by one registry,
  `src/lib/catalog/categories.ts` (slug, name, per-variant Amazon search term +
  title-match regex, accessory-reject regex, optional `referencePrices` for the
  "% off MSRP" metric — GPU only). Generic lib: `catalog/{types,db,scraper,refresh}.ts`.
- One dynamic route `src/routes/[category]/+page.svelte` renders any category from
  the config. Filters are **data-driven** (a filter only renders when the data
  supports it): rating filter hidden when no offer has a rating, condition filter
  hidden when only one condition is present, discount only when `referencePrices`
  exist (GPU). Cards show a `$/GB` (RAM) / `$/TB` (SSD) value badge + sort.
- Each category gets its own **IndexedDB `sortsafe-<slug>` (v2)**. The store is
  cleared on `onupgradeneeded` (v1→v2) and `hydrateFromSeed` stamps records
  `seeded:true` and drops the entire previous seed each load — otherwise a
  returning visitor keeps stale offers from an earlier seed whose ASINs aren't in
  the new one (this caused a "30-day-old GPU data" report). Bump `DB_VERSION` when
  the seed schema/source changes meaningfully.
- **Data sources:** RAM/SSD/CPU scraped from Amazon SEARCH via the worker
  (`node scripts/seed-via-worker.mjs ram ssd cpu`; title from `img.s-image` alt,
  price from `.a-price .a-offscreen`). **GPU is a hybrid:** worker discovers ASINs
  (free) → `bun scripts/gpus-seed-keepa.ts --no-search` Keepa-enriches them with
  per-condition (new/used/refurb/warehouse) pricing → then merge worker thumbnails
  back (Keepa returns no images for GPUs). Browser "Refresh from Amazon" does live
  PDP pulls via the worker. **Keepa key works** (~60 tok, refill ~1/min); see global
  CLAUDE.md "Keepa API" for token budgeting + the `$K` env-var gotcha.

## Proxy Worker
- `worker/index.mjs` — CORS fetch proxy (`/fetch?url=`), Amazon host-allowlisted,
  edge-cached. Live at `https://sortsafe-proxy.tribixbite.workers.dev`.
- Deploy: `node scripts/deploy-worker.mjs` (CF REST API — wrangler can't run on
  Termux; token from `~/.secrets` `CF-WORKER-KEY-SORTSAFE` or `$CLOUDFLARE_API_TOKEN`).
  CI mirror: `.github/workflows/deploy-worker.yml` (`CLOUDFLARE_API_TOKEN` secret).
- The scraper points prod fetches at this Worker (`src/lib/catalog/scraper.ts`).

## Deploy (GitHub Pages)
- Repo: `tribixbite/sortsafe`. Live at **sortsafe.com** (apex, Cloudflare-proxied → tribixbite.github.io).
- `.github/workflows/deploy.yml` builds on CI with **npm** (not the committed `bun.lock`):
  `npm install` → `npx svelte-kit sync` → `npx vite build` → `cp build/index.html build/404.html`
  → upload `build/` → deploy. npm auto-resolves the linux-x64 rollup/lightningcss natives;
  `postinstall.sh` is a no-op off Termux.
- `@rollup/rollup-android-arm64` lives in **`optionalDependencies`** so CI (linux-x64) skips it
  (else `npm install` fails `EBADPLATFORM`); Termux still installs it (matches platform).
- SPA deep links work via `build/404.html` (Pages serves it for unknown paths; the SPA boots
  and client-routes). Apex domain comes from `static/CNAME`.
- The static SPA hydrates from `static/gpus-seed.json` (no backend needed to be live). The
  Keepa/Cloudflare-Worker/cron/Discord pipeline (docs/superpowers/plans) is separate server work.

## Sibling site
The **tiny homes** directory (`~/git/tinyhomes`, repo `tribixbite/tinyhomes`) lives at
**tinyhomes.sortsafe.com** — a separate static site, not part of this repo.

## TODO / next (as of 2026-06-07)
- **Finish GPU Keepa enrichment**: only 30/43 ASINs enriched before the 60-token
  budget; re-run `bun scripts/gpus-seed-keepa.ts --no-search` (then merge worker
  thumbnails back) as tokens refill to cover the rest.
- **Best Keepa win, not yet built**: extract 180-day `stats.avg`/`stats.min` and
  show "X% below 180-day avg" as the deal signal (far better than vs-static-MSRP).
  Needs a per-offer `ref` field + a generic UI badge/sort.
- **Make a clean combined GPU seeder** (`worker-discover → keepa-enrich → merge
  thumbnails → write`) so a *gentle* cron (every ~2h, `--no-search`) can run
  without regressing thumbnails. Do NOT re-enable the old `*/30` `gpus-keepa-cron.sh`
  (it overwrites thumbnails). Both keepa crons are commented out in `crontab -l`.
- **Amazon throttle**: SSD/CPU were partially bot-walled during the 2026-06-07
  re-seed (kept the freshest prior listings); re-seed fully when Amazon cools.
- Keepa account had `tokenFlowReduction` ~0.31 (overdraft penalty, recovering) —
  watch it return toward 0 now that the `*/5` torch cron is disabled.
- Two stray cfc tabs left open in the user's Chrome (`948725717`, `948726272`).

## In-progress
See `docs/superpowers/specs/` for the original design doc (Keepa pipeline + Worker backend).
