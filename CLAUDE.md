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
  the config (variant/condition/price/rating filters, discount only when a
  reference price exists). Each category gets its own IndexedDB (`sortsafe-<slug>`).
- **Data: scraped from Amazon via the Cloudflare Worker proxy, not Keepa.** The
  Keepa key in `.env` currently returns `invalidParameter`/0 tokens (dead). Seeds:
  `node scripts/seed-via-worker.mjs ram ssd cpu` scrapes Amazon SEARCH pages
  through the worker (title from `img.s-image` alt, price from `.a-price .a-offscreen`)
  → `static/<slug>-seed.json`. GPU keeps its richer Keepa-era `gpus-seed.json`
  (per-condition offers). The browser "Refresh from Amazon" does live PDP pulls.

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

## In-progress
See `docs/superpowers/specs/` for the active design doc (Keepa pipeline + Worker backend).
