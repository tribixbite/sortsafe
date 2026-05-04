# sortsafe

Real-time Amazon deal sniper for **GPUs**, **RAM**, and **SSDs**. Watches used / refurbished / Amazon Warehouse listings, surfaces the best current offers, alerts when prices cross thresholds.

Migrated from the `/gpus` subroute in [`tribixbite/torch`](https://github.com/tribixbite/torch) — same code, dedicated repo so non-flashlight infrastructure (Cloudflare Worker, web push, Discord webhooks, multi-category scoring) can grow without crowding the flashlight project.

## Architecture (current state — pre-redesign)

- **Server-side**: cron-scheduled Keepa API seeder (`scripts/gpus-keepa-cron.sh`, every 30 min) — fetches per-condition current pricing for known ASINs, discovers new ones via search rotation, writes `static/gpus-seed.json`.
- **Client-side**: SvelteKit SPA at `/gpus` hydrates from the seed on every load (delete-and-replace per ASIN so dead listings vanish), supports filters by model, condition, % off MSRP, seller rating.
- **Local proxy** (`scripts/gpu-proxy-local.ts`): CORS-bypass for browser-side Amazon scraping (planned to move to a Cloudflare Worker, with browsers as the actual scrapers — visitor IPs spread the bot-detection load).

## Pending redesign

See `docs/superpowers/specs/` for the in-progress design doc (multi-category scoring, deal alerts via Web Push + Discord, browser-side scraper offloading, etc.).

## Setup

```sh
bun install
echo "KEEPA_API_KEY=your_key_here" > .env
bun run dev
```

Visit <http://localhost:5173/gpus>.

## Scripts

- `bun run dev` — dev server (`:5173`)
- `bun run build` — adapter-static build
- `bun run seed:gpus` — manual Keepa seed refresh
- `bun run proxy` — local CORS proxy on `:8787`
