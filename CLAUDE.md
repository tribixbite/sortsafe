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

## In-progress
See `docs/superpowers/specs/` for the active design doc.
