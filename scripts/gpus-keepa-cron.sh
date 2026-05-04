#!/bin/bash
# /gpus Keepa seeder cron wrapper.
# Suggested crontab (every 30 min — Keepa Power Plan refills 600 tokens/hour
# cap, so this averages ~30-100 token consumption per run, never starving):
#   */30 * * * * /data/data/com.termux/files/home/git/sortsafe/scripts/gpus-keepa-cron.sh
#
# Each run: refresh ALL existing seed ASINs via /product?stats=180 (1 token
# each), and if any model is below 20 entries, top up via /search.

set -o pipefail

PROJECT="/data/data/com.termux/files/home/git/sortsafe"
BUN="/data/data/com.termux/files/home/.bun/bin/bun"
LOCKFILE="/data/data/com.termux/files/home/.cache/sortsafe-gpus-keepa.lock"
LOGFILE="/data/data/com.termux/files/home/.cache/sortsafe-gpus-keepa.log"
MAX_LOG_KB=512

# Pre-flight: bun runnable
if ! "$BUN" --version >/dev/null 2>&1; then
    echo "$(date): FATAL — bun not working" >> "$LOGFILE"
    exit 1
fi

# Log rotation
if [ -f "$LOGFILE" ]; then
    sz=$(du -k "$LOGFILE" 2>/dev/null | cut -f1)
    if [ "${sz:-0}" -gt "$MAX_LOG_KB" ]; then
        mv "$LOGFILE" "${LOGFILE}.old"
        echo "$(date): rotated (${sz}KB)" > "$LOGFILE"
    fi
fi

# Lock so two crons can't stomp the seed file
exec 9>"$LOCKFILE"
if ! flock -n 9; then
    echo "$(date): another run holding lock — skipping" >> "$LOGFILE"
    exit 0
fi

cd "$PROJECT" || { echo "$(date): chdir failed" >> "$LOGFILE"; exit 1; }
echo "===== $(date) =====" >> "$LOGFILE"
"$BUN" scripts/gpus-seed-keepa.ts >> "$LOGFILE" 2>&1
echo "exit=$?" >> "$LOGFILE"
