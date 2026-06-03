/**
 * Deploy worker/index.mjs to Cloudflare Workers via the REST API.
 *
 * wrangler can't run on Termux (workerd has no android-arm64 build), so we use
 * the Workers Script Upload API directly. Token comes from ~/.secrets
 * (`export CF-WORKER-KEY-SORTSAFE=…`) or $CLOUDFLARE_API_TOKEN.
 *
 *   node scripts/deploy-worker.mjs            # deploy
 *
 * Prints the live https://<name>.<subdomain>.workers.dev URL.
 */
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const NAME = "sortsafe-proxy";
const SCRIPT = resolve(ROOT, "worker/index.mjs");
const API = "https://api.cloudflare.com/client/v4";

function token() {
  const env = process.env.CF_WORKER_KEY_SORTSAFE || process.env.CLOUDFLARE_API_TOKEN;
  if (env) return env.trim();
  // Fallback: read from ~/.secrets (accepts the dash or underscore var name).
  try {
    const sec = readFileSync(resolve(homedir(), ".secrets"), "utf8");
    const m = sec.match(/CF[_-]WORKER[_-]KEY[_-]SORTSAFE=([^\s"']+)/);
    if (m) return m[1].trim();
  } catch {
    /* no secrets file */
  }
  throw new Error("CF token not found in $CF_WORKER_KEY_SORTSAFE / $CLOUDFLARE_API_TOKEN / ~/.secrets");
}

async function cf(path, opts, tok) {
  const r = await fetch(`${API}${path}`, {
    ...opts,
    headers: { Authorization: `Bearer ${tok}`, ...(opts.headers || {}) },
  });
  const j = await r.json().catch(() => ({}));
  if (!j.success) throw new Error(`${path} -> ${r.status} ${JSON.stringify(j.errors || j)}`);
  return j.result;
}

async function main() {
  const tok = token();
  const [acct] = await cf("/accounts", {}, tok);
  const accountId = acct.id;
  const sub = await cf(`/accounts/${accountId}/workers/subdomain`, {}, tok);
  console.log(`account ${accountId} · subdomain ${sub.subdomain}`);

  const source = readFileSync(SCRIPT, "utf8");
  const form = new FormData();
  form.set(
    "metadata",
    new Blob([JSON.stringify({ main_module: "index.mjs", compatibility_date: "2024-12-01" })], {
      type: "application/json",
    }),
  );
  form.set(
    "index.mjs",
    new Blob([source], { type: "application/javascript+module" }),
    "index.mjs",
  );

  await cf(`/accounts/${accountId}/workers/scripts/${NAME}`, { method: "PUT", body: form }, tok);
  console.log(`✓ uploaded script "${NAME}"`);

  // Enable the workers.dev route for this script.
  await cf(
    `/accounts/${accountId}/workers/scripts/${NAME}/subdomain`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ enabled: true }),
    },
    tok,
  );
  const liveUrl = `https://${NAME}.${sub.subdomain}.workers.dev`;
  console.log(`✓ live: ${liveUrl}/fetch?url=…`);
}

main().catch((e) => {
  console.error(String(e));
  process.exit(1);
});
