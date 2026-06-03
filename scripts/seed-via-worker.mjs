/**
 * Seed a category's static JSON by scraping Amazon SEARCH pages through the
 * deployed Cloudflare Worker proxy (no Keepa, no browser). Search results carry
 * ASIN + title + price + thumbnail, so one search per variant yields real
 * priced offers with images — enough for a first-load directory. The browser's
 * "Refresh from Amazon" does the deeper live pull.
 *
 *   node scripts/seed-via-worker.mjs ram ssd cpu      # seed these
 *   node scripts/seed-via-worker.mjs gpus             # (optional) refresh gpus
 *
 * Output: static/<slug>-seed.json  { generated_at, offers, products }
 */
import { writeFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "node-html-parser";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const PROXY = "https://sortsafe-proxy.tribixbite.workers.dev/fetch";
const PAGES = 2;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Mirrors src/lib/catalog/categories.ts (kept compact for the node seeder).
const CATS = {
  gpus: {
    accessory: /backplate|water\s*block|waterblock|fan only|^cable|riser|bracket|mount adapter|gpu support|anti.?sag|holder/i,
    variants: [
      { id: "5090", search: "rtx 5090", match: /\b(rtx|geforce)\s*5090\b/i },
      { id: "4090", search: "rtx 4090", match: /\b(rtx|geforce)\s*4090\b/i },
      { id: "3090", search: "rtx 3090", match: /\b(rtx|geforce)\s*3090\b/i },
    ],
  },
  ram: {
    accessory: /heat\s*sink only|rgb fan|cooler|^cable|adapter|so-?dimm|laptop|server ecc|fan kit/i,
    variants: [
      { id: "DDR5", search: "ddr5 desktop ram kit", match: /\bddr5\b/i },
      { id: "DDR4", search: "ddr4 desktop ram kit", match: /\bddr4\b/i },
    ],
  },
  ssd: {
    accessory: /enclosure|heat\s*sink only|adapter|^cable|dock|caddy|bracket|screw/i,
    variants: [
      { id: "4TB", search: "nvme ssd 4tb", match: /\b4\s*tb\b/i },
      { id: "2TB", search: "nvme ssd 2tb", match: /\b2\s*tb\b/i },
      { id: "1TB", search: "nvme ssd 1tb", match: /\b1\s*tb\b/i },
    ],
  },
  cpu: {
    accessory: /cpu cooler|air cooler|liquid cooler|\baio\b|thermal (paste|compound)|heat\s*sink|water\s*block|motherboard|\bmobo\b/i,
    variants: [
      { id: "Ryzen", search: "amd ryzen desktop processor", match: /\bryzen\b/i },
      { id: "Core", search: "intel core desktop processor", match: /\bintel\s+core|\bcore\s?(ultra|i[3579])\b/i },
    ],
  },
};

function classify(cfg, title) {
  if (!title || cfg.accessory.test(title)) return null;
  for (const v of cfg.variants) if (v.match.test(title)) return v.id;
  return null;
}

function priceUsd(s) {
  if (!s) return null;
  const m = s.replace(/,/g, "").match(/\$?([\d.]+)/);
  const n = m ? parseFloat(m[1]) : NaN;
  return Number.isFinite(n) && n > 0 ? n : null;
}

async function fetchSearch(term, page) {
  let url = `https://www.amazon.com/s?k=${encodeURIComponent(term)}`;
  if (page > 1) url += `&page=${page}`;
  const r = await fetch(`${PROXY}?url=${encodeURIComponent(url)}`);
  if (!r.ok) throw new Error(`proxy ${r.status}`);
  return await r.text();
}

async function seedCategory(slug) {
  const cfg = CATS[slug];
  if (!cfg) throw new Error(`unknown category ${slug}`);
  const now = Date.now();
  const offersByAsin = new Map();
  const products = new Map();

  for (const v of cfg.variants) {
    let kept = 0;
    for (let p = 1; p <= PAGES; p++) {
      let html;
      try {
        html = await fetchSearch(v.search, p);
      } catch (e) {
        console.warn(`  ${slug}/${v.id} p${p}: ${e.message}`);
        continue;
      }
      const root = parse(html);
      for (const el of root.querySelectorAll('[data-component-type="s-search-result"]')) {
        const asin = el.getAttribute("data-asin");
        if (!asin || asin.length !== 10 || offersByAsin.has(asin)) continue;
        // Amazon splits the title across two <h2>s (brand + product line); the
        // result image's alt text is the full clean title.
        const img = el.querySelector("img.s-image");
        const title = (
          img?.getAttribute("alt") ||
          el.querySelector("[data-cy=title-recipe]")?.text ||
          el.querySelectorAll("h2").map((h) => h.text).join(" ") ||
          ""
        )
          .replace(/^sponsored(\s+ad)?\s*[-:]\s*/i, "")
          .trim();
        const variant = classify(cfg, title);
        if (variant !== v.id) continue; // wrong variant / accessory
        const price = priceUsd(el.querySelector(".a-price .a-offscreen")?.text);
        if (price == null) continue;
        const thumb = img?.getAttribute("src") || null;
        offersByAsin.set(asin, {
          offer_id: `${asin}__new_1`,
          asin,
          category: slug,
          variant,
          title: title.slice(0, 300),
          condition: "new",
          condition_note: "lowest listed (Amazon search)",
          price_usd: price,
          currency: "USD",
          seller: null,
          seller_id: null,
          seller_rating: null,
          seller_rating_count: null,
          ships_from: null,
          delivery_text: null,
          first_seen: now,
          last_seen: now,
          is_buybox: true,
        });
        products.set(asin, {
          asin,
          category: slug,
          variant,
          title: title.slice(0, 300),
          thumbnail_url: thumb,
          last_refreshed: now,
        });
        kept++;
      }
      await sleep(500);
    }
    console.log(`  ${slug}/${v.id}: ${kept} offers`);
  }

  const offers = [...offersByAsin.values()];
  const out = { generated_at: now, offers, products: [...products.values()] };
  const path = resolve(ROOT, `static/${slug}-seed.json`);
  writeFileSync(path, JSON.stringify(out, null, 2));
  console.log(`✓ ${slug}: ${offers.length} offers, ${products.size} products → static/${slug}-seed.json`);
}

async function main() {
  const slugs = process.argv.slice(2);
  if (!slugs.length) {
    console.error("usage: node scripts/seed-via-worker.mjs <slug...>  (ram ssd cpu gpus)");
    process.exit(1);
  }
  for (const s of slugs) await seedCategory(s);
}
main().catch((e) => {
  console.error(e);
  process.exit(1);
});
