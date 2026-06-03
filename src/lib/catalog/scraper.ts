/**
 * Browser-side Amazon scraper, category-generic. All fetches go through the
 * Cloudflare Worker proxy (sortsafe-proxy) which adds CORS + browser headers.
 *
 * The PDP per-condition accordion is JS-only, so a static fetch yields the
 * buy-box / lead price per ASIN — enough for deal surfacing.
 */
import type { CategoryConfig } from "./categories";
import { classify } from "./categories";
import type { Condition, Offer, Product } from "./types";

const PROXY_URL =
  typeof location !== "undefined" && location.hostname === "localhost"
    ? "http://localhost:8787/fetch"
    : "https://sortsafe-proxy.tribixbite.workers.dev/fetch";

interface Opts {
  signal?: AbortSignal;
  pages?: number;
}

async function fetchHtml(url: string, opts: Opts = {}): Promise<Document> {
  const res = await fetch(`${PROXY_URL}?url=${encodeURIComponent(url)}`, { signal: opts.signal });
  if (!res.ok) throw new Error(`proxy ${res.status} for ${url}`);
  return new DOMParser().parseFromString(await res.text(), "text/html");
}

function parsePriceUsd(s: string | null | undefined): number | null {
  if (!s) return null;
  const m = s.replace(/,/g, "").match(/\$?([\d.]+)/);
  const n = m ? parseFloat(m[1]) : NaN;
  return Number.isFinite(n) ? n : null;
}

function classifyCondition(label: string | null | undefined): Condition {
  if (!label) return "unknown";
  const l = label.toLowerCase();
  if (l.includes("amazon warehouse")) return "warehouse";
  if (l.includes("refurbished") || l.includes("renewed")) return "refurbished";
  if (l.includes("used") && l.includes("like new")) return "used-like-new";
  if (l.includes("used") && l.includes("very good")) return "used-very-good";
  if (l.includes("used") && l.includes("acceptable")) return "used-acceptable";
  if (l.includes("used")) return "used-good";
  if (l.includes("new")) return "new";
  return "unknown";
}

export interface DiscoverResult {
  asin: string;
  titleHint: string;
}

export async function discoverAsins(searchTerm: string, opts: Opts = {}): Promise<DiscoverResult[]> {
  const pages = Math.max(1, opts.pages ?? 1);
  const seen = new Map<string, DiscoverResult>();
  for (let page = 1; page <= pages; page++) {
    let url = `https://www.amazon.com/s?k=${encodeURIComponent(searchTerm)}`;
    if (page > 1) url += `&page=${page}`;
    let doc: Document;
    try {
      doc = await fetchHtml(url, opts);
    } catch (e) {
      console.warn(`discover "${searchTerm}" p${page} failed`, e);
      continue;
    }
    doc.querySelectorAll('[data-asin][data-component-type="s-search-result"]').forEach((el) => {
      const asin = (el as HTMLElement).dataset.asin;
      if (!asin || asin.length !== 10 || seen.has(asin)) return;
      const title =
        (el.querySelector("h2 span") as HTMLElement | null)?.textContent ??
        (el.querySelector("h2") as HTMLElement | null)?.textContent ??
        "";
      seen.set(asin, { asin, titleHint: title.trim().slice(0, 200) });
    });
  }
  return [...seen.values()];
}

function productMetaFromDoc(doc: Document, asin: string, cfg: CategoryConfig, variant: string): Product {
  const title = (doc.querySelector("#productTitle") as HTMLElement | null)?.textContent?.trim() ?? "";
  const img = doc.querySelector("#landingImage, #imgTagWrapperId img") as HTMLImageElement | null;
  const thumb = img?.getAttribute("data-old-hires") ?? img?.getAttribute("src") ?? null;
  return { asin, category: cfg.slug, variant, title, thumbnail_url: thumb, last_refreshed: Date.now() };
}

function offersFromDoc(doc: Document, asin: string, cfg: CategoryConfig, variant: string, title: string): Offer[] {
  const out: Offer[] = [];
  const now = Date.now();
  const base = (condition: Condition, price: number, seller: string | null, sellerId: string | null): Offer => ({
    offer_id: `${asin}__${condition}_${out.length + 1}`,
    asin,
    category: cfg.slug,
    variant,
    title,
    condition,
    condition_note: null,
    price_usd: price,
    currency: "USD",
    seller,
    seller_id: sellerId,
    seller_rating: null,
    seller_rating_count: null,
    ships_from: null,
    delivery_text: null,
    first_seen: now,
    last_seen: now,
    is_buybox: false,
  });

  const rows = doc.querySelectorAll(
    '[id^="newAccordionRow_"], [id^="usedAccordionRow_"], [id^="refurbishedAccordionRow_"]',
  );
  rows.forEach((row) => {
    const id = (row as HTMLElement).id;
    let condition: Condition = id.startsWith("new")
      ? "new"
      : id.startsWith("used")
        ? "used-good"
        : id.startsWith("refurb")
          ? "refurbished"
          : "unknown";
    const price = parsePriceUsd((row.querySelector(".a-offscreen") as HTMLElement | null)?.textContent ?? "");
    if (price == null) return;
    const label = (row.querySelector(".a-color-base, span") as HTMLElement | null)?.textContent?.trim();
    const detailed = classifyCondition(label ?? id);
    if (detailed !== "unknown") condition = detailed;
    const link = row.querySelector('a[href*="seller="]') as HTMLAnchorElement | null;
    const sellerId = link?.getAttribute("href")?.match(/seller=([A-Z0-9]+)/)?.[1] ?? null;
    out.push(base(condition, price, link?.textContent?.trim() ?? null, sellerId));
  });

  if (out.length === 0) {
    const bb = parsePriceUsd(
      (
        doc.querySelector(
          "#corePrice_feature_div .a-offscreen, #corePriceDisplay_desktop_feature_div .a-offscreen",
        ) as HTMLElement | null
      )?.textContent ?? "",
    );
    if (bb != null) {
      const o = base("new", bb, null, null);
      o.is_buybox = true;
      out.push(o);
    }
  }
  return out;
}

export async function fetchProductWithOffers(
  asin: string,
  cfg: CategoryConfig,
  opts: Opts = {},
): Promise<{ product: Product; offers: Offer[] } | null> {
  const doc = await fetchHtml(`https://www.amazon.com/dp/${asin}`, opts);
  const title = (doc.querySelector("#productTitle") as HTMLElement | null)?.textContent?.trim() ?? "";
  const variant = classify(cfg, title);
  if (!variant) return null; // not a tracked product (wrong variant / accessory)
  const product = productMetaFromDoc(doc, asin, cfg, variant);
  const offers = offersFromDoc(doc, asin, cfg, variant, title);
  return offers.length ? { product, offers } : null;
}

export { PROXY_URL };
