/**
 * Orchestrates a full category refresh through the proxy:
 *   1. discover ASINs per variant search term
 *   2. fetch each PDP (bounded concurrency), classify + extract offers
 *   3. persist offers + products + snapshots
 *
 * navigator.locks keeps one refresh per category even across tabs.
 */
import type { CategoryConfig } from "./categories";
import { discoverAsins, fetchProductWithOffers } from "./scraper";
import type { CatalogDb } from "./db";
import type { Offer, PriceSnapshot } from "./types";

export interface RefreshProgress {
  phase: "idle" | "discovering" | "enriching" | "done" | "error";
  variant?: string;
  asin?: string;
  asinsTotal?: number;
  asinsDone?: number;
  offersInserted?: number;
  error?: string;
}

const DISCOVERY_PAGES = 2;
const DISCOVERY_DELAY_MS = 1500;
const CONCURRENCY = 4;
const STAGGER_MS = 500;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function snapshotsFromOffers(offers: Offer[]): PriceSnapshot[] {
  const lowest = new Map<string, Offer>();
  for (const o of offers) {
    const k = `${o.asin}__${o.condition}`;
    const cur = lowest.get(k);
    if (!cur || o.price_usd < cur.price_usd) lowest.set(k, o);
  }
  const now = Date.now();
  return [...lowest.values()].map((o) => ({
    asin: o.asin,
    condition: o.condition,
    price_usd: o.price_usd,
    taken_at: now,
  }));
}

export async function refreshAll(
  cfg: CategoryConfig,
  db: CatalogDb,
  onProgress: (p: RefreshProgress) => void = () => {},
  signal?: AbortSignal,
): Promise<RefreshProgress> {
  return await navigator.locks.request(`refresh-${cfg.slug}`, async () => {
    try {
      let totalOffers = 0;
      const asins = new Set<string>();
      for (const v of cfg.variants) {
        if (signal?.aborted) throw new Error("aborted");
        onProgress({ phase: "discovering", variant: v.id });
        try {
          const found = await discoverAsins(v.search, { signal, pages: DISCOVERY_PAGES });
          for (const f of found) asins.add(f.asin);
        } catch (e) {
          console.warn(`discover ${v.id} failed`, e);
        }
        await sleep(DISCOVERY_DELAY_MS);
      }

      const queue = [...asins];
      const asinsTotal = queue.length;
      let asinsDone = 0;
      let cursor = 0;

      async function worker(workerId: number) {
        await sleep(workerId * STAGGER_MS);
        while (true) {
          if (signal?.aborted) throw new Error("aborted");
          const idx = cursor++;
          if (idx >= queue.length) return;
          const asin = queue[idx];
          onProgress({ phase: "enriching", asin, asinsTotal, asinsDone, offersInserted: totalOffers });
          try {
            const result = await fetchProductWithOffers(asin, cfg, { signal });
            if (result) {
              await db.putOffers(result.offers);
              await db.putProduct(result.product);
              await db.appendSnapshots(snapshotsFromOffers(result.offers));
              totalOffers += result.offers.length;
            }
          } catch (e) {
            console.warn(`enrich ${asin} failed`, e);
          }
          asinsDone++;
          await sleep(STAGGER_MS * CONCURRENCY);
        }
      }
      await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i)));

      const final: RefreshProgress = { phase: "done", asinsTotal, asinsDone, offersInserted: totalOffers };
      onProgress(final);
      return final;
    } catch (e) {
      const final: RefreshProgress = { phase: "error", error: (e as Error).message };
      onProgress(final);
      return final;
    }
  });
}
