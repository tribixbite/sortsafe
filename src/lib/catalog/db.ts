/**
 * Per-category IndexedDB store factory. Each category gets its own database
 * (`sortsafe-<category>`) with offers / products / snapshots object stores.
 */
import type { Offer, Product, PriceSnapshot, SeedFile } from "./types";

type StoreName = "offers" | "products" | "snapshots";

export interface CatalogDb {
  putOffers(offers: Offer[]): Promise<void>;
  putProduct(p: Product): Promise<void>;
  getAllOffers(): Promise<Offer[]>;
  getAllProducts(): Promise<Product[]>;
  appendSnapshots(snaps: PriceSnapshot[]): Promise<void>;
  hydrateFromSeed(seedUrl: string): Promise<{ inserted: number; deleted: number } | null>;
}

export function catalogDb(category: string): CatalogDb {
  const DB_NAME = `sortsafe-${category}`;
  const DB_VERSION = 1;
  let openPromise: Promise<IDBDatabase> | null = null;

  function open(): Promise<IDBDatabase> {
    if (openPromise) return openPromise;
    openPromise = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains("offers")) {
          const s = db.createObjectStore("offers", { keyPath: "offer_id" });
          s.createIndex("asin", "asin");
          s.createIndex("variant", "variant");
          s.createIndex("condition", "condition");
          s.createIndex("last_seen", "last_seen");
        }
        if (!db.objectStoreNames.contains("products")) {
          db.createObjectStore("products", { keyPath: "asin" });
        }
        if (!db.objectStoreNames.contains("snapshots")) {
          const s = db.createObjectStore("snapshots", { keyPath: "id", autoIncrement: true });
          s.createIndex("asin", "asin");
          s.createIndex("taken_at", "taken_at");
        }
      };
      req.onerror = () => reject(req.error);
      req.onsuccess = () => resolve(req.result);
    });
    return openPromise;
  }

  async function tx(stores: StoreName[], mode: IDBTransactionMode = "readonly") {
    return (await open()).transaction(stores, mode);
  }
  function done(t: IDBTransaction): Promise<void> {
    return new Promise((resolve, reject) => {
      t.oncomplete = () => resolve();
      t.onerror = () => reject(t.error);
      t.onabort = () => reject(t.error);
    });
  }
  function getAll<T>(store: StoreName): Promise<T[]> {
    return new Promise((resolve, reject) => {
      open().then((db) => {
        const req = db.transaction(store).objectStore(store).getAll();
        req.onsuccess = () => resolve(req.result as T[]);
        req.onerror = () => reject(req.error);
      }, reject);
    });
  }

  async function putOffers(offers: Offer[]) {
    if (!offers.length) return;
    const t = await tx(["offers"], "readwrite");
    const s = t.objectStore("offers");
    for (const o of offers) s.put(o);
    await done(t);
  }
  async function putProduct(p: Product) {
    const t = await tx(["products"], "readwrite");
    t.objectStore("products").put(p);
    await done(t);
  }
  async function appendSnapshots(snaps: PriceSnapshot[]) {
    if (!snaps.length) return;
    const t = await tx(["snapshots"], "readwrite");
    const s = t.objectStore("snapshots");
    for (const x of snaps) s.add(x);
    await done(t);
  }

  /** Replace all offers for each ASIN present in the seed (so sold/stale
   *  listings vanish), then insert the fresh seed. Legacy GPU seeds use a
   *  `model` field — normalize it to `variant` + stamp `category`. */
  async function hydrateFromSeed(seedUrl: string) {
    let res: Response;
    try {
      res = await fetch(seedUrl, { cache: "no-store" });
    } catch {
      return null;
    }
    if (!res.ok) return null;
    const seed = (await res.json()) as Partial<SeedFile> & {
      offers?: (Offer & { model?: string })[];
      products?: (Product & { model?: string })[];
    };
    if (!seed?.offers?.length) return null;

    const norm = <T extends { model?: string; variant?: string; category?: string }>(x: T): T => {
      x.variant = x.variant ?? x.model ?? "";
      x.category = x.category ?? category;
      return x;
    };
    const offers = seed.offers.map(norm) as Offer[];
    const products = (seed.products ?? []).map(norm) as Product[];

    const seedAsins = new Set<string>();
    for (const o of offers) seedAsins.add(o.asin);
    for (const p of products) seedAsins.add(p.asin);

    let deleted = 0;
    if (seedAsins.size) {
      const t = await tx(["offers"], "readwrite");
      const idx = t.objectStore("offers").index("asin");
      await new Promise<void>((resolve, reject) => {
        let pending = seedAsins.size;
        for (const asin of seedAsins) {
          const req = idx.openCursor(IDBKeyRange.only(asin));
          req.onsuccess = () => {
            const cur = req.result;
            if (cur) {
              cur.delete();
              deleted++;
              cur.continue();
            } else if (--pending === 0) resolve();
          };
          req.onerror = () => reject(req.error);
        }
      });
      await done(t);
    }

    await putOffers(offers);
    if (products.length) {
      const t = await tx(["products"], "readwrite");
      for (const p of products) t.objectStore("products").put(p);
      await done(t);
    }
    return { inserted: offers.length, deleted };
  }

  return {
    putOffers,
    putProduct,
    getAllOffers: () => getAll<Offer>("offers"),
    getAllProducts: () => getAll<Product>("products"),
    appendSnapshots,
    hydrateFromSeed,
  };
}
