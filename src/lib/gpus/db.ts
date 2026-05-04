/**
 * IndexedDB wrapper for the /gpus pipeline. Native IDB to avoid pulling
 * in a dependency for what is a pretty narrow set of object stores.
 */
import type { GpuOffer, GpuProduct, PriceSnapshot, ModelMsrp, GpuModel } from './types';
import { DEFAULT_MSRP } from './types';

const DB_NAME = 'gpus';
const DB_VERSION = 1;

export type StoreName = 'offers' | 'products' | 'snapshots' | 'msrp';

let openPromise: Promise<IDBDatabase> | null = null;

function open(): Promise<IDBDatabase> {
	if (openPromise) return openPromise;
	openPromise = new Promise((resolve, reject) => {
		const req = indexedDB.open(DB_NAME, DB_VERSION);
		req.onupgradeneeded = () => {
			const db = req.result;
			if (!db.objectStoreNames.contains('offers')) {
				const s = db.createObjectStore('offers', { keyPath: 'offer_id' });
				s.createIndex('asin', 'asin');
				s.createIndex('model', 'model');
				s.createIndex('condition', 'condition');
				s.createIndex('last_seen', 'last_seen');
			}
			if (!db.objectStoreNames.contains('products')) {
				db.createObjectStore('products', { keyPath: 'asin' });
			}
			if (!db.objectStoreNames.contains('snapshots')) {
				const s = db.createObjectStore('snapshots', { keyPath: 'id', autoIncrement: true });
				s.createIndex('asin', 'asin');
				s.createIndex('taken_at', 'taken_at');
			}
			if (!db.objectStoreNames.contains('msrp')) {
				const s = db.createObjectStore('msrp', { keyPath: 'model' });
				// seed defaults
				for (const m of Object.keys(DEFAULT_MSRP) as GpuModel[]) {
					s.put({ model: m, msrp_usd: DEFAULT_MSRP[m], source: 'launch' });
				}
			}
		};
		req.onerror = () => reject(req.error);
		req.onsuccess = () => resolve(req.result);
	});
	return openPromise;
}

async function tx(stores: StoreName[], mode: IDBTransactionMode = 'readonly'): Promise<IDBTransaction> {
	const db = await open();
	return db.transaction(stores, mode);
}

function done(t: IDBTransaction): Promise<void> {
	return new Promise((resolve, reject) => {
		t.oncomplete = () => resolve();
		t.onerror = () => reject(t.error);
		t.onabort = () => reject(t.error);
	});
}

export async function putOffers(offers: GpuOffer[]): Promise<void> {
	if (!offers.length) return;
	const t = await tx(['offers'], 'readwrite');
	const store = t.objectStore('offers');
	for (const o of offers) store.put(o);
	await done(t);
}

export async function putProduct(p: GpuProduct): Promise<void> {
	const t = await tx(['products'], 'readwrite');
	t.objectStore('products').put(p);
	await done(t);
}

export async function getAllOffers(): Promise<GpuOffer[]> {
	const t = await tx(['offers']);
	return new Promise((resolve, reject) => {
		const req = t.objectStore('offers').getAll();
		req.onsuccess = () => resolve(req.result as GpuOffer[]);
		req.onerror = () => reject(req.error);
	});
}

export async function getAllProducts(): Promise<GpuProduct[]> {
	const t = await tx(['products']);
	return new Promise((resolve, reject) => {
		const req = t.objectStore('products').getAll();
		req.onsuccess = () => resolve(req.result as GpuProduct[]);
		req.onerror = () => reject(req.error);
	});
}

export async function appendSnapshots(snaps: PriceSnapshot[]): Promise<void> {
	if (!snaps.length) return;
	const t = await tx(['snapshots'], 'readwrite');
	const store = t.objectStore('snapshots');
	for (const s of snaps) store.add(s);
	await done(t);
}

export async function getMsrp(): Promise<Record<GpuModel, number>> {
	const t = await tx(['msrp']);
	return new Promise((resolve, reject) => {
		const req = t.objectStore('msrp').getAll();
		req.onsuccess = () => {
			const out: Record<string, number> = {};
			for (const m of req.result as ModelMsrp[]) out[m.model] = m.msrp_usd;
			resolve(out as Record<GpuModel, number>);
		};
		req.onerror = () => reject(req.error);
	});
}

export async function setMsrp(model: GpuModel, msrp: number): Promise<void> {
	const t = await tx(['msrp'], 'readwrite');
	t.objectStore('msrp').put({ model, msrp_usd: msrp, source: 'user-override' });
	await done(t);
}

/**
 * Hydrate from the static seed (e.g. /gpus-seed.json built by the cron
 * Keepa seeder). For every ASIN present in the seed, replace ALL of that
 * ASIN's IDB offers with the fresh seed entries — so listings that sold,
 * went out of stock, or otherwise dropped from Keepa's current pricing
 * disappear from the page instead of lingering forever. ASINs not in the
 * seed (e.g. the user's own browser-side scrapes) are left alone.
 */
export async function hydrateFromSeed(seedUrl = '/gpus-seed.json'): Promise<{ inserted: number; deleted: number; generatedAt: number | null } | null> {
	let res: Response;
	try { res = await fetch(seedUrl, { cache: 'no-store' }); } catch { return null; }
	if (!res.ok) return null;
	const seed = await res.json() as { offers?: GpuOffer[]; products?: GpuProduct[]; generated_at?: number };
	if (!seed?.offers?.length) return null;

	const seedAsins = new Set(seed.offers.map((o) => o.asin));
	for (const p of seed.products ?? []) seedAsins.add(p.asin);

	// Drop existing IDB offers for any ASIN the seed covers so stale listings vanish.
	let deleted = 0;
	if (seedAsins.size > 0) {
		const t = await tx(['offers'], 'readwrite');
		const idx = t.objectStore('offers').index('asin');
		await new Promise<void>((resolve, reject) => {
			let pending = seedAsins.size;
			if (pending === 0) return resolve();
			for (const asin of seedAsins) {
				const req = idx.openCursor(IDBKeyRange.only(asin));
				req.onsuccess = () => {
					const cur = req.result;
					if (cur) { cur.delete(); deleted++; cur.continue(); }
					else if (--pending === 0) resolve();
				};
				req.onerror = () => reject(req.error);
			}
		});
		await done(t);
	}

	await putOffers(seed.offers);
	if (seed.products) {
		const t = await tx(['products'], 'readwrite');
		for (const p of seed.products) t.objectStore('products').put(p);
		await done(t);
	}
	return { inserted: seed.offers.length, deleted, generatedAt: seed.generated_at ?? null };
}

export async function pruneStaleOffers(maxAgeMs: number): Promise<number> {
	const cutoff = Date.now() - maxAgeMs;
	const t = await tx(['offers'], 'readwrite');
	const store = t.objectStore('offers');
	const idx = store.index('last_seen');
	const range = IDBKeyRange.upperBound(cutoff);
	let count = 0;
	return new Promise((resolve, reject) => {
		const req = idx.openCursor(range);
		req.onsuccess = () => {
			const cur = req.result;
			if (cur) { cur.delete(); count++; cur.continue(); }
			else resolve(count);
		};
		req.onerror = () => reject(req.error);
	});
}
