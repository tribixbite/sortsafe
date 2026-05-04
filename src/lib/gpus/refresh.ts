/**
 * Orchestrates a full /gpus refresh:
 *   1. Discover ASINs for each model + condition family (multi-page)
 *   2. Fetch offers per ASIN with bounded concurrency (4 in-flight)
 *   3. Persist offers + snapshots into IndexedDB
 *
 * `navigator.locks` ensures only one tab refreshes at a time even if the
 * user has multiple /gpus tabs open.
 */
import type { GpuModel } from './types';
import { discoverAsins, fetchProductWithOffers, snapshotsFromOffers } from './scraper';
import { putOffers, putProduct, appendSnapshots } from './db';

export interface RefreshProgress {
	phase: 'idle' | 'discovering' | 'enriching' | 'done' | 'error';
	model?: GpuModel;
	asin?: string;
	asinsTotal?: number;
	asinsDone?: number;
	offersInserted?: number;
	error?: string;
}

const MODELS: GpuModel[] = ['3090', '4090', '5090'];
const DISCOVERY_PAGES = 3;
const DISCOVERY_DELAY_MS = 6000;
const CONCURRENCY = 4;
const PER_REQUEST_STAGGER_MS = 800;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function refreshAll(
	onProgress: (p: RefreshProgress) => void = () => {},
	signal?: AbortSignal
): Promise<RefreshProgress> {
	return await navigator.locks.request('gpus-refresh', async () => {
		try {
			let totalOffers = 0;
			const asinsByModel = new Map<GpuModel, Set<string>>();

			for (const model of MODELS) {
				if (signal?.aborted) throw new Error('aborted');
				onProgress({ phase: 'discovering', model });
				const seen = new Set<string>();
				for (const condition of ['used', 'refurbished'] as const) {
					try {
						const found = await discoverAsins(model, condition, { signal, pages: DISCOVERY_PAGES });
						for (const f of found) seen.add(f.asin);
					} catch (e) {
						console.warn(`discover ${model}/${condition} failed`, e);
					}
					await sleep(DISCOVERY_DELAY_MS);
				}
				asinsByModel.set(model, seen);
			}

			let asinsDone = 0;
			const asinsTotal = [...asinsByModel.values()].reduce((s, x) => s + x.size, 0);

			// Flatten queue: [{ model, asin }, ...]
			const queue: Array<{ model: GpuModel; asin: string }> = [];
			for (const [model, asins] of asinsByModel) {
				for (const asin of asins) queue.push({ model, asin });
			}

			let cursor = 0;
			async function worker(workerId: number) {
				// Stagger initial fetch so all workers don't hit Amazon in the same instant
				await sleep(workerId * PER_REQUEST_STAGGER_MS);
				while (true) {
					if (signal?.aborted) throw new Error('aborted');
					const idx = cursor++;
					if (idx >= queue.length) return;
					const { model, asin } = queue[idx];
					onProgress({
						phase: 'enriching', model, asin,
						asinsTotal, asinsDone, offersInserted: totalOffers,
					});
					try {
						const result = await fetchProductWithOffers(asin, model, { signal });
						if (result && result.offers.length > 0) {
							await putOffers(result.offers);
							await putProduct(result.product);
							await appendSnapshots(snapshotsFromOffers(result.offers));
							totalOffers += result.offers.length;
						}
					} catch (e) {
						console.warn(`fetchProductWithOffers ${asin} failed`, e);
					}
					asinsDone++;
					// Per-worker stagger between iterations keeps total load steady
					await sleep(PER_REQUEST_STAGGER_MS * CONCURRENCY);
				}
			}

			await Promise.all(Array.from({ length: CONCURRENCY }, (_, i) => worker(i)));

			const final: RefreshProgress = { phase: 'done', asinsTotal, asinsDone, offersInserted: totalOffers };
			onProgress(final);
			return final;
		} catch (e) {
			const final: RefreshProgress = { phase: 'error', error: (e as Error).message };
			onProgress(final);
			return final;
		}
	});
}
