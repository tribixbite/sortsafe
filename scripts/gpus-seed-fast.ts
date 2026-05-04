/**
 * Headless node-side GPU seeder. No browser, no Playwright — uses the local
 * proxy at :8787 (same one the SPA uses) and parses with node-html-parser.
 *
 * Architecture:
 *   1. Discovery: hit Amazon search filtered by condition (used / refurbished).
 *      The search RESULT CARDS contain the lowest price for that condition,
 *      so we get (asin, condition, price) directly without per-condition PDP
 *      fetches (which Amazon doesn't render statically anyway).
 *   2. Enrichment: ONE /dp/<asin> per unique ASIN — only for title
 *      verification (must mention the model) + thumbnail.
 *   3. Emit one offer per (asin, condition_source) pair. So the same ASIN
 *      surfaced under both "used" and "refurbished" search filters becomes
 *      two offers.
 *
 * Output: static/gpus-seed.json — same shape /gpus hydrates from.
 *
 * Target runtime: ~30-60s for 100+ ASINs at concurrency=6.
 *
 * Usage:
 *   bun scripts/gpus-seed-fast.ts                 # 3 models, 2 search pages each
 *   bun scripts/gpus-seed-fast.ts --models 5090
 *   bun scripts/gpus-seed-fast.ts --pages 4
 *   PROXY=http://localhost:9999/fetch bun scripts/gpus-seed-fast.ts
 */
import { parse } from 'node-html-parser';
import { writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';

type GpuModel = '3090' | '4090' | '5090';
type GpuCondition = 'new' | 'used-like-new' | 'used-very-good' | 'used-good' | 'used-acceptable' | 'refurbished' | 'warehouse' | 'unknown';

interface SeedOffer {
	offer_id: string;
	asin: string;
	model: GpuModel;
	title: string;
	condition: GpuCondition;
	condition_note: string | null;
	price_usd: number;
	currency: 'USD';
	seller: string | null;
	seller_id: string | null;
	seller_rating: number | null;
	seller_rating_count: number | null;
	ships_from: string | null;
	delivery_text: string | null;
	first_seen: number;
	last_seen: number;
	is_buybox: boolean;
}
interface SeedProduct {
	asin: string;
	model: GpuModel;
	title: string;
	thumbnail_url: string | null;
	last_refreshed: number;
}

const SEARCH_QUERIES: Record<GpuModel, string> = {
	'3090': 'rtx 3090', '4090': 'rtx 4090', '5090': 'rtx 5090',
};
const CONDITION_FILTERS: Record<'used' | 'refurbished', { node: string; condition: GpuCondition }> = {
	used: { node: '6461716011', condition: 'used-good' },
	refurbished: { node: '16349437011', condition: 'refurbished' },
};

const PROXY = process.env.PROXY ?? 'http://localhost:8787/fetch';
const OUT_PATH = resolve(process.env.HOME!, 'git/sortsafe/static/gpus-seed.json');
const SEARCH_CONCURRENCY = 2;
const PDP_CONCURRENCY = 6;
const SEARCH_DELAY_MS = 4000;
const PDP_DELAY_MS = 250;

function inferModelFromTitle(title: string): GpuModel | null {
	const t = title.toLowerCase();
	if (/backplate|water\s*block|waterblock|fan only|cable|riser|bracket|mount|stand alone|cooler\s+(?!master\s+rtx)/i.test(t)) return null;
	if (/\b(rtx|geforce)\s*5090\b/.test(t)) return '5090';
	if (/\b(rtx|geforce)\s*4090\b/.test(t)) return '4090';
	if (/\b(rtx|geforce)\s*3090\b/.test(t)) return '3090';
	return null;
}
function parsePriceUsd(s: string | null | undefined): number | null {
	if (!s) return null;
	const m = s.replace(/,/g, '').match(/\$?([\d.]+)/);
	if (!m) return null;
	const n = parseFloat(m[1]);
	return Number.isFinite(n) ? n : null;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface FetchResult { html: string | null; status: number; }
async function proxyFetch(url: string): Promise<FetchResult> {
	try {
		const res = await fetch(`${PROXY}?url=${encodeURIComponent(url)}`);
		const html = await res.text();
		// The proxy passes through Amazon status. 200 with "Sorry" page is bot detection.
		if (html.length < 5000 && /Sorry! Something went wrong/i.test(html)) {
			return { html: null, status: 503 };
		}
		return { html: res.ok ? html : null, status: res.status };
	} catch {
		return { html: null, status: 0 };
	}
}

interface DiscoverHit {
	asin: string;
	priceUsd: number | null;
	condition: GpuCondition;
	model: GpuModel;
}

async function discoverPage(model: GpuModel, conditionKey: 'used' | 'refurbished', pageNum: number): Promise<DiscoverHit[]> {
	const q = SEARCH_QUERIES[model];
	const cf = CONDITION_FILTERS[conditionKey];
	let url = `https://www.amazon.com/s?k=${encodeURIComponent(q)}&rh=p_n_condition_type%3A${cf.node}`;
	if (pageNum > 1) url += `&page=${pageNum}`;
	const { html, status } = await proxyFetch(url);
	if (!html) {
		console.warn(`  ${model}/${conditionKey} p${pageNum}: HTTP ${status} (skip)`);
		return [];
	}
	const root = parse(html);
	const items = root.querySelectorAll('[data-asin][data-component-type="s-search-result"]');
	const out: DiscoverHit[] = [];
	for (const el of items) {
		const a = el.getAttribute('data-asin');
		if (!a || a.length !== 10) continue;
		const priceText = el.querySelector('.a-price .a-offscreen')?.text ?? null;
		out.push({
			asin: a, priceUsd: parsePriceUsd(priceText),
			condition: cf.condition, model,
		});
	}
	return out;
}

interface ProductMeta { title: string; thumb: string | null; verifiedModel: GpuModel | null; }
async function fetchProductMeta(asin: string): Promise<ProductMeta | null> {
	const { html } = await proxyFetch(`https://www.amazon.com/dp/${asin}`);
	if (!html) return null;
	const root = parse(html);
	const title = root.querySelector('#productTitle')?.text?.trim() ?? '';
	if (!title) return null;
	const thumbEl = root.querySelector('#landingImage') ?? root.querySelector('#imgTagWrapperId img');
	const thumb = thumbEl?.getAttribute('data-old-hires') ?? thumbEl?.getAttribute('src') ?? null;
	return { title, thumb, verifiedModel: inferModelFromTitle(title) };
}

async function pool<T>(items: T[], n: number, delayMs: number, fn: (item: T, i: number) => Promise<void>): Promise<void> {
	let cursor = 0;
	const workers = Array.from({ length: n }, async (_, w) => {
		await sleep(w * delayMs);
		while (cursor < items.length) {
			const idx = cursor++;
			await fn(items[idx], idx);
			await sleep(delayMs);
		}
	});
	await Promise.all(workers);
}

async function main() {
	const t0 = Date.now();
	const args = process.argv.slice(2);
	const modelsArg = args.indexOf('--models');
	const pagesArg = args.indexOf('--pages');
	const models: GpuModel[] = modelsArg >= 0 ? args[modelsArg + 1].split(',') as GpuModel[] : ['3090', '4090', '5090'];
	const pages = pagesArg >= 0 ? parseInt(args[pagesArg + 1], 10) : 2;

	console.log(`gpus-seed-fast: models=${models.join(',')} pages=${pages} proxy=${PROXY}`);

	try {
		const h = await fetch(PROXY.replace('/fetch', '/healthz'));
		if (!h.ok) throw new Error(`healthz ${h.status}`);
	} catch {
		console.error(`proxy not reachable at ${PROXY} — start with \`bun scripts/gpu-proxy-local.ts\``);
		process.exit(1);
	}

	// Phase 1: discover with condition + price baked in
	const discoverJobs: { model: GpuModel; cond: 'used' | 'refurbished'; page: number }[] = [];
	for (const m of models) for (const c of ['used', 'refurbished'] as const) for (let p = 1; p <= pages; p++) discoverJobs.push({ model: m, cond: c, page: p });

	console.log(`phase 1: ${discoverJobs.length} search pages (concurrency=${SEARCH_CONCURRENCY}, ${SEARCH_DELAY_MS}ms delay)`);
	const allHits: DiscoverHit[] = [];
	await pool(discoverJobs, SEARCH_CONCURRENCY, SEARCH_DELAY_MS, async (j) => {
		const hits = await discoverPage(j.model, j.cond, j.page);
		console.log(`  ${j.model}/${j.cond} p${j.page}: ${hits.length} hits`);
		allHits.push(...hits);
	});

	if (allHits.length === 0) {
		console.error('No hits discovered — Amazon likely rate-limiting search pages. Aborting.');
		process.exit(2);
	}

	// Dedup ASINs that need PDP enrichment (one PDP per unique ASIN)
	const pdpQueue = [...new Set(allHits.map((h) => h.asin))];
	console.log(`discovered ${allHits.length} (asin,condition) hits, ${pdpQueue.length} unique ASINs`);

	// Phase 2: enrich (title verification + thumbnail) — concurrency safe, PDPs aren't rate-limited
	console.log(`phase 2: ${pdpQueue.length} PDPs (concurrency=${PDP_CONCURRENCY})`);
	const meta = new Map<string, ProductMeta>();
	let pdpKept = 0, pdpSkipped = 0;
	await pool(pdpQueue, PDP_CONCURRENCY, PDP_DELAY_MS, async (asin) => {
		const m = await fetchProductMeta(asin);
		if (m && m.verifiedModel) {
			meta.set(asin, m);
			pdpKept++;
		} else {
			pdpSkipped++;
		}
	});
	console.log(`  PDP: ${pdpKept} verified, ${pdpSkipped} rejected (wrong model / accessory / blocked)`);

	// Phase 3: assemble offers from discovery hits + verified meta
	const now = Date.now();
	const offers: SeedOffer[] = [];
	const products: SeedProduct[] = [];
	const seenProduct = new Set<string>();
	let i = 0;
	for (const h of allHits) {
		const m = meta.get(h.asin);
		if (!m || !m.verifiedModel) continue;
		// Use the verified model from the title rather than the search-query model
		// (e.g., "rtx 5090" search sometimes returns 5070/5080 listings).
		const model = m.verifiedModel;
		if (h.priceUsd == null) continue;
		offers.push({
			offer_id: `${h.asin}__search_${h.condition}_${++i}`,
			asin: h.asin, model, title: m.title,
			condition: h.condition, condition_note: 'lowest price under Amazon condition filter',
			price_usd: h.priceUsd, currency: 'USD',
			seller: null, seller_id: null,
			seller_rating: null, seller_rating_count: null,
			ships_from: null, delivery_text: null,
			first_seen: now, last_seen: now,
			is_buybox: false,
		});
		if (!seenProduct.has(h.asin)) {
			products.push({
				asin: h.asin, model, title: m.title,
				thumbnail_url: m.thumb, last_refreshed: now,
			});
			seenProduct.add(h.asin);
		}
	}

	const out = { generated_at: now, offers, products };
	mkdirSync(resolve(OUT_PATH, '..'), { recursive: true });
	writeFileSync(OUT_PATH, JSON.stringify(out, null, 2));
	const dt = ((Date.now() - t0) / 1000).toFixed(1);
	const byCond: Record<string, number> = {};
	for (const o of offers) byCond[o.condition] = (byCond[o.condition] ?? 0) + 1;
	console.log(`\nseed written in ${dt}s: ${products.length} products, ${offers.length} offers (${Object.entries(byCond).map(([k,v])=>`${k}:${v}`).join(' ')}) → ${OUT_PATH}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
