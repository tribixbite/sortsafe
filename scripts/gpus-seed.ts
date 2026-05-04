/**
 * Playwright-based GPU offer seeder for the /gpus pipeline.
 *
 * Bypasses the browser-side scraper's two big constraints:
 *   - no Amazon "Continue shopping" interstitial (we click through it)
 *   - no CORS proxy needed (we run in node)
 *
 * Output: writes static/gpus-seed.json with the same shape the /gpus page
 * uses internally (offers + products + msrp). On first /gpus visit, if the
 * IDB is empty, the page hydrates from this file so users see data
 * immediately without triggering a fresh refresh.
 *
 * Usage:
 *   DISPLAY=:1 bun scripts/gpus-seed.ts                  # all models, 3 pages each
 *   DISPLAY=:1 bun scripts/gpus-seed.ts --models 3090    # subset
 *   DISPLAY=:1 bun scripts/gpus-seed.ts --pages 5
 */
import { chromium } from '/data/data/com.termux/files/home/.bun/install/global/node_modules/playwright-core';
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
interface SeedFile {
	generated_at: number;
	offers: SeedOffer[];
	products: SeedProduct[];
}

const SEARCH_QUERIES: Record<GpuModel, string> = {
	'3090': 'rtx 3090',
	'4090': 'rtx 4090',
	'5090': 'rtx 5090',
};
const CONDITION_FILTERS = {
	used: '6461716011',
	refurbished: '16349437011',
};
const USER_DATA = resolve(process.env.HOME!, '.cache/gpus-seed-profile');
const OUT_PATH = resolve(process.env.HOME!, 'git/sortsafe/static/gpus-seed.json');
const CRAWL_DELAY = 2500;

function inferModelFromTitle(title: string): GpuModel | null {
	const t = title.toLowerCase();
	const isAccessory = /backplate|water\s*block|waterblock|fan|cable|riser|bracket|mount|stand|cooler\s+(?!master\s+rtx)/i.test(t);
	if (isAccessory) return null;
	if (/\b(rtx|geforce)\s*5090\b/.test(t)) return '5090';
	if (/\b(rtx|geforce)\s*4090\b/.test(t)) return '4090';
	if (/\b(rtx|geforce)\s*3090\b/.test(t)) return '3090';
	return null;
}
function classifyCondition(label: string | null | undefined): GpuCondition {
	if (!label) return 'unknown';
	const l = label.toLowerCase();
	if (l.includes('amazon warehouse')) return 'warehouse';
	if (l.includes('refurbished') || l.includes('renewed')) return 'refurbished';
	if (l.includes('used') && l.includes('like new')) return 'used-like-new';
	if (l.includes('used') && l.includes('very good')) return 'used-very-good';
	if (l.includes('used') && l.includes('acceptable')) return 'used-acceptable';
	if (l.includes('used') && l.includes('good')) return 'used-good';
	if (l.includes('used')) return 'used-good';
	if (l.includes('new')) return 'new';
	return 'unknown';
}
function parsePriceUsd(s: string | null | undefined): number | null {
	if (!s) return null;
	const m = s.replace(/,/g, '').match(/\$?([\d.]+)/);
	if (!m) return null;
	const n = parseFloat(m[1]);
	return Number.isFinite(n) ? n : null;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function clickThroughInterstitial(page: any, targetUrl: string) {
	for (let attempt = 0; attempt < 2; attempt++) {
		const need = await page.evaluate(() => {
			const txt = (document.body?.innerText ?? '').toLowerCase();
			return /click the button below to continue shopping|continue shopping/.test(txt) && !document.querySelector('#productTitle, [data-component-type="s-search-result"]');
		}).catch(() => false);
		if (!need) return;
		try {
			await page.click('button.a-button-text, button[type="submit"], a.a-link-normal', { timeout: 4000 });
			await page.waitForLoadState('domcontentloaded', { timeout: 15000 });
		} catch {
			try { await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 20000 }); } catch {}
		}
	}
}

async function discoverPage(page: any, model: GpuModel, condition: 'used' | 'refurbished', pageNum: number): Promise<string[]> {
	const q = SEARCH_QUERIES[model];
	let url = `https://www.amazon.com/s?k=${encodeURIComponent(q)}&rh=p_n_condition_type%3A${CONDITION_FILTERS[condition]}`;
	if (pageNum > 1) url += `&page=${pageNum}`;
	try {
		await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
		await clickThroughInterstitial(page, url);
	} catch (e) {
		console.warn(`  search ${model}/${condition} p${pageNum} failed: ${(e as Error).message}`);
		return [];
	}
	const asins: string[] = await page.evaluate(() => {
		const els = Array.from(document.querySelectorAll('[data-asin][data-component-type="s-search-result"]')) as HTMLElement[];
		return els.map((el) => el.dataset.asin ?? '').filter((a) => a.length === 10);
	}).catch(() => []);
	return asins;
}

interface ProductScrape {
	product: SeedProduct | null;
	offers: SeedOffer[];
}

async function scrapeProduct(page: any, asin: string, hintModel: GpuModel): Promise<ProductScrape> {
	const url = `https://www.amazon.com/dp/${asin}`;
	try {
		await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
		await clickThroughInterstitial(page, url);
	} catch {
		return { product: null, offers: [] };
	}
	const data = await page.evaluate(() => {
		const text = (sel: string) => (document.querySelector(sel) as HTMLElement | null)?.innerText?.trim() ?? null;
		const title = text('#productTitle');
		const thumbEl = document.querySelector('#landingImage, #imgTagWrapperId img') as HTMLImageElement | null;
		const thumb = thumbEl?.dataset.oldHires ?? thumbEl?.src ?? null;

		const rows = Array.from(document.querySelectorAll('[id^="newAccordionRow_"], [id^="usedAccordionRow_"], [id^="refurbishedAccordionRow_"]')) as HTMLElement[];
		const accordion = rows.map((row) => {
			const id = row.id;
			const priceEl = row.querySelector('.a-price .a-offscreen, .a-offscreen') as HTMLElement | null;
			const labelEl = row.querySelector('.a-accordion-row-container .a-color-base, .a-accordion-row span') as HTMLElement | null;
			const sellerLink = row.querySelector('a[href*="seller="]') as HTMLAnchorElement | null;
			return {
				id,
				priceText: priceEl?.innerText ?? null,
				label: labelEl?.innerText?.trim() ?? null,
				sellerName: sellerLink?.innerText?.trim() ?? null,
				sellerHref: sellerLink?.getAttribute('href') ?? null,
			};
		});

		const buybox = text('#corePrice_feature_div .a-offscreen') ?? text('#corePriceDisplay_desktop_feature_div .a-offscreen');
		const delivery = text('#mir-layout-DELIVERY_BLOCK_PRIMARY_DELIVERY_MESSAGE_LARGE') ?? text('#mir-layout-DELIVERY_BLOCK');
		return { title, thumb, accordion, buybox, delivery };
	}).catch(() => null);

	if (!data || !data.title) return { product: null, offers: [] };

	const verifiedModel = inferModelFromTitle(data.title);
	if (!verifiedModel) return { product: null, offers: [] };

	const now = Date.now();
	const product: SeedProduct = {
		asin, model: verifiedModel, title: data.title,
		thumbnail_url: data.thumb, last_refreshed: now,
	};
	const offers: SeedOffer[] = [];
	let i = 0;
	for (const row of data.accordion) {
		const price = parsePriceUsd(row.priceText);
		if (price == null) continue;
		let condition: GpuCondition = 'unknown';
		if (row.id.startsWith('new')) condition = 'new';
		else if (row.id.startsWith('used')) condition = 'used-good';
		else if (row.id.startsWith('refurb')) condition = 'refurbished';
		const detailed = classifyCondition(row.label ?? row.id);
		if (detailed !== 'unknown') condition = detailed;
		const sellerId = row.sellerHref?.match(/seller=([A-Z0-9]+)/)?.[1] ?? null;
		offers.push({
			offer_id: `${asin}__leadrow_${condition}_${++i}`,
			asin, model: verifiedModel, title: data.title,
			condition, condition_note: null,
			price_usd: price, currency: 'USD',
			seller: row.sellerName, seller_id: sellerId,
			seller_rating: null, seller_rating_count: null,
			ships_from: null, delivery_text: data.delivery?.slice(0, 200) ?? null,
			first_seen: now, last_seen: now,
			is_buybox: condition === 'new' || (offers.length === 0 && condition.startsWith('used')),
		});
	}
	if (offers.length === 0 && data.buybox) {
		const price = parsePriceUsd(data.buybox);
		if (price != null) {
			offers.push({
				offer_id: `${asin}__buybox`, asin, model: verifiedModel, title: data.title,
				condition: 'new', condition_note: null,
				price_usd: price, currency: 'USD',
				seller: null, seller_id: null, seller_rating: null, seller_rating_count: null,
				ships_from: null, delivery_text: null,
				first_seen: now, last_seen: now, is_buybox: true,
			});
		}
	}
	return { product, offers };
}

async function main() {
	const args = process.argv.slice(2);
	const modelsArg = args.indexOf('--models');
	const pagesArg = args.indexOf('--pages');
	const models: GpuModel[] = modelsArg >= 0
		? args[modelsArg + 1].split(',') as GpuModel[]
		: ['3090', '4090', '5090'];
	const pagesPerQuery = pagesArg >= 0 ? parseInt(args[pagesArg + 1], 10) : 3;

	console.log(`gpus-seed: models=${models.join(',')} pages=${pagesPerQuery}`);
	console.log(`output: ${OUT_PATH}`);

	const ctx = await chromium.launchPersistentContext(USER_DATA, {
		executablePath: '/data/data/com.termux/files/usr/bin/chromium-browser',
		headless: process.env.DISPLAY ? false : true,
		args: ['--no-sandbox', '--disable-gpu', '--disable-notifications', '--disable-dev-shm-usage'],
		viewport: { width: 1280, height: 720 },
		userAgent: 'Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
	});
	const page = ctx.pages()[0] ?? await ctx.newPage();

	// Phase 1 — discover
	const discoveredByModel = new Map<GpuModel, Set<string>>();
	for (const model of models) {
		const set = new Set<string>();
		for (const cond of ['used', 'refurbished'] as const) {
			for (let p = 1; p <= pagesPerQuery; p++) {
				const asins = await discoverPage(page, model, cond, p);
				console.log(`  discover ${model}/${cond} p${p}: ${asins.length} ASINs`);
				asins.forEach((a) => set.add(a));
				await sleep(CRAWL_DELAY);
			}
		}
		discoveredByModel.set(model, set);
	}

	// Phase 2 — enrich
	const allOffers: SeedOffer[] = [];
	const allProducts: SeedProduct[] = [];
	let total = 0, kept = 0;
	for (const [model, asins] of discoveredByModel) {
		for (const asin of asins) {
			total++;
			const r = await scrapeProduct(page, asin, model);
			if (r.product && r.offers.length > 0) {
				allOffers.push(...r.offers);
				allProducts.push(r.product);
				kept++;
				console.log(`  ${asin} (${r.product.model}) +${r.offers.length} offers`);
			} else {
				console.log(`  ${asin} skipped`);
			}
			await sleep(CRAWL_DELAY);
		}
	}

	const out: SeedFile = { generated_at: Date.now(), offers: allOffers, products: allProducts };
	mkdirSync(resolve(OUT_PATH, '..'), { recursive: true });
	writeFileSync(OUT_PATH, JSON.stringify(out, null, 2));

	console.log(`\nseed written: ${kept}/${total} ASINs kept, ${allOffers.length} offers`);
	await ctx.close();
}

main().catch((e) => {
	console.error(e);
	process.exit(1);
});
