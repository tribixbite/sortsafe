/**
 * Browser-side scraper for the /gpus pipeline.
 *
 * Flow:
 *   1. discoverAsins(model)         — Amazon search by model + condition filter
 *   2. fetchOffers(asin, model)     — /gp/offer-listing/<asin>?<conditions>
 *   3. fetchProductMeta(asin)       — /dp/<asin> for title + thumbnail
 *
 * All fetches go through the proxy so the browser sees CORS-friendly responses.
 * HTML parsing uses DOMParser (browser-native).
 */
import type { GpuModel, GpuCondition, GpuOffer, GpuProduct, PriceSnapshot } from './types';

const PROXY_URL_DEFAULT = (typeof location !== 'undefined' && location.hostname === 'localhost')
	? 'http://localhost:8787/fetch'
	: '/proxy/fetch';

const SEARCH_QUERIES: Record<GpuModel, string> = {
	'3090': 'rtx 3090',
	'4090': 'rtx 4090',
	'5090': 'rtx 5090',
};

// Amazon condition_type filter values (browse node IDs vary occasionally; these are stable as of 2026).
const CONDITION_FILTERS = {
	used: '6461716011',
	refurbished: '16349437011',
	// "warehouse" doesn't have its own filter; warehouse offers appear in the offer-listing as condition rows
};

interface ProxyOpts {
	proxyUrl?: string;
	signal?: AbortSignal;
}

async function fetchHtml(url: string, opts: ProxyOpts = {}): Promise<Document> {
	const proxy = opts.proxyUrl ?? PROXY_URL_DEFAULT;
	const res = await fetch(`${proxy}?url=${encodeURIComponent(url)}`, { signal: opts.signal });
	if (!res.ok) throw new Error(`proxy returned ${res.status} for ${url}`);
	const html = await res.text();
	return new DOMParser().parseFromString(html, 'text/html');
}

export interface DiscoverResult {
	asin: string;
	titleHint: string;
	priceHint: number | null;
}

export async function discoverAsins(
	model: GpuModel,
	condition: 'used' | 'refurbished' | 'all',
	opts: ProxyOpts & { pages?: number } = {}
): Promise<DiscoverResult[]> {
	const q = SEARCH_QUERIES[model];
	const pages = Math.max(1, opts.pages ?? 1);
	const seen = new Map<string, DiscoverResult>();

	for (let page = 1; page <= pages; page++) {
		let url = `https://www.amazon.com/s?k=${encodeURIComponent(q)}`;
		if (condition !== 'all') url += `&rh=p_n_condition_type%3A${CONDITION_FILTERS[condition]}`;
		if (page > 1) url += `&page=${page}`;
		let doc: Document;
		try {
			doc = await fetchHtml(url, opts);
		} catch (e) {
			console.warn(`discover page ${page} failed:`, e);
			continue;
		}
		const items = doc.querySelectorAll('[data-asin][data-component-type="s-search-result"]');
		items.forEach((el) => {
			const asin = (el as HTMLElement).dataset.asin;
			if (!asin || asin.length !== 10 || seen.has(asin)) return;
			const title = (el.querySelector('h2 span') as HTMLElement | null)?.textContent
				?? (el.querySelector('h2') as HTMLElement | null)?.textContent
				?? '';
			const priceText = (el.querySelector('.a-price .a-offscreen') as HTMLElement | null)?.textContent ?? '';
			const priceHint = parsePriceUsd(priceText);
			seen.set(asin, { asin, titleHint: title.trim().slice(0, 200), priceHint });
		});
	}
	return [...seen.values()];
}

function parsePriceUsd(s: string | null | undefined): number | null {
	if (!s) return null;
	const m = s.replace(/,/g, '').match(/\$?([\d.]+)/);
	if (!m) return null;
	const n = parseFloat(m[1]);
	return Number.isFinite(n) ? n : null;
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

/**
 * Match the GPU model in a product title (5090 wins over 5080 wins over 5070, etc.)
 * so a "rtx 5090" search hit that's actually a 5070 gets reassigned correctly.
 * Skips parts/accessories (backplates, waterblocks, cables) since those tend to
 * be cheap items that pollute deal lists.
 */
export function inferModelFromTitle(title: string): GpuModel | null {
	const t = title.toLowerCase();
	const isAccessory = /backplate|water\s*block|waterblock|fan|cable|riser|bracket|mount|stand|cooler\s+(?!master\s+rtx)/i.test(t);
	if (isAccessory) return null;
	// require "rtx <model>" or "geforce <model>" so a generic listing that just mentions a model number doesn't match
	if (/\b(rtx|geforce)\s*5090\b/.test(t)) return '5090';
	if (/\b(rtx|geforce)\s*4090\b/.test(t)) return '4090';
	if (/\b(rtx|geforce)\s*3090\b/.test(t)) return '3090';
	return null;
}

/**
 * Extract per-condition lead offers from a product detail page.
 *
 * Amazon's `/gp/offer-listing/` URL only returns full per-seller listings when
 * the AOD widget loads via JS — a plain HTTP fetch redirects to the PDP. So we
 * scrape the PDP itself, which always has the new/used buy-box accordion plus
 * the lead seller per condition. That's 1-4 offers per ASIN (new/used/refurb/
 * warehouse) instead of every seller, but enough for deal hunting and works
 * with a static-HTML proxy.
 */
export async function fetchOffers(
	asin: string,
	model: GpuModel,
	opts: ProxyOpts = {}
): Promise<GpuOffer[]> {
	const doc = await fetchHtml(`https://www.amazon.com/dp/${asin}`, opts);
	const out: GpuOffer[] = [];
	const now = Date.now();

	const accordionRows = doc.querySelectorAll('[id^="newAccordionRow_"], [id^="usedAccordionRow_"], [id^="refurbishedAccordionRow_"]');
	let i = 0;
	accordionRows.forEach((row) => {
		const id = row.id;
		let condition: GpuCondition = 'unknown';
		if (id.startsWith('new')) condition = 'new';
		else if (id.startsWith('used')) condition = 'used-good'; // PDP doesn't break out used grade in the lead row
		else if (id.startsWith('refurb')) condition = 'refurbished';

		const priceText = (row.querySelector('.a-price .a-offscreen, .a-offscreen') as HTMLElement | null)?.textContent ?? '';
		const price = parsePriceUsd(priceText);
		if (price == null) return;

		const accordionLabel = (row.querySelector('.a-accordion-row-container .a-color-base, .a-accordion-row span') as HTMLElement | null)?.textContent?.trim() ?? null;
		const detailedCond = classifyCondition(accordionLabel ?? id);
		if (detailedCond !== 'unknown') condition = detailedCond;

		const sellerLink = row.querySelector('a[href*="seller="]') as HTMLAnchorElement | null;
		const seller = sellerLink?.textContent?.trim() ?? null;
		const sellerHref = sellerLink?.getAttribute('href') ?? '';
		const sellerId = sellerHref.match(/seller=([A-Z0-9]+)/)?.[1] ?? null;

		// Delivery text is usually in #mir-layout-DELIVERY_BLOCK at the page level, not per-row
		const delivery = (doc.querySelector('#mir-layout-DELIVERY_BLOCK_PRIMARY_DELIVERY_MESSAGE_LARGE, #mir-layout-DELIVERY_BLOCK') as HTMLElement | null)?.textContent?.trim().slice(0, 200) ?? null;

		const offerId = `${asin}__leadrow_${condition}_${++i}`;
		out.push({
			offer_id: offerId,
			asin,
			model,
			title: '',
			condition,
			condition_note: null,
			price_usd: price,
			currency: 'USD',
			seller,
			seller_id: sellerId,
			seller_rating: null,
			seller_rating_count: null,
			ships_from: null,
			delivery_text: delivery,
			first_seen: now,
			last_seen: now,
			is_buybox: condition === 'new' || (out.length === 0 && condition.startsWith('used')),
		});
	});

	// Fallback: if accordion didn't render, take the buy-box price as a single 'new' offer
	if (out.length === 0) {
		const buyboxPriceText = (doc.querySelector('#corePrice_feature_div .a-offscreen, #corePriceDisplay_desktop_feature_div .a-offscreen') as HTMLElement | null)?.textContent ?? '';
		const buyboxPrice = parsePriceUsd(buyboxPriceText);
		if (buyboxPrice != null) {
			out.push({
				offer_id: `${asin}__buybox`, asin, model, title: '',
				condition: 'new', condition_note: null,
				price_usd: buyboxPrice, currency: 'USD',
				seller: null, seller_id: null, seller_rating: null, seller_rating_count: null,
				ships_from: null, delivery_text: null,
				first_seen: now, last_seen: now, is_buybox: true,
			});
		}
	}
	return out;
}

export async function fetchProductMeta(asin: string, model: GpuModel, opts: ProxyOpts = {}): Promise<GpuProduct> {
	const doc = await fetchHtml(`https://www.amazon.com/dp/${asin}`, opts);
	return productMetaFromDoc(doc, asin, model);
}

function productMetaFromDoc(doc: Document, asin: string, model: GpuModel): GpuProduct {
	const title = (doc.querySelector('#productTitle') as HTMLElement | null)?.textContent?.trim() ?? '';
	const thumb = (doc.querySelector('#landingImage, #imgTagWrapperId img') as HTMLImageElement | null);
	const thumbUrl = thumb?.getAttribute('data-old-hires') ?? thumb?.getAttribute('src') ?? null;
	return { asin, model, title, thumbnail_url: thumbUrl, last_refreshed: Date.now() };
}

/**
 * Combined fetchOffers + fetchProductMeta — both hit /dp/<asin>, no reason to
 * pay the proxy fetch twice. Returns null if the title doesn't match a tracked
 * GPU model (so the caller can skip without inserting bad data).
 */
export async function fetchProductWithOffers(
	asin: string,
	hintModel: GpuModel,
	opts: ProxyOpts = {}
): Promise<{ product: GpuProduct; offers: GpuOffer[] } | null> {
	const doc = await fetchHtml(`https://www.amazon.com/dp/${asin}`, opts);
	const product = productMetaFromDoc(doc, asin, hintModel);
	const verifiedModel = inferModelFromTitle(product.title);
	if (!verifiedModel) return null;
	product.model = verifiedModel;
	const offers = extractOffersFromDoc(doc, asin, verifiedModel);
	offers.forEach((o) => { o.title = product.title; });
	return { product, offers };
}

function extractOffersFromDoc(doc: Document, asin: string, model: GpuModel): GpuOffer[] {
	const out: GpuOffer[] = [];
	const now = Date.now();
	const accordionRows = doc.querySelectorAll('[id^="newAccordionRow_"], [id^="usedAccordionRow_"], [id^="refurbishedAccordionRow_"]');
	let i = 0;
	accordionRows.forEach((row) => {
		const id = row.id;
		let condition: GpuCondition = 'unknown';
		if (id.startsWith('new')) condition = 'new';
		else if (id.startsWith('used')) condition = 'used-good';
		else if (id.startsWith('refurb')) condition = 'refurbished';
		const priceText = (row.querySelector('.a-price .a-offscreen, .a-offscreen') as HTMLElement | null)?.textContent ?? '';
		const price = parsePriceUsd(priceText);
		if (price == null) return;
		const accordionLabel = (row.querySelector('.a-accordion-row-container .a-color-base, .a-accordion-row span') as HTMLElement | null)?.textContent?.trim() ?? null;
		const detailedCond = classifyCondition(accordionLabel ?? id);
		if (detailedCond !== 'unknown') condition = detailedCond;
		const sellerLink = row.querySelector('a[href*="seller="]') as HTMLAnchorElement | null;
		const seller = sellerLink?.textContent?.trim() ?? null;
		const sellerHref = sellerLink?.getAttribute('href') ?? '';
		const sellerId = sellerHref.match(/seller=([A-Z0-9]+)/)?.[1] ?? null;
		const delivery = (doc.querySelector('#mir-layout-DELIVERY_BLOCK_PRIMARY_DELIVERY_MESSAGE_LARGE, #mir-layout-DELIVERY_BLOCK') as HTMLElement | null)?.textContent?.trim().slice(0, 200) ?? null;
		out.push({
			offer_id: `${asin}__leadrow_${condition}_${++i}`, asin, model, title: '',
			condition, condition_note: null, price_usd: price, currency: 'USD',
			seller, seller_id: sellerId,
			seller_rating: null, seller_rating_count: null,
			ships_from: null, delivery_text: delivery,
			first_seen: now, last_seen: now,
			is_buybox: condition === 'new' || (out.length === 0 && condition.startsWith('used')),
		});
	});
	if (out.length === 0) {
		const buyboxPriceText = (doc.querySelector('#corePrice_feature_div .a-offscreen, #corePriceDisplay_desktop_feature_div .a-offscreen') as HTMLElement | null)?.textContent ?? '';
		const buyboxPrice = parsePriceUsd(buyboxPriceText);
		if (buyboxPrice != null) {
			out.push({
				offer_id: `${asin}__buybox`, asin, model, title: '',
				condition: 'new', condition_note: null,
				price_usd: buyboxPrice, currency: 'USD',
				seller: null, seller_id: null, seller_rating: null, seller_rating_count: null,
				ships_from: null, delivery_text: null,
				first_seen: now, last_seen: now, is_buybox: true,
			});
		}
	}
	return out;
}

/** Build a per-asin/condition snapshot from the lowest offer in each (asin, condition) pair. */
export function snapshotsFromOffers(offers: GpuOffer[]): PriceSnapshot[] {
	const lowest = new Map<string, GpuOffer>();
	for (const o of offers) {
		const key = `${o.asin}__${o.condition}`;
		const cur = lowest.get(key);
		if (!cur || o.price_usd < cur.price_usd) lowest.set(key, o);
	}
	const now = Date.now();
	return [...lowest.values()].map((o) => ({
		asin: o.asin, condition: o.condition, price_usd: o.price_usd, taken_at: now,
	}));
}
