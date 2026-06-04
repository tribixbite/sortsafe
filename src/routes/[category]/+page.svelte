<script lang="ts">
	import type { PageData } from './$types';
	import { getCategory } from '$lib/catalog/categories';
	import { CONDITION_LABELS, type Condition, type Offer, type Product } from '$lib/catalog/types';
	import { catalogDb } from '$lib/catalog/db';
	import { refreshAll, type RefreshProgress } from '$lib/catalog/refresh';

	let { data }: { data: PageData } = $props();
	const cfg = $derived(getCategory(data.slug)!);
	const db = $derived(catalogDb(cfg.slug));
	const hasRef = $derived(!!cfg.referencePrices);

	let offers: Offer[] = $state([]);
	let products: Map<string, Product> = $state(new Map());
	let progress: RefreshProgress | null = $state(null);

	let variantFilter = $state('all');
	let conditionFilter = $state('all');
	let hideNew = $state(false);
	let minDiscountPct = $state(-300);
	let minSellerRating = $state(0);
	let goodDealOnly = $state(false);
	let maxPrice = $state(0); // 0 = no ceiling; set after load
	let sortKey: 'price' | 'price-desc' | 'discount' | 'value' | 'rating' | 'last_seen' = $state('price');

	let abortCtl: AbortController | null = null;

	// (Re)hydrate whenever the category changes (covers initial mount and any
	// client-side navigation between categories).
	$effect(() => {
		const slug = cfg.slug; // tracked dependency
		const thisDb = db;
		let cancelled = false;
		offers = [];
		products = new Map();
		progress = null;
		maxPrice = 0;
		sortKey = hasRef ? 'discount' : 'price';
		(async () => {
			const hydrated = await thisDb.hydrateFromSeed(`/${slug}-seed.json`).catch(() => null);
			if (cancelled) return;
			await reload();
			if (hydrated && !cancelled)
				progress = { phase: 'done', offersInserted: hydrated.inserted, asinsTotal: products.size, asinsDone: products.size };
		})();
		return () => { cancelled = true; };
	});

	async function reload() {
		offers = await db.getAllOffers();
		const prods = await db.getAllProducts();
		products = new Map(prods.map((p) => [p.asin, p]));
		const max = offers.reduce((m, o) => Math.max(m, o.price_usd), 0);
		priceCeiling = Math.ceil(max / 50) * 50 || 100;
		if (maxPrice === 0) maxPrice = priceCeiling;
	}
	let priceCeiling = $state(5000);

	async function doRefresh() {
		if (progress?.phase === 'discovering' || progress?.phase === 'enriching') return;
		abortCtl = new AbortController();
		progress = { phase: 'discovering' };
		await refreshAll(cfg, db, (p) => { progress = p; }, abortCtl.signal);
		await reload();
	}
	function abortRefresh() { abortCtl?.abort(); }

	function refPrice(o: Offer): number | null {
		return cfg.referencePrices?.[o.variant] ?? null;
	}
	function discountPct(o: Offer): number | null {
		const r = refPrice(o);
		return r ? ((r - o.price_usd) / r) * 100 : null;
	}
	function isGoodDeal(o: Offer): boolean {
		// Unknown rating passes (don't require a rating we don't have).
		const d = discountPct(o);
		return d != null && d >= 25 && (o.seller_rating == null || o.seller_rating >= 4.3);
	}
	function variantLabel(id: string): string {
		return cfg.variants.find((v) => v.id === id)?.label ?? id;
	}
	/** Price per capacity unit ($/TB, $/GB), when the category + title support it. */
	function valuePerUnit(o: Offer): number | null {
		if (!cfg.unit) return null;
		const q = cfg.unit.parse(o.title);
		return q && q > 0 ? o.price_usd / q : null;
	}

	// Data-driven facets: only surface filters/options the data actually supports.
	const hasRatings = $derived(offers.some((o) => o.seller_rating != null));
	const presentConditions = $derived([...new Set(offers.map((o) => o.condition))]);
	const variantCount = $derived.by(() => {
		const m = new Map<string, number>();
		for (const o of offers) m.set(o.variant, (m.get(o.variant) ?? 0) + 1);
		return m;
	});
	const conditionCount = $derived.by(() => {
		const m = new Map<string, number>();
		for (const o of offers) m.set(o.condition, (m.get(o.condition) ?? 0) + 1);
		return m;
	});

	const filtered = $derived.by(() => {
		const out = offers.filter((o) => {
			if (variantFilter !== 'all' && o.variant !== variantFilter) return false;
			if (conditionFilter !== 'all' && o.condition !== conditionFilter) return false;
			if (hideNew && o.condition === 'new') return false;
			if (maxPrice && o.price_usd > maxPrice) return false;
			// Only exclude on rating when the offer actually has one (null passes).
			if (minSellerRating && o.seller_rating != null && o.seller_rating < minSellerRating) return false;
			if (hasRef) {
				const d = discountPct(o);
				if (minDiscountPct > -300 && d != null && d < minDiscountPct) return false;
				if (goodDealOnly && !isGoodDeal(o)) return false;
			}
			return true;
		});
		out.sort((a, b) => {
			switch (sortKey) {
				case 'price': return a.price_usd - b.price_usd;
				case 'price-desc': return b.price_usd - a.price_usd;
				case 'value': return (valuePerUnit(a) ?? 1e12) - (valuePerUnit(b) ?? 1e12);
				case 'rating': return (b.seller_rating ?? 0) - (a.seller_rating ?? 0);
				case 'last_seen': return b.last_seen - a.last_seen;
				case 'discount':
				default: return (discountPct(b) ?? -1e9) - (discountPct(a) ?? -1e9);
			}
		});
		return out;
	});

	function fmtPrice(n: number) { return `$${n.toLocaleString(undefined, { maximumFractionDigits: 0 })}`; }
	function fmtDate(ms: number) {
		const d = Math.floor((Date.now() - ms) / 60000);
		if (d < 1) return 'just now';
		if (d < 60) return `${d}m ago`;
		const h = Math.floor(d / 60);
		if (h < 24) return `${h}h ago`;
		return `${Math.floor(h / 24)}d ago`;
	}
	const busy = $derived(progress?.phase === 'discovering' || progress?.phase === 'enriching');
</script>

<svelte:head>
	<title>sortsafe · {cfg.short}</title>
	<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
</svelte:head>

<div class="page">
	<header class="hdr">
		<h1><a class="back" href="/">sortsafe</a> <span class="sep">/</span> {cfg.name}</h1>
		<div class="actions">
			{#if busy}
				<button class="abort" onclick={abortRefresh}>Abort</button>
				<span class="progress">
					{progress?.phase}{#if progress?.variant} · {progress.variant}{/if}{#if progress?.asinsTotal != null} · {progress.asinsDone}/{progress.asinsTotal}{/if}{#if progress?.offersInserted != null} · {progress.offersInserted} offers{/if}
				</span>
			{:else}
				<button class="refresh" onclick={doRefresh}>Refresh from Amazon</button>
				{#if progress?.phase === 'done'}
					<span class="progress done">done — {progress.offersInserted} offers</span>
				{:else if progress?.phase === 'error'}
					<span class="progress err">error: {progress.error}</span>
				{/if}
			{/if}
		</div>
	</header>

	<section class="filters">
		<label>
			<span>{cfg.referencePrices ? 'Model' : 'Type'}</span>
			<select bind:value={variantFilter}>
				<option value="all">All ({offers.length})</option>
				{#each cfg.variants.filter((v) => (variantCount.get(v.id) ?? 0) > 0) as v (v.id)}
					<option value={v.id}>{v.label} ({variantCount.get(v.id)})</option>
				{/each}
			</select>
		</label>
		{#if presentConditions.length > 1}
			<label>
				<span>Condition</span>
				<select bind:value={conditionFilter}>
					<option value="all">All</option>
					{#each presentConditions as c}
						<option value={c}>{CONDITION_LABELS[c as Condition]} ({conditionCount.get(c)})</option>
					{/each}
				</select>
			</label>
		{/if}
		<label>
			<span>Max price <output>{fmtPrice(maxPrice)}</output></span>
			<input type="range" min="0" max={priceCeiling} step="25" bind:value={maxPrice} />
		</label>
		{#if hasRef}
			<label>
				<span>Min % off {cfg.referenceLabel} <output>{minDiscountPct <= -300 ? 'Any' : `${minDiscountPct}%`}</output></span>
				<input type="range" min="-300" max="80" step="5" bind:value={minDiscountPct} />
			</label>
		{/if}
		{#if hasRatings}
			<label>
				<span>Min seller ★ <output>{minSellerRating.toFixed(1)}</output></span>
				<input type="range" min="0" max="5" step="0.1" bind:value={minSellerRating} />
			</label>
		{/if}
		{#if presentConditions.length > 1}
			<label class="checkbox"><input type="checkbox" bind:checked={hideNew} /><span>Hide "new"</span></label>
		{/if}
		{#if hasRef}
			<label class="checkbox"><input type="checkbox" bind:checked={goodDealOnly} /><span>Good deals only (≥25% off)</span></label>
		{/if}
		<label class="sort">
			<span>Sort</span>
			<select bind:value={sortKey}>
				{#if hasRef}<option value="discount">% off {cfg.referenceLabel} ↓</option>{/if}
				<option value="price">Price ↑</option>
				<option value="price-desc">Price ↓</option>
				{#if cfg.unit}<option value="value">$/{cfg.unit.per} ↑</option>{/if}
				{#if hasRatings}<option value="rating">Seller ★ ↓</option>{/if}
				<option value="last_seen">Recently seen ↓</option>
			</select>
		</label>
	</section>

	<p class="count">{filtered.length} / {offers.length} offers</p>

	<section class="grid">
		{#each filtered as o (o.offer_id)}
			{@const d = discountPct(o)}
			{@const good = isGoodDeal(o)}
			{@const prod = products.get(o.asin)}
			{@const vpu = valuePerUnit(o)}
			<a class="card" class:good href={`https://www.amazon.com/dp/${o.asin}`} target="_blank" rel="noopener">
				<div class="thumb" class:empty={!prod?.thumbnail_url}>
					{#if prod?.thumbnail_url}<img src={prod.thumbnail_url} alt={o.title || o.asin} loading="lazy" />{/if}
				</div>
				<div class="meta">
					<div class="row1">
						<span class="model">{variantLabel(o.variant)}</span>
						<span class="price">{fmtPrice(o.price_usd)}</span>
						{#if d != null}<span class="discount" class:positive={d > 0} class:negative={d < 0}>{d > 0 ? '−' : '+'}{Math.abs(d).toFixed(0)}%</span>{/if}
						{#if vpu != null}<span class="vpu">${vpu < 10 ? vpu.toFixed(2) : Math.round(vpu)}/{cfg.unit?.per}</span>{/if}
						{#if good}<span class="badge">DEAL</span>{/if}
					</div>
					{#if o.title}<div class="ptitle">{o.title}</div>{/if}
					<div class="row2">
						<span class="cond">{CONDITION_LABELS[o.condition as Condition]}</span>
						{#if o.seller}<span class="seller">{o.seller}</span>{/if}
						{#if o.seller_rating != null}<span class="srating">{o.seller_rating.toFixed(1)}★ ({o.seller_rating_count ?? 0})</span>{/if}
					</div>
					<div class="row3">
						{#if o.ships_from}<span>ships from {o.ships_from}</span>{/if}
						<span class="seen">seen {fmtDate(o.last_seen)}</span>
					</div>
				</div>
			</a>
		{/each}
		{#if filtered.length === 0}
			<p class="empty">
				{offers.length === 0 ? 'No offers yet — click "Refresh from Amazon" to pull live listings.' : 'No matches. Loosen the filters.'}
			</p>
		{/if}
	</section>
</div>

<style>
	.page { min-height: 100vh; background: var(--bg-primary); color: var(--text-primary); padding: 1rem; max-width: 1400px; margin: 0 auto; }
	.hdr { display: flex; justify-content: space-between; align-items: center; gap: 1rem; flex-wrap: wrap; margin-bottom: 0.75rem; }
	.hdr h1 { margin: 0; font-size: 1.3rem; font-weight: 600; }
	.back { color: var(--accent); text-decoration: none; }
	.sep { color: var(--text-muted); }
	.actions { display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; }
	.refresh, .abort { background: var(--accent); color: var(--bg-primary); border: 0; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 0.9rem; }
	.abort { background: var(--danger); color: #fff; }
	.progress { color: var(--text-secondary); font-size: 0.8rem; font-variant-numeric: tabular-nums; }
	.progress.done { color: var(--success); }
	.progress.err { color: var(--danger); }
	.filters { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 0.6rem; padding: 0.75rem; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 6px; margin-bottom: 0.75rem; }
	.filters label { display: flex; flex-direction: column; gap: 0.2rem; font-size: 0.75rem; color: var(--text-secondary); }
	.filters label span { display: flex; justify-content: space-between; }
	.filters output { font-variant-numeric: tabular-nums; color: var(--text-primary); }
	.filters input[type='range'] { width: 100%; accent-color: var(--accent); }
	.filters select { background: var(--bg-tertiary); border: 1px solid var(--border); color: var(--text-primary); padding: 0.35rem 0.5rem; border-radius: 4px; font-size: 0.85rem; }
	.filters .checkbox { flex-direction: row; align-items: center; gap: 0.5rem; }
	.filters .checkbox input { accent-color: var(--accent); }
	.count { color: var(--text-muted); font-size: 0.8rem; margin: 0.25rem 0 0.75rem; }
	.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 0.6rem; }
	.card { display: flex; flex-direction: column; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; text-decoration: none; color: inherit; transition: transform 0.15s, border-color 0.15s; }
	.card:hover { transform: translateY(-2px); border-color: var(--accent); }
	.card.good { border-color: var(--deal-green); box-shadow: 0 0 0 1px var(--deal-green) inset; }
	.thumb { aspect-ratio: 1.4; background: #fff; display: flex; align-items: center; justify-content: center; }
	.thumb img { max-width: 100%; max-height: 100%; object-fit: contain; }
	.thumb.empty { background: var(--bg-tertiary); }
	.thumb.empty::after { content: 'no image'; color: var(--text-muted); font-size: 0.72rem; letter-spacing: 0.03em; }
	.vpu { color: var(--text-secondary); font-size: 0.8rem; font-variant-numeric: tabular-nums; }
	.meta { padding: 0.55rem; display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.78rem; }
	.row1 { display: flex; gap: 0.4rem; align-items: baseline; flex-wrap: wrap; font-variant-numeric: tabular-nums; }
	.model { color: var(--text-secondary); font-weight: 600; }
	.price { color: var(--accent); font-weight: 700; font-size: 0.95rem; }
	.discount.positive { color: var(--deal-green); font-weight: 600; }
	.discount.negative { color: var(--danger); font-weight: 600; }
	.badge { background: var(--deal-green); color: var(--bg-primary); font-size: 0.65rem; padding: 0.05rem 0.35rem; border-radius: 3px; font-weight: 700; letter-spacing: 0.04em; }
	.ptitle { color: var(--text-secondary); font-size: 0.72rem; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; line-height: 1.25; }
	.row2 { display: flex; gap: 0.4rem; align-items: center; flex-wrap: wrap; font-size: 0.72rem; }
	.cond { color: var(--text-primary); }
	.seller { color: var(--text-secondary); }
	.srating { color: var(--star); }
	.row3 { display: flex; gap: 0.5rem; flex-wrap: wrap; font-size: 0.65rem; color: var(--text-muted); }
	.seen { margin-left: auto; }
	.empty { grid-column: 1 / -1; text-align: center; color: var(--text-muted); padding: 2rem; }
	@media (max-width: 600px) { .grid { grid-template-columns: 1fr; } .filters { grid-template-columns: 1fr 1fr; } }
</style>
