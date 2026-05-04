<script lang="ts">
	import { onMount } from 'svelte';
	import type { GpuModel, GpuOffer, GpuProduct } from '$lib/gpus/types';
	import { CONDITION_LABELS, DEFAULT_MSRP } from '$lib/gpus/types';
	import { getAllOffers, getAllProducts, getMsrp, setMsrp, hydrateFromSeed } from '$lib/gpus/db';
	import { refreshAll, type RefreshProgress } from '$lib/gpus/refresh';

	let offers: GpuOffer[] = $state([]);
	let products: Map<string, GpuProduct> = $state(new Map());
	let msrp: Record<GpuModel, number> = $state({ ...DEFAULT_MSRP });
	let progress: RefreshProgress | null = $state(null);

	let modelFilter: GpuModel | 'all' = $state('all');
	let conditionFilter = $state('all');
	let hideNew = $state(false);
	// Default well below 0 so above-MSRP listings (most 5090/4090 today) are visible.
	let minDiscountPct = $state(-300);
	let minSellerRating = $state(0);
	let goodDealOnly = $state(false);
	let sortKey: 'price' | 'discount' | 'rating' | 'last_seen' = $state('discount');

	let abortCtl: AbortController | null = null;

	onMount(async () => {
		const hydrated = await hydrateFromSeed().catch(() => null);
		await reload();
		if (hydrated) {
			progress = { phase: 'done', offersInserted: hydrated.inserted, asinsTotal: products.size, asinsDone: products.size };
		}
	});

	async function reload() {
		offers = await getAllOffers();
		const prods = await getAllProducts();
		products = new Map(prods.map((p) => [p.asin, p]));
		msrp = await getMsrp();
	}

	async function doRefresh() {
		if (progress?.phase === 'discovering' || progress?.phase === 'enriching') return;
		abortCtl = new AbortController();
		progress = { phase: 'discovering' };
		await refreshAll((p) => { progress = p; }, abortCtl.signal);
		await reload();
	}

	function abortRefresh() { abortCtl?.abort(); }

	async function updateMsrp(model: GpuModel, val: number) {
		await setMsrp(model, val);
		msrp = { ...msrp, [model]: val };
	}

	function discountPct(o: GpuOffer): number {
		const m = msrp[o.model] ?? DEFAULT_MSRP[o.model];
		return ((m - o.price_usd) / m) * 100;
	}

	function isGoodDeal(o: GpuOffer): boolean {
		const d = discountPct(o);
		const ratingOk = (o.seller_rating ?? 0) >= 4.3;
		return d >= 25 && ratingOk;
	}

	const filtered = $derived.by(() => {
		const out = offers.filter((o) => {
			if (modelFilter !== 'all' && o.model !== modelFilter) return false;
			if (conditionFilter !== 'all' && o.condition !== conditionFilter) return false;
			if (hideNew && o.condition === 'new') return false;
			if (discountPct(o) < minDiscountPct) return false;
			if ((o.seller_rating ?? 0) < minSellerRating) return false;
			if (goodDealOnly && !isGoodDeal(o)) return false;
			return true;
		});
		out.sort((a, b) => {
			switch (sortKey) {
				case 'price': return a.price_usd - b.price_usd;
				case 'rating': return (b.seller_rating ?? 0) - (a.seller_rating ?? 0);
				case 'last_seen': return b.last_seen - a.last_seen;
				case 'discount':
				default: return discountPct(b) - discountPct(a);
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
</script>

<svelte:head>
	<title>GPU deal browser</title>
	<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
</svelte:head>

<div class="page">
	<header class="hdr">
		<h1>GPU deals — 3090 / 4090 / 5090</h1>
		<div class="actions">
			{#if progress?.phase === 'discovering' || progress?.phase === 'enriching'}
				<button class="abort" onclick={abortRefresh}>Abort</button>
				<span class="progress">
					{progress.phase}
					{#if progress.model} · {progress.model}{/if}
					{#if progress.asin} · {progress.asin}{/if}
					{#if progress.asinsTotal != null} · {progress.asinsDone}/{progress.asinsTotal} ASINs{/if}
					{#if progress.offersInserted != null} · {progress.offersInserted} offers{/if}
				</span>
			{:else}
				<button class="refresh" onclick={doRefresh}>Refresh from Amazon</button>
				{#if progress?.phase === 'done'}
					<span class="progress done">done — {progress.offersInserted} offers across {progress.asinsTotal} ASINs</span>
				{:else if progress?.phase === 'error'}
					<span class="progress err">error: {progress.error}</span>
				{/if}
			{/if}
		</div>
	</header>

	<section class="msrp">
		<span class="msrp-label">MSRP:</span>
		{#each (['3090', '4090', '5090'] as GpuModel[]) as m}
			<label class="msrp-input">
				<span>{m}</span>
				<input type="number" min="100" step="50" value={msrp[m]} onchange={(e) => updateMsrp(m, parseInt((e.target as HTMLInputElement).value, 10))} />
			</label>
		{/each}
	</section>

	<section class="filters">
		<label>
			<span>Model</span>
			<select bind:value={modelFilter}>
				<option value="all">All</option>
				<option value="3090">3090</option>
				<option value="4090">4090</option>
				<option value="5090">5090</option>
			</select>
		</label>
		<label>
			<span>Condition</span>
			<select bind:value={conditionFilter}>
				<option value="all">All</option>
				{#each Object.entries(CONDITION_LABELS) as [k, v]}
					<option value={k}>{v}</option>
				{/each}
			</select>
		</label>
		<label>
			<span>Min % off MSRP</span>
			<input type="range" min="-300" max="80" step="5" bind:value={minDiscountPct} />
			<output>{minDiscountPct}%</output>
		</label>
		<label>
			<span>Min seller ★</span>
			<input type="range" min="0" max="5" step="0.1" bind:value={minSellerRating} />
			<output>{minSellerRating.toFixed(1)}</output>
		</label>
		<label class="checkbox">
			<input type="checkbox" bind:checked={hideNew} />
			<span>Hide "new" condition</span>
		</label>
		<label class="checkbox">
			<input type="checkbox" bind:checked={goodDealOnly} />
			<span>Good deals only (≥25% off + ≥4.3★)</span>
		</label>
		<label class="sort">
			<span>Sort</span>
			<select bind:value={sortKey}>
				<option value="discount">% off MSRP ↓</option>
				<option value="price">Price ↑</option>
				<option value="rating">Seller ★ ↓</option>
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
			<a class="card" class:good href={`https://www.amazon.com/dp/${o.asin}`} target="_blank" rel="noopener">
				<div class="thumb">
					{#if prod?.thumbnail_url}
						<img src={prod.thumbnail_url} alt={o.title || o.asin} loading="lazy" />
					{/if}
				</div>
				<div class="meta">
					<div class="row1">
						<span class="model">RTX {o.model}</span>
						<span class="price">{fmtPrice(o.price_usd)}</span>
						<span class="discount" class:positive={d > 0} class:negative={d < 0}>{d > 0 ? '−' : '+'}{Math.abs(d).toFixed(0)}%</span>
						{#if good}<span class="badge">DEAL</span>{/if}
					</div>
					<div class="row2">
						<span class="cond">{CONDITION_LABELS[o.condition]}</span>
						{#if o.seller}<span class="seller">{o.seller}</span>{/if}
						{#if o.seller_rating != null}<span class="srating">{o.seller_rating.toFixed(1)}★ ({o.seller_rating_count ?? 0})</span>{/if}
					</div>
					{#if o.condition_note}<div class="note" title={o.condition_note}>{o.condition_note}</div>{/if}
					<div class="row3">
						{#if o.ships_from}<span>ships from {o.ships_from}</span>{/if}
						<span class="seen">seen {fmtDate(o.last_seen)}</span>
					</div>
				</div>
			</a>
		{/each}
		{#if filtered.length === 0}
			<p class="empty">
				{offers.length === 0 ? 'No offers stored yet. Click "Refresh from Amazon" — make sure the proxy is running on :8787.' : 'No matches. Loosen the filters.'}
			</p>
		{/if}
	</section>
</div>

<style>
	.page { min-height: 100vh; background: var(--bg-primary); color: var(--text-primary); padding: 1rem; max-width: 1400px; margin: 0 auto; }
	.hdr { display: flex; justify-content: space-between; align-items: center; gap: 1rem; flex-wrap: wrap; margin-bottom: 0.75rem; }
	.hdr h1 { margin: 0; font-size: 1.4rem; font-weight: 600; }
	.actions { display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; }
	.refresh, .abort { background: var(--accent); color: var(--bg-primary); border: 0; padding: 0.5rem 1rem; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 0.9rem; }
	.abort { background: var(--danger); color: white; }
	.progress { color: var(--text-secondary); font-size: 0.8rem; font-variant-numeric: tabular-nums; }
	.progress.done { color: var(--success); }
	.progress.err { color: var(--danger); }
	.msrp { display: flex; gap: 1rem; align-items: center; padding: 0.5rem 0.75rem; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 6px; margin-bottom: 0.75rem; flex-wrap: wrap; }
	.msrp-label { color: var(--text-secondary); font-size: 0.8rem; }
	.msrp-input { display: flex; gap: 0.4rem; align-items: center; font-size: 0.85rem; }
	.msrp-input input { width: 5.5rem; background: var(--bg-tertiary); border: 1px solid var(--border); color: var(--text-primary); padding: 0.25rem 0.4rem; border-radius: 4px; font-variant-numeric: tabular-nums; }
	.filters { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 0.6rem; padding: 0.75rem; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 6px; margin-bottom: 0.75rem; }
	.filters label { display: flex; flex-direction: column; gap: 0.2rem; font-size: 0.75rem; color: var(--text-secondary); }
	.filters label span { display: flex; justify-content: space-between; }
	.filters output { font-variant-numeric: tabular-nums; color: var(--text-primary); }
	.filters input[type='range'] { width: 100%; accent-color: var(--accent); }
	.filters input[type='text'], .filters select { background: var(--bg-tertiary); border: 1px solid var(--border); color: var(--text-primary); padding: 0.35rem 0.5rem; border-radius: 4px; font-size: 0.85rem; }
	.filters .checkbox { flex-direction: row; align-items: center; gap: 0.5rem; }
	.filters .checkbox input { accent-color: var(--accent); }
	.count { color: var(--text-muted); font-size: 0.8rem; margin: 0.25rem 0 0.75rem; }
	.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 0.6rem; }
	.card { display: flex; flex-direction: column; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 8px; overflow: hidden; text-decoration: none; color: inherit; transition: transform 0.15s, border-color 0.15s; }
	.card:hover { transform: translateY(-2px); border-color: var(--accent); }
	.card.good { border-color: var(--deal-green); box-shadow: 0 0 0 1px var(--deal-green) inset; }
	.thumb { aspect-ratio: 1.4; background: #fff; display: flex; align-items: center; justify-content: center; }
	.thumb img { max-width: 100%; max-height: 100%; object-fit: contain; }
	.meta { padding: 0.55rem; display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.78rem; }
	.row1 { display: flex; gap: 0.4rem; align-items: baseline; flex-wrap: wrap; font-variant-numeric: tabular-nums; }
	.model { color: var(--text-secondary); font-weight: 600; }
	.price { color: var(--accent); font-weight: 700; font-size: 0.95rem; }
	.discount.positive { color: var(--deal-green); font-weight: 600; }
	.discount.negative { color: var(--danger); font-weight: 600; }
	.badge { background: var(--deal-green); color: var(--bg-primary); font-size: 0.65rem; padding: 0.05rem 0.35rem; border-radius: 3px; font-weight: 700; letter-spacing: 0.04em; }
	.row2 { display: flex; gap: 0.4rem; align-items: center; flex-wrap: wrap; font-size: 0.72rem; }
	.cond { color: var(--text-primary); }
	.seller { color: var(--text-secondary); }
	.srating { color: var(--star); }
	.note { color: var(--text-muted); font-size: 0.7rem; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; line-height: 1.25; }
	.row3 { display: flex; gap: 0.5rem; flex-wrap: wrap; font-size: 0.65rem; color: var(--text-muted); }
	.seen { margin-left: auto; }
	.empty { grid-column: 1 / -1; text-align: center; color: var(--text-muted); padding: 2rem; }
	@media (max-width: 600px) {
		.grid { grid-template-columns: 1fr; }
		.filters { grid-template-columns: 1fr 1fr; }
	}
</style>
