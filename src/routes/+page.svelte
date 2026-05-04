<script lang="ts">
	// Landing page — links to each category. Once the multi-category pipeline
	// lands, this can become a unified deals dashboard with cross-category
	// "best right now" cards.
	const categories = [
		{ slug: 'gpus', name: 'GPUs', desc: '3090 / 4090 / 5090 — used, refurbished, Amazon Warehouse', enabled: true },
		{ slug: 'ram', name: 'RAM', desc: 'DDR5 kits — by speed, capacity, CAS latency', enabled: false },
		{ slug: 'ssd', name: 'SSDs', desc: 'NVMe Gen4/5 — by form factor, interface, capacity', enabled: false },
	];
</script>

<svelte:head>
	<title>sortsafe — deal sniper</title>
</svelte:head>

<main class="page">
	<header>
		<h1>sortsafe</h1>
		<p>real-time amazon deal sniper · used / refurb / warehouse pricing</p>
	</header>

	<div class="grid">
		{#each categories as c (c.slug)}
			{#if c.enabled}
				<a class="card" href="/{c.slug}/">
					<h2>{c.name}</h2>
					<p>{c.desc}</p>
				</a>
			{:else}
				<div class="card disabled">
					<h2>{c.name}</h2>
					<p>{c.desc}</p>
					<span class="soon">coming soon</span>
				</div>
			{/if}
		{/each}
	</div>
</main>

<style>
	.page { min-height: 100vh; background: var(--bg-primary); color: var(--text-primary); padding: 2rem 1rem; max-width: 900px; margin: 0 auto; }
	header { margin-bottom: 2rem; }
	header h1 { font-size: 2rem; margin: 0 0 0.25rem; font-weight: 700; letter-spacing: -0.02em; }
	header p { color: var(--text-secondary); margin: 0; font-size: 0.95rem; }
	.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 1rem; }
	.card { display: block; padding: 1.25rem; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 10px; text-decoration: none; color: inherit; transition: border-color 0.15s, transform 0.15s; }
	.card:hover { border-color: var(--accent); transform: translateY(-2px); }
	.card.disabled { opacity: 0.55; cursor: default; }
	.card h2 { margin: 0 0 0.4rem; font-size: 1.15rem; font-weight: 600; }
	.card p { margin: 0; color: var(--text-secondary); font-size: 0.85rem; line-height: 1.4; }
	.soon { display: inline-block; margin-top: 0.5rem; font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.06em; }
</style>
