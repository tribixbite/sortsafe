<script lang="ts">
	import { CATEGORIES } from '$lib/catalog/categories';
</script>

<svelte:head>
	<title>sortsafe — deal sniper</title>
	<meta name="description" content="Real-time Amazon deal sniper across GPUs, RAM, SSDs and CPUs — used / refurbished / Warehouse pricing." />
</svelte:head>

<main class="page">
	<header>
		<h1>sortsafe</h1>
		<p>real-time amazon deal sniper · used / refurb / warehouse pricing</p>
	</header>

	<div class="grid">
		{#each CATEGORIES as c (c.slug)}
			<a class="card" href="/{c.slug}/">
				<h2>{c.short}</h2>
				<p>{c.desc}</p>
			</a>
		{/each}
	</div>

	<footer>Prices scraped live from Amazon via a CORS proxy · client-side, no tracking</footer>
</main>

<style>
	.page { min-height: 100vh; background: var(--bg-primary); color: var(--text-primary); padding: 2rem 1rem; max-width: 900px; margin: 0 auto; }
	header { margin-bottom: 2rem; }
	header h1 { font-size: 2rem; margin: 0 0 0.25rem; font-weight: 700; letter-spacing: -0.02em; }
	header p { color: var(--text-secondary); margin: 0; font-size: 0.95rem; }
	.grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 1rem; }
	.card { display: block; padding: 1.25rem; background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 10px; text-decoration: none; color: inherit; transition: border-color 0.15s, transform 0.15s; }
	.card:hover { border-color: var(--accent); transform: translateY(-2px); }
	.card h2 { margin: 0 0 0.4rem; font-size: 1.15rem; font-weight: 600; }
	.card p { margin: 0; color: var(--text-secondary); font-size: 0.85rem; line-height: 1.4; }
	footer { margin-top: 2rem; color: var(--text-muted); font-size: 0.75rem; }
</style>
