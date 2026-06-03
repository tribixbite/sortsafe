import { error } from "@sveltejs/kit";
import { getCategory } from "$lib/catalog/categories";

export const ssr = false;
export const prerender = false;

export function load({ params }: { params: { category: string } }) {
  const cfg = getCategory(params.category);
  if (!cfg) throw error(404, `Unknown category "${params.category}"`);
  return { slug: cfg.slug };
}
