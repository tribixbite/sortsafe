export type GpuModel = '3090' | '4090' | '5090';
export type GpuCondition = 'new' | 'used-like-new' | 'used-very-good' | 'used-good' | 'used-acceptable' | 'refurbished' | 'warehouse' | 'unknown';

export interface GpuOffer {
	offer_id: string;       // composite: `${asin}__${seller_id || hashOf(condition+price)}`
	asin: string;
	model: GpuModel;
	title: string;
	condition: GpuCondition;
	condition_note: string | null;   // free-text seller note
	price_usd: number;
	currency: string;                 // 'USD' assumed for now
	seller: string | null;
	seller_id: string | null;
	seller_rating: number | null;     // 0..5
	seller_rating_count: number | null;
	ships_from: string | null;
	delivery_text: string | null;
	first_seen: number;               // epoch ms
	last_seen: number;                // epoch ms
	is_buybox: boolean;
}

export interface GpuProduct {
	asin: string;
	model: GpuModel;
	title: string;
	thumbnail_url: string | null;
	last_refreshed: number;           // epoch ms
}

export interface PriceSnapshot {
	id?: number;
	asin: string;
	condition: GpuCondition;
	price_usd: number;
	taken_at: number;
}

export interface ModelMsrp {
	model: GpuModel;
	msrp_usd: number;
	source: string;                   // 'launch' | 'user-override'
}

export const DEFAULT_MSRP: Record<GpuModel, number> = {
	'3090': 1499,
	'4090': 1599,
	'5090': 1999,
};

export const CONDITION_LABELS: Record<GpuCondition, string> = {
	'new': 'New',
	'used-like-new': 'Used — Like New',
	'used-very-good': 'Used — Very Good',
	'used-good': 'Used — Good',
	'used-acceptable': 'Used — Acceptable',
	'refurbished': 'Refurbished',
	'warehouse': 'Amazon Warehouse',
	'unknown': 'Unknown',
};
