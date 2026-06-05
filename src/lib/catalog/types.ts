/** Generic catalog types — shared across all categories (GPU/RAM/SSD/CPU). */

export type Condition =
  | "new"
  | "used-like-new"
  | "used-very-good"
  | "used-good"
  | "used-acceptable"
  | "refurbished"
  | "warehouse"
  | "unknown";

export const CONDITION_LABELS: Record<Condition, string> = {
  new: "New",
  "used-like-new": "Used — Like New",
  "used-very-good": "Used — Very Good",
  "used-good": "Used — Good",
  "used-acceptable": "Used — Acceptable",
  refurbished: "Refurbished",
  warehouse: "Amazon Warehouse",
  unknown: "Unknown",
};

export interface Offer {
  offer_id: string;
  asin: string;
  category: string;
  /** Bucket within the category, e.g. '4090', 'DDR5', '2TB', 'Ryzen'. */
  variant: string;
  title: string;
  condition: Condition;
  condition_note: string | null;
  price_usd: number;
  currency: string;
  seller: string | null;
  seller_id: string | null;
  seller_rating: number | null;
  seller_rating_count: number | null;
  ships_from: string | null;
  delivery_text: string | null;
  first_seen: number;
  last_seen: number;
  is_buybox: boolean;
  /** Client-only: true when this record came from the static seed (not a live
   *  user refresh). Lets hydrate prune the previous seed cleanly. */
  seeded?: boolean;
}

export interface Product {
  asin: string;
  category: string;
  variant: string;
  title: string;
  thumbnail_url: string | null;
  last_refreshed: number;
  seeded?: boolean;
}

export interface PriceSnapshot {
  id?: number;
  asin: string;
  condition: Condition;
  price_usd: number;
  taken_at: number;
}

/** On-disk seed shape (static/<slug>-seed.json). */
export interface SeedFile {
  generated_at: number;
  offers: Offer[];
  products: Product[];
}
