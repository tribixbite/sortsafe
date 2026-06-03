/** Category registry — each entry drives search, classification, and the UI. */

export interface VariantDef {
  id: string;
  label: string;
  /** Amazon search term used to discover ASINs for this variant. */
  search: string;
  /** Title must match this to be classified into the variant. */
  match: RegExp;
}

export interface CategoryConfig {
  slug: string;
  name: string; // page H1, e.g. "GPUs"
  short: string; // landing card title
  desc: string; // landing card description
  variants: VariantDef[];
  /** Titles matching this are parts/accessories, not the product — rejected. */
  accessory: RegExp;
  /** Reference price per variant (only where a real launch MSRP exists). */
  referencePrices?: Record<string, number>;
  referenceLabel?: string; // e.g. "MSRP"
}

export const CATEGORIES: CategoryConfig[] = [
  {
    slug: "gpus",
    name: "GPUs — 3090 / 4090 / 5090",
    short: "GPUs",
    desc: "RTX 3090 / 4090 / 5090 — new, used, refurbished, Amazon Warehouse",
    accessory:
      /backplate|water\s*block|waterblock|fan only|^cable|riser|bracket|mount adapter|cooler\s+(?!master\s+rtx)|gpu support|anti.?sag|holder/i,
    referenceLabel: "MSRP",
    referencePrices: { "3090": 1499, "4090": 1599, "5090": 1999 },
    variants: [
      { id: "5090", label: "RTX 5090", search: "rtx 5090", match: /\b(rtx|geforce)\s*5090\b/i },
      { id: "4090", label: "RTX 4090", search: "rtx 4090", match: /\b(rtx|geforce)\s*4090\b/i },
      { id: "3090", label: "RTX 3090", search: "rtx 3090", match: /\b(rtx|geforce)\s*3090\b/i },
    ],
  },
  {
    slug: "ram",
    name: "RAM — DDR5 / DDR4 kits",
    short: "RAM",
    desc: "Desktop DDR5 & DDR4 memory kits — by generation, new & used",
    accessory: /heat\s*sink only|rgb fan|cooler|^cable|adapter|so-?dimm|laptop|server ecc|fan kit/i,
    variants: [
      { id: "DDR5", label: "DDR5", search: "ddr5 desktop ram kit", match: /\bddr5\b/i },
      { id: "DDR4", label: "DDR4", search: "ddr4 desktop ram kit", match: /\bddr4\b/i },
    ],
  },
  {
    slug: "ssd",
    name: "SSDs — NVMe by capacity",
    short: "SSDs",
    desc: "NVMe solid-state drives — 1TB / 2TB / 4TB, new & used",
    accessory: /enclosure|heat\s*sink only|adapter|^cable|dock|caddy|bracket|screw/i,
    variants: [
      { id: "4TB", label: "4 TB", search: "nvme ssd 4tb", match: /\b4\s*tb\b/i },
      { id: "2TB", label: "2 TB", search: "nvme ssd 2tb", match: /\b2\s*tb\b/i },
      { id: "1TB", label: "1 TB", search: "nvme ssd 1tb", match: /\b1\s*tb\b/i },
    ],
  },
  {
    slug: "cpu",
    name: "CPUs — Ryzen / Core",
    short: "CPUs",
    desc: "Desktop processors — AMD Ryzen & Intel Core, new & used",
    // Reject standalone coolers/boards but NOT CPUs that merely mention an
    // included cooler ("with Wraith Cooler").
    accessory: /cpu cooler|air cooler|liquid cooler|\baio\b|thermal (paste|compound)|heat\s*sink|water\s*block|motherboard|\bmobo\b/i,
    variants: [
      { id: "Ryzen", label: "AMD Ryzen", search: "amd ryzen desktop processor", match: /\bryzen\b/i },
      {
        id: "Core",
        label: "Intel Core",
        search: "intel core desktop processor",
        match: /\bintel\s+core|\bcore\s?(ultra|i[3579])\b/i,
      },
    ],
  },
];

export function getCategory(slug: string): CategoryConfig | undefined {
  return CATEGORIES.find((c) => c.slug === slug);
}

/** Classify a product title into a variant id, or null if it's an accessory /
 *  doesn't match any variant. */
export function classify(cfg: CategoryConfig, title: string): string | null {
  if (!title) return null;
  if (cfg.accessory.test(title)) return null;
  for (const v of cfg.variants) if (v.match.test(title)) return v.id;
  return null;
}
