// The tab strip of every entity page, declared once.
//
// Plain TS with no React import and no "use client", so this is readable
// from Server Components — the route `loading.tsx` shells and `page.tsx`
// both derive their tabs from here. That shared derivation is the point:
// the loading skeleton used to hard-code its own `tabCount`, which
// disagreed with the real page on three routes out of four and made the
// tab strip visibly grow when the skeleton handed off to the SSR shell.

export type EntityType = "food" | "chemical" | "disease" | "bioactivity";

export type EntityTabDef = {
  readonly id: string;
  readonly label: string;
  // Whether this tab ever shows a count badge. Drives the badge
  // placeholder in both the loading shell and the live strip, so a chip's
  // width is final from first paint instead of growing when its count
  // lands. Distinct from "the count is currently null", which means
  // pending — every tab below except Overview eventually publishes one.
  readonly hasCount: boolean;
};

// Order here is the rendered order, for the loading shell and the live
// page alike. Adding or reordering a tab is a one-line change here.
export const ENTITY_TABS = {
  food: [
    { id: "composition", label: "Composition", hasCount: true },
    { id: "bioactivities", label: "Bioactivities", hasCount: true },
    { id: "overview", label: "IDs & Metadata", hasCount: false },
  ],
  chemical: [
    { id: "composition", label: "Foods Containing", hasCount: true },
    { id: "bioactivities", label: "Bioactivities", hasCount: true },
    { id: "health", label: "Health Impacts", hasCount: true },
    { id: "assay-inferred", label: "Diseases (assay-inferred)", hasCount: true },
    { id: "overview", label: "IDs & Metadata", hasCount: false },
  ],
  disease: [
    { id: "health", label: "Health Impacts", hasCount: true },
    {
      id: "assay-inferred",
      label: "Chemicals (assay-inferred)",
      hasCount: true,
    },
    { id: "bioactivities", label: "Bioactivities", hasCount: true },
    { id: "overview", label: "IDs & Metadata", hasCount: false },
  ],
  bioactivity: [
    { id: "foods", label: "Foods Exhibiting", hasCount: true },
    { id: "chemicals", label: "Chemicals Measured", hasCount: true },
    { id: "diseases", label: "Diseases", hasCount: true },
    { id: "overview", label: "IDs & Metadata", hasCount: false },
  ],
} as const satisfies Record<EntityType, readonly EntityTabDef[]>;

export const DEFAULT_TAB_ID: Record<EntityType, string> = {
  food: "composition",
  chemical: "composition",
  disease: "health",
  bioactivity: "foods",
};

// The literal tab ids available on a given entity, so `buildTabs` can
// demand exactly the right set and a typo becomes a compile error.
export type TabIdOf<E extends EntityType> =
  (typeof ENTITY_TABS)[E][number]["id"];
