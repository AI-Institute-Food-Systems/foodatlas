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

// Width at which the chip strip stops overflowing and replaces the
// Listbox, per entity.
//
// EntityTabs decides this by measuring at runtime, which a server-rendered
// loading shell cannot do. The shell used to assume `sm` (640px) and so
// drew a chip strip wherever the real page was still drawing a select — on
// a chemical page that was every width from 640px to 1200px.
//
// The numbers are measured, not derived: the threshold depends on label
// length as well as tab count, which is why disease and bioactivity differ
// despite both having four tabs. Each is rounded UP to the first width at
// which the strip was observed to fit, so the shell errs toward the select
// — the form the real page shows more often, and the safer guess because
// the select is a fixed-height block that any strip width can replace
// without reflowing the page.
//
// Literal class strings: Tailwind only emits what it can see in source, so
// these cannot be built by interpolation. Re-measure if a tab label
// changes materially.
export const TAB_STRIP_FITS: Record<
  EntityType,
  { select: string; strip: string }
> = {
  // Three short labels; the strip fits from the first width at which it
  // is shown at all, so this is just the mobile breakpoint (sm, 640px).
  food: { select: "min-[640px]:hidden", strip: "hidden min-[640px]:flex" },
  bioactivity: {
    select: "min-[950px]:hidden",
    strip: "hidden min-[950px]:flex",
  },
  disease: {
    select: "min-[1100px]:hidden",
    strip: "hidden min-[1100px]:flex",
  },
  chemical: {
    select: "min-[1200px]:hidden",
    strip: "hidden min-[1200px]:flex",
  },
};

// Geometry of a tab's count badge, shared with the live strip so the
// loading shell reserves exactly the box the real badge will occupy.
// A mismatch here resizes every chip at the handoff, which re-runs the
// overflow measurement above and can flip the strip mid-load.
export const TAB_BADGE_W = "w-[2.5rem] shrink-0";
