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
// Listbox, per entity. THE source of that decision — the live strip and
// the loading shell both read it, so they cannot disagree.
//
// This replaced a ResizeObserver in EntityTabs. Measuring was more
// adaptive but could only report after mount, and it initialised to
// "fits": every first paint drew the chip strip and then snapped to the
// select at widths where the strip did not fit. The loading shell could
// not measure at all, so it drew a strip across the same range — on a
// chemical page, every width from 640px to 1200px.
//
// A CSS breakpoint is less clever and always agrees with itself, which is
// what this needs. Browser zoom is not a factor — it scales the viewport
// and these rem-based chips together. A non-default ROOT FONT SIZE is:
// it widens the chips without changing the viewport in CSS px, so the
// strip can need more room than the number assumes. Both surfaces then
// render the same too-wide strip rather than disagreeing, and the
// overflow-x-auto wrapper they share lets it scroll instead of pushing
// the page wider.
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
// `stripFlex` is for a container that IS the chip row (the loading
// shell); `stripBlock` is for one that wraps it (the live TabList).
// Both encode the same width — the parity test below pins that.
export const TAB_STRIP_FITS: Record<
  EntityType,
  { select: string; stripFlex: string; stripBlock: string }
> = {
  // Three short labels; the strip fits from the first width at which it
  // is shown at all, so this is just the mobile breakpoint (sm, 640px).
  food: {
    select: "min-[640px]:hidden",
    stripFlex: "hidden min-[640px]:flex",
    stripBlock: "hidden min-[640px]:block",
  },
  bioactivity: {
    select: "min-[950px]:hidden",
    stripFlex: "hidden min-[950px]:flex",
    stripBlock: "hidden min-[950px]:block",
  },
  disease: {
    select: "min-[1100px]:hidden",
    stripFlex: "hidden min-[1100px]:flex",
    stripBlock: "hidden min-[1100px]:block",
  },
  chemical: {
    select: "min-[1200px]:hidden",
    stripFlex: "hidden min-[1200px]:flex",
    stripBlock: "hidden min-[1200px]:block",
  },
};

// Geometry of a tab's count badge, shared with the live strip so the
// loading shell reserves exactly the box the real badge will occupy.
// A mismatch here resizes every chip at the handoff, which re-runs the
// overflow measurement above and can flip the strip mid-load.
export const TAB_BADGE_W = "w-[2.5rem] shrink-0";
