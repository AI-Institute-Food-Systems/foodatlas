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
  // Naming rule: a tab is named for what it LISTS, not for its topic or
  // its method. So food and chemical read as mirror images, and the same
  // word means the same kind of row everywhere.
  //
  // The assay-inferred tabs are deliberately absent from chemical and
  // disease. They listed the same rows as the literature tabs from a
  // weaker source, and rendered `therapeutic` and `marker/mechanism` —
  // opposite directions — as identical grey chips, with marker/mechanism
  // being 91% of rows. A tab that presents mostly not-a-benefit evidence
  // as neutral is worse than no tab. The sections and their fetchers are
  // still here; restore the entries once the direction, protein target
  // and source-assay work from the bioactivity-disease branch lands.
  food: [
    { id: "composition", label: "Chemicals", hasCount: true },
    { id: "bioactivities", label: "Bioactivities", hasCount: true },
    { id: "overview", label: "Metadata", hasCount: false },
  ],
  chemical: [
    { id: "composition", label: "Foods", hasCount: true },
    { id: "bioactivities", label: "Bioactivities", hasCount: true },
    { id: "health", label: "Diseases", hasCount: true },
    { id: "overview", label: "Metadata", hasCount: false },
  ],
  disease: [
    { id: "health", label: "Chemicals", hasCount: true },
    { id: "bioactivities", label: "Bioactivities", hasCount: true },
    { id: "overview", label: "Metadata", hasCount: false },
  ],
  bioactivity: [
    { id: "foods", label: "Foods", hasCount: true },
    { id: "chemicals", label: "Chemicals", hasCount: true },
    { id: "diseases", label: "Diseases", hasCount: true },
    { id: "overview", label: "Metadata", hasCount: false },
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
// not measure at all, so it drew a strip across the same range.
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
// Each number is the strip's measured natural width plus the container
// inset, rounded up. Measured, not derived: it depends on label length as
// well as tab count. The container is `px-4 md:px-24` inside `max-w-5xl`,
// so it is viewport-32 below 768px and viewport-192 above, capped at
// 1024 — which is why the strip can fit just under 768px, stop fitting at
// 768px, and fit again later. Rounding up puts the select across that gap.
//
// To re-measure: render the chip row with the built CSS at a wide
// viewport and read its natural width, then add
//   192  container inset (px-24 both sides)
//    15  the vertical scrollbar, which entity pages always have and a
//        short measuring harness does not
//    ~40 headroom
// A threshold set to the bare sum sits exactly on the boundary, where
// rounding or a slightly wider glyph clips the last chip against the
// card edge. That is how 900px shipped for chemical and still clipped.
//
// Literal class strings: Tailwind only emits what it can see in source, so
// these cannot be built by interpolation. Re-measure if a tab label
// changes materially.
//
// `stripFlex` is for a container that IS the chip row (the loading
// shell); `stripBlock` is for one that wraps it (the live TabList).
// Both encode the same width — the parity test pins that.
export const TAB_STRIP_FITS: Record<
  EntityType,
  { select: string; stripFlex: string; stripBlock: string }
> = {
  // Three short labels; fits at every width it is shown at.
  food: {
    select: "min-[640px]:hidden",
    stripFlex: "hidden min-[640px]:flex",
    stripBlock: "hidden min-[640px]:block",
  },
  // Three short labels, same as food now that the assay tab is gone.
  disease: {
    select: "min-[640px]:hidden",
    stripFlex: "hidden min-[640px]:flex",
    stripBlock: "hidden min-[640px]:block",
  },
  // Four labels, all single words.
  bioactivity: {
    select: "min-[825px]:hidden",
    stripFlex: "hidden min-[825px]:flex",
    stripBlock: "hidden min-[825px]:block",
  },
  // Four labels; the widest strip, but far narrower than the 1025px the
  // qualified "Diseases (Literature)" wording needed.
  chemical: {
    select: "min-[850px]:hidden",
    stripFlex: "hidden min-[850px]:flex",
    stripBlock: "hidden min-[850px]:block",
  },
};

// Geometry of a tab's count badge, shared with the live strip so the
// loading shell reserves exactly the box the real badge will occupy.
// A mismatch here resizes every chip at the handoff, which re-runs the
// overflow measurement above and can flip the strip mid-load.
export const TAB_BADGE_W = "w-[2.5rem] shrink-0";
