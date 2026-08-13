// Single source of truth for every loading placeholder's visual
// contract. Deliberately plain TS with no React import: these tokens are
// consumed by Server Components (route `loading.tsx` shells, `<Suspense>`
// fallbacks) and by client tables' `isLoading` branches alike, so both
// sides of every handoff render byte-identical skeletons.

export type SkeletonTone = "default" | "cream";
export type SkeletonShape = "text" | "block" | "pill";

export const SKELETON_TONE: Record<SkeletonTone, string> = {
  // Resting fill is light-800 (#21201f) — deliberately subtle, only
  // ~1.13:1 against a Card (bg-light-950). The pulse BRIGHTENS toward
  // light-700 rather than fading opacity: an opacity fade on light-800
  // bottoms out near 1.06:1, so the placeholder would vanish for half of
  // every cycle. Under prefers-reduced-motion there is no animation to
  // carry the signal, so the fill rests at light-700 instead.
  default:
    "bg-light-800 motion-safe:animate-skeleton-pulse motion-reduce:bg-light-700",
  // Stands in for genuinely cream elements (overview section chips, the
  // selected tab chip). Already high-contrast on dark, so a plain
  // opacity pulse reads fine here.
  cream: "bg-light-200/60 motion-safe:animate-pulse",
};

export const SKELETON_SHAPE: Record<SkeletonShape, string> = {
  // Inline word/value bars: table cells, labels, metadata values.
  text: "rounded",
  // Panel-sized areas: plots, overview cards, whole sections.
  block: "rounded-lg",
  // Chips, badges, tab counts.
  pill: "rounded-full",
};

// THE row-count rule: never render more skeleton rows than the real
// first page will render. Under-reserving only grows the page as data
// lands (harmless); over-reserving shrinks it and jumps content upward
// under the user's cursor. Every paginated table here serves 20 rows per
// page (Pagination hardcodes it), so:
export const TABLE_SKELETON_ROWS = 20;
// Mobile cards are roughly 5x taller than a desktop row, so 20 would be
// over two viewports of skeleton. A short skeleton that lengthens beats
// a long one that collapses.
export const TABLE_SKELETON_ROWS_MOBILE = 8;

const WIDTHS = ["w-3/4", "w-1/2", "w-5/6", "w-2/3"] as const;

// Varies bar width per cell so a skeleton column reads like ragged text
// rather than a comb. Deterministic rather than Math.random so the
// server and client renders agree — a random width would hydrate-mismatch
// and, worse, change on every re-render.
export const cellWidth = (row: number, col: number): string => {
  const widths: readonly string[] = WIDTHS;
  // Coprime strides keep the pattern from banding into obvious diagonals.
  return widths[(row * 3 + col * 5) % widths.length] ?? "w-3/4";
};

// The shape a table skeleton needs in order to mirror its real table.
// Deliberately a SUBSET of BioactivityTable's `SortableColumn`, so a real
// column spec can be handed straight to <TableSkeletonRows /> with no
// adapter — that shared spec is what stops the two from drifting.
export type SkeletonColumn = {
  key: string;
  /** Tailwind width class (e.g. "w-[40%]"), mirroring the <colgroup>. */
  width?: string;
  align?: "left" | "right";
};

// Real table cells pad the outer edges flush and the interior columns
// symmetrically (see BioactivityTableRow). Skeleton cells must use the
// exact same rule or the placeholder grid sits 16px off the real one.
export const cellPadding = (index: number, count: number): string =>
  index === 0 ? "pr-4" : index === count - 1 ? "pl-4" : "px-4";
