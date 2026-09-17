// How every facet decides WHICH options to list and in WHAT order.
//
// FilterControls owns how one option looks and when it is disabled; this
// owns the list. Two rules, and both exist because the opposite shipped:
//
// 1. The option set is fixed for the entity. It comes from the unfiltered
//    data — every value that exists for this pivot — never from a faceted
//    count response, because a GROUP BY only returns what is present. Wire
//    the list to the counts and picking "Experimental" makes every
//    predicted-only unit vanish from the sidebar instead of greying out,
//    and the food page's Assay Source did exactly that.
//
// 2. The order is alphabetical, case-insensitive, with a reset ("All") or a
//    catch-all ("unclassified") pinned to an end. Busiest-first sounds
//    helpful and was the default everywhere, but the counts change with
//    every other filter, so the options reshuffled on every click — the
//    assays modal's Evidence list moved under the cursor whenever Outcome
//    changed. An option has to stay where the eye left it.

export interface FacetOption {
  value: string;
  count: number;
}

const byLabel = (a: string, b: string): number =>
  a.localeCompare(b, undefined, { sensitivity: "base" });

/**
 * Alphabetical by `label`, case-insensitive. `pinFirst` / `pinLast` name
 * labels held at the ends in the order given — reset options at the top,
 * catch-alls at the bottom — so they neither float into the middle of the
 * alphabet nor swap places.
 */
export function sortFacetOptions<T>(
  options: readonly T[],
  label: (option: T) => string,
  { pinFirst = [], pinLast = [] }: { pinFirst?: string[]; pinLast?: string[] } = {}
): T[] {
  const rank = (o: T): number => {
    const l = label(o);
    const first = pinFirst.indexOf(l);
    if (first !== -1) return first - pinFirst.length; // negative: before all
    const last = pinLast.indexOf(l);
    if (last !== -1) return last + 1; // positive: after all
    return 0;
  };
  return [...options].sort(
    (a, b) => rank(a) - rank(b) || byLabel(label(a), label(b))
  );
}

/**
 * The universe with its faceted counts laid over it. Every value in
 * `universe` comes back exactly once; a value the counts do not mention
 * is zero, which FilterOption then renders disabled — not absent.
 *
 * Values that appear in `counts` but not in `universe` are dropped: the
 * universe is the contract for what the facet offers, and a count for
 * something outside it is a stale response from a previous entity.
 */
export function overlayFacetCounts(
  universe: readonly string[],
  counts: ReadonlyMap<string, number> | Readonly<Record<string, number>>
): FacetOption[] {
  const get = (v: string): number =>
    counts instanceof Map
      ? (counts.get(v) ?? 0)
      : ((counts as Readonly<Record<string, number>>)[v] ?? 0);
  return Array.from(new Set(universe)).map((value) => ({
    value,
    count: get(value),
  }));
}

/** Universe ∪ counts, sorted — the whole list rule in one call. */
export function facetOptions(
  universe: readonly string[],
  counts: ReadonlyMap<string, number> | Readonly<Record<string, number>>,
  pins?: { pinFirst?: string[]; pinLast?: string[] }
): FacetOption[] {
  return sortFacetOptions(overlayFacetCounts(universe, counts), (o) => o.value, pins);
}

/**
 * Several count lists summed by value. For facets that span two fetches
 * — the food page's sidebar drives a direct table and an inferred table,
 * each with its own counts endpoint — so a unit present in both is one
 * option with one number. Blank values are dropped, as they are nowhere
 * to click.
 */
export function sumFacetCounts(
  ...lists: readonly (readonly FacetOption[])[]
): FacetOption[] {
  const totals = new Map<string, number>();
  for (const list of lists) {
    for (const { value, count } of list) {
      const v = (value ?? "").trim();
      if (!v) continue;
      totals.set(v, (totals.get(v) ?? 0) + (count ?? 0));
    }
  }
  return Array.from(totals, ([value, count]) => ({ value, count }));
}

/** The distinct, non-blank values a list of records carries. */
export function facetUniverse<T>(
  records: readonly T[],
  value: (record: T) => string | null | undefined
): string[] {
  const seen = new Set<string>();
  for (const r of records) {
    const v = (value(r) ?? "").trim();
    if (v) seen.add(v);
  }
  return Array.from(seen);
}
