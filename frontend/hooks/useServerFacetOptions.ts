import { useEffect, useState } from "react";

import {
  facetOptions,
  type FacetOption,
} from "@/components/entities/shared/filters/facetOptions";

// The option list for a facet whose counts come from the API.
//
// Two fetches, deliberately. The counts endpoints (/bioactivity/endpoints,
// /categories, /evidence_types) take the OTHER active filters and return
// one row per value that survives them — a GROUP BY. That is right for the
// numbers and wrong for the list: a value with no rows under the current
// filters is not in the response at all, so a sidebar built from the
// response drops it. Picking "Experimental" on the food page made every
// predicted-only unit disappear rather than grey out.
//
// So the universe is fetched once per entity with NO filters — the same
// call the counts make on a fresh panel, and cached the same way, so it
// costs nothing extra on first paint — and the faceted counts are laid
// over it. A value in the universe but not in the counts is a zero, and
// FilterOption renders zeros disabled. The list never changes shape while
// the entity is the same, and it is alphabetical, so an option stays
// where the eye left it.
//
// Both arguments must be referentially stable, because they ARE the
// dependency list: `fetchCounts` via useCallback keyed on the entity (a
// new function is a new entity, and refetches the universe), `filters`
// via useMemo keyed on the filter values (a new object is a new
// selection, and refetches the counts). `noFilters` is the value that
// means "nothing selected" for the caller's filter shape; a module-level
// constant is the natural form.
export const useServerFacetOptions = <F extends object>(
  fetchCounts: (filters: F) => Promise<FacetOption[]>,
  filters: F,
  noFilters: F
): { options: FacetOption[]; loaded: boolean } => {
  const [universe, setUniverse] = useState<string[] | null>(null);
  const [counts, setCounts] = useState<Map<string, number> | null>(null);

  useEffect(() => {
    let cancelled = false;
    setUniverse(null);
    (async () => {
      const all = await fetchCounts(noFilters);
      if (cancelled) return;
      setUniverse(all.map((o) => o.value));
    })();
    return () => {
      cancelled = true;
    };
  }, [fetchCounts, noFilters]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const faceted = await fetchCounts(filters);
      if (cancelled) return;
      setCounts(new Map(faceted.map((o) => [o.value, o.count])));
    })();
    return () => {
      cancelled = true;
    };
  }, [fetchCounts, filters]);

  return {
    options: universe === null ? [] : facetOptions(universe, counts ?? {}),
    loaded: universe !== null && counts !== null,
  };
};
