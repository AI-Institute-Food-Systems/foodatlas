"use client";

// Fetch → search → signal filter → page, for the assay-inferred tables.
//
// Lives apart from AssayInferredAssociationsTable because that file is
// the table's markup and was pushing past the 300-line limit; this half
// is the only part with an ordering that matters:
//
//   rows → searched → (facet counts)    counts come from the searched set
//        → filtered → visible           so a facet never zeroes the option
//                                       the user is reaching for
//
// The endpoint returns everything in one payload, so every step is in
// memory. Pagination lives in PaginationsContext, keyed per direction —
// the chemical and disease pages each render one of these and must not
// share a page number.

import { useEffect, useMemo, useRef, useState } from "react";

import { peerName, type PeerDirection } from "@/components/entities/shared/AssayInferredRow";
import {
  activitiesOf,
  countActivities,
  matchesActivities,
} from "@/components/entities/shared/filters/ActivityFilterGroup";
import {
  countSignals,
  matchesSignals,
} from "@/components/entities/shared/filters/SignalFilterGroup";
import {
  nextSort,
  type SortDir,
} from "@/components/entities/shared/EvidenceTable";
import { usePaginations } from "@/context/paginationsContext";
import type { AssayInferredAssociation } from "@/types";

export const ROWS_PER_PAGE = 20;

// Client-side: the endpoint returns every row for the anchor, so the
// order is ours to choose. The peer's name and the assay count are the
// two scalar columns; Signal, Activities and Target are lists, and a
// list has no single order to sort by.
export type AssayInferredSortKey = "name" | "n_assays";
export type AssayInferredSort = { by: AssayInferredSortKey; dir: SortDir };
// Most assays first — the order the endpoint already ships.
export const DEFAULT_ASSAY_SORT: AssayInferredSort = { by: "n_assays", dir: "desc" };

export const sortAssayInferredRows = (
  rows: readonly AssayInferredAssociation[],
  peer: PeerDirection,
  sort: AssayInferredSort
): AssayInferredAssociation[] => {
  const sign = sort.dir === "asc" ? 1 : -1;
  const byName = (a: AssayInferredAssociation, b: AssayInferredAssociation) =>
    peerName(a, peer).localeCompare(peerName(b, peer), undefined, {
      sensitivity: "base",
    });
  return [...rows].sort((a, b) => {
    if (sort.by === "n_assays") {
      // Name breaks ties, A→Z whichever way the count runs, so equal
      // counts read as a list rather than as arbitrary.
      return sign * (a.n_assays - b.n_assays) || byName(a, b);
    }
    return sign * byName(a, b);
  });
};

interface Args {
  commonName: string;
  peer: PeerDirection;
  fetcher: () => Promise<{
    data: AssayInferredAssociation[];
    metadata: { row_count: number };
  } | null>;
  search: string;
  signals: string[];
  activities: string[];
  onSignalCountsChange?: (counts: Record<string, number>) => void;
  onActivityCountsChange?: (counts: Record<string, number>) => void;
}

export const useAssayInferredRows = ({
  commonName,
  peer,
  fetcher,
  search,
  signals,
  activities,
  onSignalCountsChange,
  onActivityCountsChange,
}: Args) => {
  const [rows, setRows] = useState<AssayInferredAssociation[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [sort, setSort] = useState<AssayInferredSort>(DEFAULT_ASSAY_SORT);
  const sortBy = (key: AssayInferredSortKey) =>
    setSort((s) => nextSort(s, key, key === "name" ? "asc" : "desc"));

  const tableId = `assay-inferred-${peer}-${commonName}`;
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const payload = await fetcher();
      if (cancelled) return;
      setRows(payload?.data ?? []);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [fetcher]);

  // Search applies to the peer's name — the only free-text column here.
  const searched = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return rows;
    return rows.filter((row) =>
      peerName(row, peer).toLowerCase().includes(term)
    );
  }, [rows, search, peer]);

  // Each facet's counts apply every OTHER filter — search plus the other
  // facet — and exclude its own, so a number is "rows you get if you pick
  // this" under the current view. They used to apply the search only, so
  // picking an activity left the Signal numbers frozen.
  const signalCounts = useMemo(
    () =>
      countSignals(
        searched.filter((row) => matchesActivities(row.bioactivities, activities))
      ),
    [searched, activities]
  );
  useEffect(() => {
    if (onSignalCountsChange && !isLoading) onSignalCountsChange(signalCounts);
  }, [onSignalCountsChange, signalCounts, isLoading]);

  // The option set is every activity in the UNFILTERED rows; only the
  // counts follow the filters. A search that excludes an activity zeroes
  // it, and a zero renders disabled rather than dropping out of the list.
  const activityUniverse = useMemo(() => activitiesOf(rows), [rows]);
  const activityCounts = useMemo(
    () =>
      countActivities(
        searched.filter((row) => matchesSignals(row.relationships, signals)),
        activityUniverse
      ),
    [searched, signals, activityUniverse]
  );
  useEffect(() => {
    if (onActivityCountsChange && !isLoading) {
      onActivityCountsChange(activityCounts);
    }
  }, [onActivityCountsChange, activityCounts, isLoading]);

  const filtered = useMemo(
    () =>
      sortAssayInferredRows(
        searched.filter(
          (row) =>
            matchesSignals(row.relationships, signals) &&
            matchesActivities(row.bioactivities, activities)
        ),
        peer,
        sort
      ),
    [searched, signals, activities, peer, sort]
  );

  const totalPages = Math.max(1, Math.ceil(filtered.length / ROWS_PER_PAGE));
  const visible = useMemo(
    () =>
      filtered.slice(
        (currentPage - 1) * ROWS_PER_PAGE,
        currentPage * ROWS_PER_PAGE
      ),
    [filtered, currentPage]
  );

  // Filtering to fewer pages while on a later one slices past the end and
  // renders the empty state over real rows, with the paginator unmounted
  // and no way back. Guarded on !isLoading because rows are empty on the
  // first render, when an unguarded clamp would discard a persisted page.
  useEffect(() => {
    if (!isLoading && currentPage > totalPages) {
      setTablePaginations(tableId, 1, ROWS_PER_PAGE);
    }
  }, [isLoading, currentPage, totalPages, tableId, setTablePaginations]);

  // A new query should land on its best matches, not on page 4 of them.
  // Ref-compared rather than a bare effect so mounting doesn't reset a
  // page the user navigated to.
  const filterKey = `${search}|${signals.join()}|${activities.join()}|${sort.by}|${sort.dir}`;
  const lastFilters = useRef(filterKey);
  useEffect(() => {
    if (lastFilters.current === filterKey) return;
    lastFilters.current = filterKey;
    setTablePaginations(tableId, 1, ROWS_PER_PAGE);
  }, [filterKey, tableId, setTablePaginations]);

  return {
    rows,
    isLoading,
    filtered,
    visible,
    totalPages,
    tableId,
    sort,
    setSort,
    sortBy,
  };
};
