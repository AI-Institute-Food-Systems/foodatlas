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
  countActivities,
  matchesActivities,
} from "@/components/entities/shared/filters/ActivityFilterGroup";
import {
  countSignals,
  matchesSignals,
} from "@/components/entities/shared/filters/SignalFilterGroup";
import { usePaginations } from "@/context/paginationsContext";
import type { AssayInferredAssociation } from "@/types";

export const ROWS_PER_PAGE = 20;

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

  const signalCounts = useMemo(() => countSignals(searched), [searched]);
  useEffect(() => {
    if (onSignalCountsChange && !isLoading) onSignalCountsChange(signalCounts);
  }, [onSignalCountsChange, signalCounts, isLoading]);

  const activityCounts = useMemo(() => countActivities(searched), [searched]);
  useEffect(() => {
    if (onActivityCountsChange && !isLoading) {
      onActivityCountsChange(activityCounts);
    }
  }, [onActivityCountsChange, activityCounts, isLoading]);

  const filtered = useMemo(
    () =>
      searched.filter(
        (row) =>
          matchesSignals(row.relationships, signals) &&
          matchesActivities(row.bioactivities, activities)
      ),
    [searched, signals, activities]
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
  const lastFilters = useRef(`${search}|${signals.join()}|${activities.join()}`);
  useEffect(() => {
    const next = `${search}|${signals.join()}|${activities.join()}`;
    if (lastFilters.current === next) return;
    lastFilters.current = next;
    setTablePaginations(tableId, 1, ROWS_PER_PAGE);
  }, [search, signals, activities, tableId, setTablePaginations]);

  return { rows, isLoading, filtered, visible, totalPages, tableId };
};
