// Shared table chrome for the four bioactivity views — mirrors the
// /food/composition table's toolbar + sortable headers + pagination so
// the user gets identical interactions across composition and bioactivity
// pages. Each consumer passes a fetcher, column spec, link builder, and a
// stable tableId; this component owns search/sort/page state, the
// /bioactivity/measurements modal, and the loading skeleton.

"use client";

import { ReactNode, useCallback, useEffect, useMemo, useState } from "react";

import { usePublishTabCount } from "@/context/tabCountsContext";
import {
  MdCheck,
  MdClose,
  MdDescription,
  MdTune,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import Chip from "@/components/basic/Chip";
import AssayIcon from "@/components/icons/AssayIcon";
import Link from "@/components/basic/Link";
import {
  TableSkeletonCards,
  TableSkeletonRows,
} from "@/components/basic/TableSkeleton";
import Pagination from "@/components/basic/Pagination";
import SortListbox from "@/components/basic/SortListbox";
import {
  FACET_MAX_HEIGHT,
  FilterGroup,
  FilterOption,
  FilterOptionList,
  FilterSearchInput,
} from "@/components/entities/shared/filters/FilterControls";
import FilterPanel from "@/components/entities/shared/filters/FilterPanel";
import { MobileSort, Th } from "@/components/entities/shared/EvidenceTable";
import { sumFacetCounts } from "@/components/entities/shared/filters/facetOptions";
import { useReportRows } from "@/context/reportModeContext";
import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import { formatTopMeasurement, topMeasurementOf } from "@/components/entities/bioactivity/format";
import { usePaginations } from "@/context/paginationsContext";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";
import { useServerFacetOptions } from "@/hooks/useServerFacetOptions";
import { encodeSpace } from "@/utils/utils";
import TableEmptyState from "@/components/entities/shared/TableEmptyState";
import {
  getBioactivityCategoryOptions,
  getBioactivityEndpointOptions,
  getBioactivityEvidenceTypeCounts,
  getBioactivitySourceKindCounts,
  NO_SIDEBAR_FILTERS,
  type BioactivitySidebarFilters,
  type BioactivitySourceKindCounts,
  type BioactivityDirection,
  type BioactivityListParams,
} from "@/utils/fetching";
import type {
  BioactivityChemicalRow,
  BioactivityFoodRow,
} from "@/types";

// A row in either MV — chemical-side rows carry active/inactive_count + may
// have a richer measurements array; food-side rows only have the count.
export type BioactivityRow = BioactivityChemicalRow | BioactivityFoodRow;

export type SortDir = "asc" | "desc";

export type ColumnContext = {
  openModal: () => void;
};

export type SortableColumn = {
  key: string;
  label: string;
  align?: "left" | "right";
  // Width as a Tailwind class like "w-[20%]"; passed to <col />.
  width: string;
  // Whether this column maps to a server-side sort_by key.
  sortable?: boolean;
  // Human labels for asc/desc in the mobile sort dropdown. Arrows
  // ("Chemical ↓") are ambiguous for text vs numeric columns, so each
  // sortable column should supply a phrase like "Chemical A–Z" /
  // "Highest concentration". Falls back to `${label} ↑ / ↓` if omitted.
  sortLabels?: { asc: string; desc: string };
  // Renders the cell content for a row.
  render: (row: BioactivityRow, ctx: ColumnContext) => ReactNode;
};

// Sort-by value that the API understands as "max value across matching
// measurements" — only meaningful when an endpoint+unit filter is set.
const TOP_VALUE_SORT_KEY = "top_measurement_value";

interface Props {
  // Optional overrides for shared-chrome layouts (e.g. the Food page's
  // Bioactivities tab hosts one search + filter sidebar for both the
  // direct table and the inferred table). When any of these is set,
  // the internal state is replaced; when `hideChrome` is true, the
  // table renders as a bare table + pagination + modal with no
  // sidebar / mobile trigger / drawer.
  externalSearch?: string;
  externalSourceKind?: string;
  externalUnit?: string;
  externalEvidenceType?: string;
  hideChrome?: boolean;
  // Stable identifier for pagination context — e.g. "food-bioact-foodId".
  tableId: string;
  // Pivot+direction combo used to fetch the endpoint-filter chip options.
  // Optional — when absent, the chip row is hidden (table behaves as
  // before). Must match the route the fetcher hits.
  direction?: BioactivityDirection;
  // The pivot entity's common_name, used by getBioactivityEndpointOptions
  // to look up the bioactivity_id (or chem/food id) in the right MV.
  pivotName?: string;
  // Server-side fetcher; the table passes page/search/sort/sortDir.
  fetcher: (
    params: BioactivityListParams
  ) => Promise<
    | {
        data: BioactivityRow[];
        metadata: {
          row_count: number;
          total_rows: number;
          total_pages: number;
          current_page: number;
          rows_per_page: number;
        };
      }
    | null
  >;
  columns: SortableColumn[];
  // Initial sort_by / sort_dir.
  defaultSortBy?: string;
  defaultSortDir?: SortDir;
  searchPlaceholder?: string;
  // Empty-state message when no rows + no filter applied.
  emptyMessage: ReactNode;
  // Empty-state message when filters are active but nothing matched.
  // Distinguishes "no data at all" from "your filters filtered it to 0"
  // so users don't assume the entity has nothing to show. Optional; a
  // sensible generic default is used when omitted.
  emptyMessageFiltered?: ReactNode;
  // Reset callback used by the filter-driven empty state's inline
  // "clear filters" button. Standalone usage falls through to the
  // internal resetAllFilters; a parent driving external filters should
  // pass its own so the sidebar clears too.
  onResetFilters?: () => void;
  // Modal-related — head/tail labels and the relationship type for the
  // measurements query. headIsRow flips which side of the (row, anchor)
  // pair is the head of the relationship.
  modalConfig: {
    anchorLabel: string;
    headIsRow: boolean;
    relationship: "r5" | "r6";
    // Anchor entity's foodatlas_id, when known. Combined with the
    // selected row's id, the modal lazy-fetches the full measurement set
    // from /bioactivity/measurements (which carries Hill-fit fields for
    // the dose-response sparkline). When absent, modal falls back to the
    // row's MV-nested sample only.
    anchorId?: string | null;
  };
  // When set, the table publishes its current filtered totalRows to
  // the tab-count context under this key — the tab badge picks it up
  // and overrides the server-prefetched static count.
  tabIdForCount?: string;
  // Alternative to tabIdForCount for wrappers that aggregate multiple
  // tables into one tab count (e.g. FoodBioactivitiesTab summing
  // direct + inferred). Fires whenever totalRows changes.
  onTotalRowsChange?: (total: number) => void;
}

// The direction the table was fetched with tells us which entity the
// page belongs to — used to label the per-row report context.
const pageEntityTypeFromDirection = (
  direction?: BioactivityDirection,
): "food" | "chemical" | "bioactivity" => {
  if (direction === "chemical-bioactivities") return "chemical";
  if (direction === "food-bioactivities") return "food";
  return "bioactivity";
};

const BioactivityTable = ({
  tableId,
  direction,
  pivotName,
  fetcher,
  columns,
  defaultSortBy = "measurement_count",
  defaultSortDir = "desc",
  searchPlaceholder = "Search…",
  emptyMessage,
  emptyMessageFiltered,
  onResetFilters,
  modalConfig,
  externalSearch,
  externalSourceKind,
  externalUnit,
  externalEvidenceType,
  hideChrome = false,
  tabIdForCount,
  onTotalRowsChange,
}: Props) => {
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);
  const reporter = useReportRows();
  const pageEntityType = pageEntityTypeFromDirection(direction);

  const [searchTerm, setSearchTerm] = useState("");
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);
  const [sort, setSort] = useState<{ by: string; dir: SortDir }>({
    by: defaultSortBy,
    dir: defaultSortDir,
  });
  const [selectedUnits, setSelectedUnits] = useState<string[]>([]);
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  // Multi-select Evidence filter. Values are the NPASS-style buckets
  // returned by /bioactivity/evidence_types (typically molecular-level /
  // in vitro / in vivo / adme-tox). Backend accepts them '+'-joined.
  const [selectedEvidenceTypes, setSelectedEvidenceTypes] = useState<string[]>(
    []
  );
  // Single-select Assay Source filter. Value is one of:
  //   "" — "both" (no filter, default)
  //   "experimental"
  //   "predicted"
  const [selectedSourceKind, setSelectedSourceKind] = useState<string>("");
  const unitFilterParam = selectedUnits.join("+");
  const categoryFilterParam = selectedCategories.join("+");
  const evidenceTypeFilterParam = selectedEvidenceTypes.join("+");
  // External overrides win when present so a parent (e.g. the food
  // page's Bioactivities tab) can drive search + source kind + unit +
  // evidence type for its tables from one shared sidebar.
  // Debounced after the external/internal choice so both paths get it:
  // the input stays instant, only the fetch waits. Previously every
  // keystroke was its own request that blanked the table.
  const effectiveSearchTerm = useDebouncedValue(
    externalSearch !== undefined ? externalSearch : searchTerm
  );
  const effectiveSourceKindParam =
    externalSourceKind !== undefined ? externalSourceKind : selectedSourceKind;
  const effectiveUnitParam =
    externalUnit !== undefined ? externalUnit : unitFilterParam;
  const effectiveEvidenceTypeParam =
    externalEvidenceType !== undefined
      ? externalEvidenceType
      : evidenceTypeFilterParam;

  // The three facet lists. Each fetcher is keyed on the pivot so the hook
  // fetches the full option set once per entity, then lays the faceted
  // counts over it — every OTHER dimension applied, the facet's own
  // excluded, since the list is what the user picks that dimension from.
  // See useServerFacetOptions for why the list and the counts are two
  // fetches.
  const fetchUnitCounts = useCallback(
    async (f: BioactivitySidebarFilters) => {
      if (!direction || !pivotName) return [];
      // The endpoint is per (endpoint, unit); the facet is per unit.
      const opts = await getBioactivityEndpointOptions(pivotName, direction, f);
      return sumFacetCounts(opts.map((o) => ({ value: o.unit, count: o.count })));
    },
    [direction, pivotName]
  );
  const unitFacetFilters = useMemo<BioactivitySidebarFilters>(
    () => ({
      filterEvidenceType: effectiveEvidenceTypeParam,
      filterSourceKind: effectiveSourceKindParam,
      search: effectiveSearchTerm,
    }),
    [effectiveEvidenceTypeParam, effectiveSourceKindParam, effectiveSearchTerm]
  );
  const { options: unitOptions, loaded: unitsLoaded } = useServerFacetOptions(
    fetchUnitCounts,
    unitFacetFilters,
    NO_SIDEBAR_FILTERS
  );

  const toggleUnit = (unit: string) => {
    setTablePaginations(tableId, 1, 20);
    setSelectedUnits((prev) =>
      prev.includes(unit) ? prev.filter((u) => u !== unit) : [...prev, unit]
    );
  };
  const clearUnits = () => {
    setTablePaginations(tableId, 1, 20);
    setSelectedUnits([]);
  };

  const [rows, setRows] = useState<BioactivityRow[]>([]);
  const [totalPages, setTotalPages] = useState(0);
  const [totalRows, setTotalRows] = useState(0);
  const [isLoading, setIsLoading] = useState(true);
  // A fetch with rows already on screen is a refetch (page, sort, filter,
  // search) — keep them and dim, rather than blanking the table. Only a
  // fetch with nothing to keep gets the skeleton.
  const showSkeleton = isLoading && rows.length === 0;
  const isRefetching = isLoading && rows.length > 0;
  // Publish filtered total to the tab-count context OR bubble to a
  // wrapper via callback (never both — pick one at the call site).
  usePublishTabCount(
    tabIdForCount ?? "",
    tabIdForCount && !isLoading ? totalRows : null,
  );
  useEffect(() => {
    if (onTotalRowsChange && !isLoading) onTotalRowsChange(totalRows);
  }, [onTotalRowsChange, totalRows, isLoading]);

  // Chemical Category options come from a dedicated endpoint that
  // aggregates classifications across ALL matching chemicals for the
  // pivot bioactivity — so counts reflect the full result set, not just
  // the current page. Only meaningful for the bioactivity-chemicals
  // direction; other directions get [].
  const fetchCategoryCounts = useCallback(
    async (f: BioactivitySidebarFilters) => {
      if (direction !== "bioactivity-chemicals" || !pivotName) return [];
      const opts = await getBioactivityCategoryOptions(pivotName, f);
      return opts.map((o) => ({ value: o.category, count: o.count }));
    },
    [direction, pivotName]
  );
  const categoryFacetFilters = useMemo<BioactivitySidebarFilters>(
    () => ({
      filterUnit: effectiveUnitParam,
      filterSourceKind: effectiveSourceKindParam,
      filterEvidenceType: effectiveEvidenceTypeParam,
      search: effectiveSearchTerm,
    }),
    [
      effectiveUnitParam,
      effectiveSourceKindParam,
      effectiveEvidenceTypeParam,
      effectiveSearchTerm,
    ]
  );
  const { options: categoryOptions, loaded: categoriesLoaded } =
    useServerFacetOptions(
      fetchCategoryCounts,
      categoryFacetFilters,
      NO_SIDEBAR_FILTERS
    );

  // Sidebar Assay Source counts. Aggregate across ALL matching rows
  // (not just the current page) and apply every other active filter so
  // the counts stay in sync with the visible table.
  const [sourceKindCounts, setSourceKindCounts] =
    useState<BioactivitySourceKindCounts | null>(null);
  useEffect(() => {
    if (!direction || !pivotName) {
      setSourceKindCounts(null);
      return;
    }
    let cancelled = false;
    (async () => {
      const counts = await getBioactivitySourceKindCounts(
        pivotName,
        direction,
        {
          filterUnit: effectiveUnitParam,
          filterCategory: categoryFilterParam,
          filterEvidenceType: effectiveEvidenceTypeParam,
          search: effectiveSearchTerm,
        },
      );
      if (!cancelled) setSourceKindCounts(counts);
    })();
    return () => {
      cancelled = true;
    };
  }, [
    direction,
    pivotName,
    effectiveUnitParam,
    categoryFilterParam,
    effectiveEvidenceTypeParam,
    effectiveSearchTerm,
  ]);

  // Sidebar Evidence counts. Same aggregate-across-all-rows semantics
  // as source-kind + category counts.
  const fetchEvidenceTypeCounts = useCallback(
    async (f: BioactivitySidebarFilters) => {
      if (!direction || !pivotName) return [];
      const opts = await getBioactivityEvidenceTypeCounts(pivotName, direction, f);
      return opts.map((o) => ({ value: o.evidence_type, count: o.count }));
    },
    [direction, pivotName]
  );
  const evidenceFacetFilters = useMemo<BioactivitySidebarFilters>(
    () => ({
      filterUnit: effectiveUnitParam,
      filterSourceKind: effectiveSourceKindParam,
      search: effectiveSearchTerm,
    }),
    [effectiveUnitParam, effectiveSourceKindParam, effectiveSearchTerm]
  );
  const { options: evidenceTypeOptions, loaded: evidenceTypesLoaded } =
    useServerFacetOptions(
      fetchEvidenceTypeCounts,
      evidenceFacetFilters,
      NO_SIDEBAR_FILTERS
    );

  const toggleCategory = (category: string) => {
    setTablePaginations(tableId, 1, 20);
    setSelectedCategories((prev) =>
      prev.includes(category)
        ? prev.filter((c) => c !== category)
        : [...prev, category]
    );
  };
  const clearCategories = () => {
    setTablePaginations(tableId, 1, 20);
    setSelectedCategories([]);
  };

  const toggleEvidenceType = (etype: string) => {
    setTablePaginations(tableId, 1, 20);
    setSelectedEvidenceTypes((prev) =>
      prev.includes(etype) ? prev.filter((e) => e !== etype) : [...prev, etype]
    );
  };
  const clearEvidenceTypes = () => {
    setTablePaginations(tableId, 1, 20);
    setSelectedEvidenceTypes([]);
  };

  // Single-select Assay Source. "both" (key="") is the default = no
  // filter; the other two narrow rows to those with ≥1 measurement of
  // the chosen kind (backend classifies against the capped
  // measurements sample by evidence_source prefix).
  const SOURCE_KINDS: { key: string; label: string }[] = [
    { key: "", label: "All" },
    { key: "experimental", label: "Experimental" },
    { key: "predicted", label: "Predicted" },
  ];
  const chooseSourceKind = (kind: string) => {
    setTablePaginations(tableId, 1, 20);
    setSelectedSourceKind(kind);
  };

  const [selected, setSelected] = useState<BioactivityRow | null>(null);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      // Fetcher returns null on staging blips; treat as empty so we render
      // the empty-state instead of a noisy "An error occurred" banner.
      const payload = await fetcher({
        page: currentPage,
        search: effectiveSearchTerm,
        sortBy: sort.by,
        sortDir: sort.dir,
        filterUnit: effectiveUnitParam || undefined,
        filterCategory: categoryFilterParam || undefined,
        filterSourceKind: effectiveSourceKindParam || undefined,
        filterEvidenceType: effectiveEvidenceTypeParam || undefined,
      });
      if (cancelled) return;
      setRows(payload?.data ?? []);
      setTotalPages(payload?.metadata?.total_pages ?? 0);
      setTotalRows(payload?.metadata?.total_rows ?? 0);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [
    fetcher,
    currentPage,
    effectiveSearchTerm,
    sort,
    effectiveUnitParam,
    categoryFilterParam,
    effectiveSourceKindParam,
    effectiveEvidenceTypeParam,
  ]);

  // Snap back to page 1 when the server reports fewer pages than the page
  // we're on. Search and filter chrome can live on a parent (see
  // `hideChrome`, e.g. FoodBioactivitiesTab), and that parent doesn't know
  // this table's tableId — so the in-component resets below are unreachable
  // and a narrowed result set would otherwise strand the user on an
  // out-of-range page with the paginator unmounted.
  //
  // Guards: `!isLoading` because total_pages is 0 mid-flight, and
  // `totalPages > 0` so a genuinely empty result set keeps its own empty
  // state instead of bouncing the page.
  useEffect(() => {
    if (!isLoading && totalPages > 0 && currentPage > totalPages) {
      setTablePaginations(tableId, 1, 20);
    }
  }, [isLoading, totalPages, currentPage, tableId, setTablePaginations]);

  const handleSearchChange = (value: string) => {
    setSearchTerm(value.toLowerCase());
    setTablePaginations(tableId, 1, 20);
  };
  const handleSearchClear = () => {
    setSearchTerm("");
    setTablePaginations(tableId, 1, 20);
  };
  const handleSortClick = (key: string) => {
    setTablePaginations(tableId, 1, 20);
    setSort((prev) =>
      prev.by === key
        ? { by: key, dir: prev.dir === "asc" ? "desc" : "asc" }
        : { by: key, dir: "desc" }
    );
  };
  const colSpan = columns.length;
  const showingPaginator = totalPages > 1 || isLoading;
  const showEmpty = !isLoading && rows.length === 0;

  // Effective-filter dirtiness — accounts for both internal state and
  // parent-driven overrides. Used to swap the empty-state copy so
  // "nothing here" reads differently from "your filters gave 0."
  const hasActiveFilters =
    (effectiveSearchTerm ?? "") !== "" ||
    (effectiveUnitParam ?? "") !== "" ||
    (categoryFilterParam ?? "") !== "" ||
    (effectiveSourceKindParam ?? "") !== "" ||
    (effectiveEvidenceTypeParam ?? "") !== "";

  // Search field used in three places: sidebar, drawer's sidebar-copy,
  // and standalone left of the mobile Filters button.
  const searchInput = (
    <FilterSearchInput
      value={searchTerm}
      onChange={handleSearchChange}
      onClear={handleSearchClear}
      placeholder="Search…"
      ariaLabel={searchPlaceholder}
    />
  );

  // True when any filter differs from a fresh page load. Drives the
  // Reset link visibility so it's only there when there's something
  // to reset.
  const isFiltersDirty =
    searchTerm !== "" ||
    selectedUnits.length > 0 ||
    selectedCategories.length > 0 ||
    selectedSourceKind !== "" ||
    selectedEvidenceTypes.length > 0 ||
    sort.by !== defaultSortBy ||
    sort.dir !== defaultSortDir;

  const resetAllFilters = () => {
    setTablePaginations(tableId, 1, 20);
    setSearchTerm("");
    setSelectedUnits([]);
    setSelectedCategories([]);
    setSelectedSourceKind("");
    setSelectedEvidenceTypes([]);
    setSort({ by: defaultSortBy, dir: defaultSortDir });
  };

  // Empty-state body — same on desktop table row and mobile card list.
  // When filters are active we surface the filter-aware message + an
  // inline "clear filters" button that either delegates to a parent
  // (externally-driven case) or resets locally. When there are truly
  // no rows, we show the caller-supplied `emptyMessage` untouched.
  const resetForEmptyState = onResetFilters ?? resetAllFilters;
  const emptyStateBody = hasActiveFilters ? (
    <TableEmptyState onClearFilters={resetForEmptyState}>
      {emptyMessageFiltered ?? "No results match the current filters."}
    </TableEmptyState>
  ) : (
    <TableEmptyState>{emptyMessage}</TableEmptyState>
  );

  // Non-search filters. Drawer on small viewports uses this alone (search
  // stays visible outside the drawer). A group is omitted only when the
  // entity has NO values for that dimension at all — the full option set
  // is empty — never because the current filters zeroed it; those options
  // stay, disabled, so the list keeps its shape.
  const filtersOnlyPanel = (
    <div className="flex flex-col gap-5">
      {unitOptions.length > 0 && (
        <FilterGroup
          label="Unit"
          onClear={selectedUnits.length > 0 ? clearUnits : undefined}
        >
          {/* Alphabetical, so the tail is a scroll rather than a "5 more…"
            * collapse: with a fixed order a top-N would hide whatever sorts
            * late (uM, the commonest unit, under "5 more"). */}
          <FilterOptionList maxHeightClass={FACET_MAX_HEIGHT}>
            {unitOptions.map(({ value, count }) => (
              <FilterOption
                key={value}
                label={value}
                count={count}
                countsLoaded={unitsLoaded}
                selected={selectedUnits.includes(value)}
                onClick={() => toggleUnit(value)}
                capitalize={false}
              />
            ))}
          </FilterOptionList>
        </FilterGroup>
      )}

      {categoryOptions.length > 0 && (
        <FilterGroup
          label="Category"
          onClear={selectedCategories.length > 0 ? clearCategories : undefined}
        >
          <FilterOptionList maxHeightClass={FACET_MAX_HEIGHT}>
            {categoryOptions.map(({ value, count }) => (
              <FilterOption
                key={value}
                label={value}
                count={count}
                countsLoaded={categoriesLoaded}
                selected={selectedCategories.includes(value)}
                onClick={() => toggleCategory(value)}
              />
            ))}
          </FilterOptionList>
        </FilterGroup>
      )}

      {evidenceTypeOptions.length > 0 && (
        <FilterGroup
          label="Evidence"
          onClear={
            selectedEvidenceTypes.length > 0 ? clearEvidenceTypes : undefined
          }
        >
          <FilterOptionList>
            {evidenceTypeOptions.map(({ value, count }) => (
              <FilterOption
                key={value}
                label={value}
                count={count}
                countsLoaded={evidenceTypesLoaded}
                selected={selectedEvidenceTypes.includes(value)}
                onClick={() => toggleEvidenceType(value)}
              />
            ))}
          </FilterOptionList>
        </FilterGroup>
      )}

      <FilterGroup label="Assay Source">
        {/* Single-select, so radio affordance — the only thing `mode`
          * changes. Behaviour is identical to the check rows above. */}
        <FilterOptionList mode="radio" ariaLabel="Assay Source">
          {SOURCE_KINDS.map(({ key, label }) => {
            const c =
              sourceKindCounts === null
                ? undefined
                : key === ""
                ? sourceKindCounts.both
                : key === "experimental"
                ? sourceKindCounts.experimental
                : sourceKindCounts.predicted;
            return (
              <FilterOption
                key={label}
                mode="radio"
                label={label}
                count={c}
                countsLoaded={sourceKindCounts !== null}
                selected={selectedSourceKind === key}
                resetOption={key === ""}
                onClick={() => chooseSourceKind(key)}
              />
            );
          })}
        </FilterOptionList>
      </FilterGroup>
    </div>
  );

  return (
    <FilterPanel
      search={searchInput}
      filters={filtersOnlyPanel}
      isDirty={isFiltersDirty}
      onReset={resetAllFilters}
      open={mobileFiltersOpen}
      onOpenChange={setMobileFiltersOpen}
      hideChrome={hideChrome}
    >

      <div className="flex flex-col gap-7">
      <div>
      {/* Row-count caption dropped — the tab badge now carries the
       * filtered total via usePublishTabCount / onTotalRowsChange.
       * Mobile sort listbox stays here (no clickable column headers on
       * card view). */}
      {!isLoading && totalRows > 0 && columns.some((c) => c.sortable) && (
        <MobileSort
          sort={sort}
          columns={columns
            .filter((c) => c.sortable)
            .map((c) => ({
              key: c.key,
              labels: c.sortLabels ?? {
                desc: `${c.label} ↓`,
                asc: `${c.label} ↑`,
              },
            }))}
          onChange={(next) => {
            setSort(next);
            setTablePaginations(tableId, 1, 20);
          }}
        />
      )}
      <div
        aria-busy={isRefetching}
        className={twMerge(
          "hidden md:block overflow-x-auto",
          // Current rows stay readable but visibly stale, and inert so a
          // click can't act on data that's about to be replaced.
          isRefetching && "opacity-60 pointer-events-none transition-opacity"
        )}
      >
        <table className="w-full table-fixed">
          <colgroup>
            {columns.map((c) => (
              <col key={c.key} className={c.width} />
            ))}
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              {columns.map((c, idx) => {
                // The Assays column gets a "(experimental)" /
                // "(predicted)" suffix when a source kind filter is
                // active, so readers know the count reflects only
                // that subset. Applies whether the column is sortable
                // (bioactivity's chemicals table) or not (all others).
                const label =
                  c.label === "Assays" && effectiveSourceKindParam
                    ? `Assays (${effectiveSourceKindParam})`
                    : c.label;
                return (
                  <Th
                    key={c.key}
                    align={c.align === "right" ? "right" : undefined}
                    className={twMerge(
                      "break-all md:break-normal",
                      idx === 0
                        ? "pr-4 pl-0"
                        : idx === columns.length - 1
                        ? "pl-4 pr-0"
                        : "px-4"
                    )}
                    sort={
                      c.sortable
                        ? {
                            active: sort.by === c.key,
                            dir: sort.dir,
                            onClick: () => handleSortClick(c.key),
                          }
                        : undefined
                    }
                  >
                    {label}
                  </Th>
                );
              })}
            </tr>
          </thead>
          <tbody className="text-sm font-light">
            {showSkeleton ? (
              <TableSkeletonRows columns={columns} />
            ) : showEmpty ? (
              <tr>
                <td colSpan={colSpan}>{emptyStateBody}</td>
              </tr>
            ) : (
              rows.map((row) => (
                <BioactivityTableRow
                  key={row.id}
                  row={row}
                  columns={columns}
                  onOpen={() => setSelected(row)}
                  rowReportProps={reporter.getRowProps(
                    {
                      kind: "bioactivity-row",
                      entityType: pageEntityType,
                      entitySlug: pivotName,
                      bioactivityId: String(row.id),
                      bioactivityName: row.name,
                      activeCount: (row as BioactivityChemicalRow).active_count,
                      inactiveCount: (row as BioactivityChemicalRow)
                        .inactive_count,
                    },
                    { disabled: row.measurement_count === 0 },
                  )}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Card list — mobile. Walks the same `columns[]` spec: first
       * column renders as the primary line (typically NameLinkCell),
       * remaining columns become label:value rows with justify-between.
       * That way every consumer (bioactivity chemicals / foods /
       * measurements) gets a mobile view without a per-caller
       * override. */}
      {showSkeleton ? (
        <TableSkeletonCards columns={columns} />
      ) : (
      <div
        aria-busy={isRefetching}
        className={twMerge(
          "md:hidden w-full flex flex-col divide-y divide-light-800",
          isRefetching && "opacity-60 pointer-events-none transition-opacity"
        )}
      >
        {showEmpty ? (
          emptyStateBody
        ) : (
          rows.map((row) => {
            const ctx: ColumnContext = { openModal: () => setSelected(row) };
            const [primary, ...rest] = columns;
            const rowReportProps = reporter.getRowProps(
              {
                kind: "bioactivity-row",
                entityType: pageEntityType,
                entitySlug: pivotName,
                bioactivityId: String(row.id),
                bioactivityName: row.name,
                activeCount: (row as BioactivityChemicalRow).active_count,
                inactiveCount: (row as BioactivityChemicalRow).inactive_count,
              },
              { disabled: row.measurement_count === 0 },
            );
            return (
              <div
                key={row.id}
                {...rowReportProps}
                className={twMerge(
                  "w-full py-3 flex flex-col gap-2 text-sm",
                  rowReportProps.className,
                )}
              >
                <div className="w-full flex items-center gap-2 flex-wrap">
                  {primary.render(row, ctx)}
                </div>
                {rest.map((c) => {
                  const label =
                    c.label === "Assays" && effectiveSourceKindParam
                      ? `Assays (${effectiveSourceKindParam})`
                      : c.label;
                  return (
                    <div
                      key={c.key}
                      className="w-full flex items-center justify-between gap-2"
                    >
                      <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                        {label}
                      </span>
                      <div className="text-right">
                        {c.render(row, ctx)}
                      </div>
                    </div>
                  );
                })}
              </div>
            );
          })
        )}
      </div>
      )}
      </div>

      {showingPaginator && (
        <div className="mt-2 max-w-xl w-full mx-auto">
          <Pagination
            tableId={tableId}
            numberOfPages={totalPages}
            isLoading={showSkeleton}
            isBusy={isRefetching}
          />
        </div>
      )}
      </div>

      <BioactivityMeasurementsModal
        isOpen={selected !== null}
        onClose={() => setSelected(null)}
        headLabel={
          modalConfig.headIsRow ? selected?.name ?? "" : modalConfig.anchorLabel
        }
        tailLabel={
          modalConfig.headIsRow ? modalConfig.anchorLabel : selected?.name ?? ""
        }
        initialMeasurements={selected?.measurements ?? []}
        expectedCount={selected?.measurement_count}
        anchorId={modalConfig.anchorId}
        selectedId={selected?.id}
        relationship={modalConfig.relationship}
        headIsRow={modalConfig.headIsRow}
      />
    </FilterPanel>
  );
};

const BioactivityTableRow = ({
  row,
  columns,
  onOpen,
  rowReportProps,
}: {
  row: BioactivityRow;
  columns: SortableColumn[];
  onOpen: () => void;
  rowReportProps?: ReturnType<
    ReturnType<typeof useReportRows>["getRowProps"]
  >;
}) => {
  const ctx: ColumnContext = { openModal: onOpen };
  return (
    <tr {...rowReportProps}>
      {columns.map((c, idx) => (
        <td
          key={c.key}
          className={`py-1.5 ${
            idx === 0
              ? "pr-4"
              : idx === columns.length - 1
              ? "pl-4"
              : "px-4"
          }`}
        >
          <div
            className={`flex min-h-9 items-center ${
              c.align === "right" ? "justify-end" : ""
            }`}
          >
            {c.render(row, ctx)}
          </div>
        </td>
      ))}
    </tr>
  );
};

// Sort key recognised by the API as "sort by max value across
// measurements that match filter_endpoint + filter_unit". Exported so
// section column specs can use it as the Top Measurement sortable key.
export const TOP_MEASUREMENT_SORT_KEY = TOP_VALUE_SORT_KEY;

// Shared cell renderers used by sections (re-exported so each section's
// column spec stays terse).
export const NameLinkCell = ({
  row,
  hrefPrefix,
}: {
  row: BioactivityRow;
  hrefPrefix: "/bioactivity/" | "/chemical/" | "/food/";
}) => (
  <div className="capitalize">
    <Link
      href={`${hrefPrefix}${encodeURIComponent(encodeSpace(row.name))}`}
      isExternal={false}
    >
      {row.name}
    </Link>
  </div>
);

export const NumberCell = ({ value }: { value: number }) => (
  <span className="tabular-nums">{value.toLocaleString()}</span>
);

export const TopMeasurementCell = ({ row }: { row: BioactivityRow }) => (
  <span className="font-mono text-xs text-light-200">
    {formatTopMeasurement(topMeasurementOf(row))}
  </span>
);


export const ViewAssaysCell = ({
  row,
  ctx,
}: {
  row: BioactivityRow;
  ctx: ColumnContext;
}) => (
  <Chip
    icon={<AssayIcon />}
    label={`${row.measurement_count.toLocaleString()} assay${
      row.measurement_count === 1 ? "" : "s"
    }`}
    tone="outline"
    size="md"
    onClick={ctx.openModal}
    disabled={row.measurement_count === 0}
  />
);

// Chemical classification (["flavonoid", "polyphenol"] → "flavonoid,
// polyphenol"). Trims to the first entry + "N more" once we have more
// than two so the column stays narrow. Silent em-dash for empty/null
// so unclassified rows read the same as unmeasured elsewhere.
export const CategoryCell = ({
  value,
}: {
  value?: string[] | null;
}) => {
  const cats = Array.isArray(value) ? value.filter(Boolean) : [];
  if (cats.length === 0) return <span className="text-light-600">—</span>;
  const first = cats[0];
  const extra = cats.length - 1;
  return (
    <span
      className="capitalize text-light-200 truncate"
      title={cats.join(", ")}
    >
      {first}
      {extra > 0 && (
        <span className="ml-1 text-light-500">+{extra}</span>
      )}
    </span>
  );
};

BioactivityTable.displayName = "BioactivityTable";

export default BioactivityTable;
