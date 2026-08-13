"use client";

import { useEffect, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { Portal, Switch } from "@headlessui/react";
import {
  MdCheck,
  MdClose,
  MdDescription,
  MdErrorOutline,
  MdInfoOutline,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdSearch,
  MdTune,
  MdUnfoldMore,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import ResetFiltersButton from "@/components/basic/ResetFiltersButton";
import Chip from "@/components/basic/Chip";
import Link from "@/components/basic/Link";
import Pagination from "@/components/basic/Pagination";
import {
  TableSkeletonCards,
  TableSkeletonRows,
} from "@/components/basic/TableSkeleton";
import SortListbox from "@/components/basic/SortListbox";
import { useReportRows } from "@/context/reportModeContext";
import { AmbiguityBadge } from "@/components/basic/Ambiguity";
import { TrustBadge } from "@/components/basic/TrustBadge";
import FoodCompositionEvidenceModal, {
  EvidenceFilter,
} from "@/components/entities/food/FoodCompositionEvidenceModal";
import { usePaginations } from "@/context/paginationsContext";
import { usePublishTabCount } from "@/context/tabCountsContext";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import {
  getFoodCompositionCounts,
  getFoodCompositionData,
} from "@/utils/fetching";
import { FoodCompositionData } from "@/types";

// headers for table
// One spec drives the <colgroup>, the <th>s and the loading skeleton, so
// the placeholder grid can't drift from the real one.
const TABLE_HEADERS = [
  {
    key: "chemical",
    label: "Chemical",
    sortName: "common_name",
    align: "left" as const,
    width: "w-[30%]",
  },
  {
    key: "classification",
    label: "Classification",
    align: "left" as const,
    width: "w-[20%]",
  },
  {
    key: "concentration",
    label: "Concentration (mg/100g)",
    sortName: "median_concentration",
    align: "right" as const,
    width: "w-[25%]",
  },
  {
    key: "evidence",
    label: "Evidence",
    sortName: "evidence_count",
    align: "right" as const,
    width: "w-[25%]",
  },
];

const CLASSIFICATION_OPTIONS = [
  "alkaloid",
  "amino acid",
  "carbohydrate",
  "fatty acid",
  "flavonoid",
  "glucosinolate",
  "lignan",
  "nucleotide",
  "peptide",
  "polyphenol",
  "stilbenoid",
  "tannin",
  "terpenoid",
  "vitamin",
  "n/a",
];

// mapping of source filters to their labels
const SOURCE_OPTIONS = [
  { value: "fdc", label: "FDC" },
  { value: "foodatlas", label: "FoodAtlas" },
  { value: "ptfi", label: "PTFI" },
];

// Every source starts selected. Derived rather than hardcoded — the initial
// state and the reset handler both used to list sources literally, so adding
// PTFI silently left it unselected on load and un-selected it again on reset.
const ALL_SOURCE_VALUES = SOURCE_OPTIONS.map((o) => o.value);

interface FoodCompositionSectionProps {
  commonName: string;
}

const FoodCompositionSection = ({
  commonName,
}: FoodCompositionSectionProps) => {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [data, setData] = useState<FoodCompositionData[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isError, setIsError] = useState(false);
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations("food-composition-table");
  // Highlight a single row when the user arrived from a chemical page link
  // (`?highlight=`). The backend resolves the page containing the chemical
  // and reports it as metadata.highlight_page; we navigate pagination there
  // on the first response, then clear `findChemical` so subsequent paging
  // doesn't keep snapping back. Highlight dismisses on any click.
  const initialHighlight = (searchParams.get("highlight") ?? "").toLowerCase();
  const [highlightName, setHighlightName] = useState(initialHighlight);
  const [findChemical, setFindChemical] = useState(initialHighlight);
  const [isDismissing, setIsDismissing] = useState(false);
  const [overlayRect, setOverlayRect] = useState<
    { top: number; height: number } | null
  >(null);
  const highlightRowRef = useRef<HTMLTableRowElement | null>(null);
  const tableWrapperRef = useRef<HTMLDivElement | null>(null);
  const [numberOfPages, setNumberOfPages] = useState(-1);
  const [numberOfRows, setNumberOfRows] = useState(-1);
  // Publish the current filtered row count to the Composition tab
  // badge. -1 = "unknown" (initial state) → the badge falls back to
  // the server-prefetched static count until the first fetch resolves.
  usePublishTabCount("composition", numberOfRows >= 0 ? numberOfRows : null);
  const [searchTerm, setSearchTerm] = useState(
    searchParams.get("search") ?? ""
  );
  const [sourceFilters, setSourceFilters] = useState<string[]>(ALL_SOURCE_VALUES);
  const [sort, setSort] = useState({
    column: "median_concentration",
    direction: "desc",
  });
  const [showAllConcentrations, setShowAllConcentrations] = useState(true);
  const [showLowTrust, setShowLowTrust] = useState(false);
  const reporter = useReportRows();
  const [selectedEvidenceName, setSelectedEvidenceName] = useState("");
  const [evidenceFilter, setEvidenceFilter] =
    useState<EvidenceFilter>("all");
  const [sourceCounts, setSourceCounts] = useState<Record<string, number>>({});
  // Empty selection means "no class filter" (show all rows). Users
  // pre-2026-07 saw every checkbox pre-checked which inverted the mental
  // model — clicking "flavonoid" REMOVED it, so rows returned were the
  // complement. Start empty so click = include.
  const [classificationFilter, setClassificationFilter] = useState<string[]>(
    [],
  );
  const [classificationCounts, setClassificationCounts] = useState<
    Record<string, number>
  >({});
  // Counterfactual counts for the Options toggle switches. Undefined
  // while loading OR when the API hasn't returned the field yet (round-2
  // backend addition; frontend gracefully hides the count in that case).
  const [noConcentrationCount, setNoConcentrationCount] = useState<
    number | undefined
  >(undefined);
  const [lowTrustCount, setLowTrustCount] = useState<number | undefined>(
    undefined,
  );
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  // Mobile card view sort options — mirror the sortable desktop
  // columns. Each option encodes column|direction as a single value so
  // the <select> can drive both dimensions in one interaction.
  const MOBILE_SORT_OPTIONS: {
    value: string;
    label: string;
    column: string;
    direction: "asc" | "desc";
  }[] = [
    {
      value: "median_concentration|desc",
      label: "Highest concentration",
      column: "median_concentration",
      direction: "desc",
    },
    {
      value: "median_concentration|asc",
      label: "Lowest concentration",
      column: "median_concentration",
      direction: "asc",
    },
    {
      value: "evidence_count|desc",
      label: "Most evidence",
      column: "evidence_count",
      direction: "desc",
    },
    {
      value: "evidence_count|asc",
      label: "Least evidence",
      column: "evidence_count",
      direction: "asc",
    },
    {
      value: "common_name|asc",
      label: "Chemical A–Z",
      column: "common_name",
      direction: "asc",
    },
    {
      value: "common_name|desc",
      label: "Chemical Z–A",
      column: "common_name",
      direction: "desc",
    },
  ];

  // Faceted counts — refetched whenever any filter changes. Each
  // dimension in the response reflects the other active filters, so
  // the sidebar numbers update as the user narrows the view (e.g.
  // deselecting Source drops the per-Class counts to just what's left).
  useEffect(() => {
    let cancelled = false;
    const fetchCounts = async () => {
      try {
        const counts = await getFoodCompositionCounts(commonName, {
          sourceFilters,
          classificationFilters: classificationFilter,
          showAllConcentrations,
          showLowTrust,
          searchTerm,
        });
        if (cancelled) return;
        setSourceCounts(counts.source_counts);
        setClassificationCounts(counts.classification_counts);
        setNoConcentrationCount(counts.no_concentration_count);
        setLowTrustCount(counts.low_trust_count);
      } catch {
        if (cancelled) return;
        setSourceCounts({});
        setClassificationCounts({});
        setNoConcentrationCount(undefined);
        setLowTrustCount(undefined);
      }
    };
    fetchCounts();
    return () => {
      cancelled = true;
    };
  }, [
    commonName,
    sourceFilters,
    classificationFilter,
    showAllConcentrations,
    showLowTrust,
    searchTerm,
  ]);

  // data fetching
  useEffect(() => {
    const fetchData = async () => {
      // no sources selected, show empty state
      if (sourceFilters.length === 0) {
        setData([]);
        setNumberOfPages(0);
        setNumberOfRows(0);
        setIsLoading(false);
        return;
      }

      try {
        setIsError(false);
        setIsLoading(true);
        // Empty = no filter (show all). Any selection narrows results to
        // rows whose classification array overlaps the selection.
        const activeClsFilter = classificationFilter;
        const result = await getFoodCompositionData(
          commonName,
          currentPage,
          sourceFilters,
          searchTerm,
          sort,
          showAllConcentrations,
          activeClsFilter,
          showLowTrust ? "show_all" : "default",
          findChemical
        );
        // When find_chemical resolves, snap pagination to the served page
        // and stop forcing the find so the user can paginate freely after.
        const resolvedPage: number | null =
          result.metadata?.highlight_page ?? null;
        if (findChemical && resolvedPage && resolvedPage !== currentPage) {
          setTablePaginations(
            "food-composition-table",
            resolvedPage,
            20
          );
        }
        if (findChemical) {
          setFindChemical("");
        }
        // client-side filter: only keep rows with evidence from selected sources
        const filteredData = (
          result.data as FoodCompositionData[]
        ).filter((row) =>
          sourceFilters.some((source) => {
            const evidences =
              row[
                `${source}_evidences` as keyof FoodCompositionData
              ];
            return Array.isArray(evidences) && evidences.length > 0;
          })
        );
        setData(filteredData);
        setNumberOfPages(result.metadata.total_pages);
        setNumberOfRows(result.metadata.total_rows);
      } catch (error) {
        console.error("Error fetching food composition data:", error);
        setIsError(true);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [
    currentPage,
    commonName,
    sourceFilters,
    searchTerm,
    sort,
    showAllConcentrations,
    showLowTrust,
    classificationFilter,
    findChemical,
    setTablePaginations,
  ]);

  // Once the highlighted row is rendered, (1) measure it relative to the
  // table wrapper so the overlay rectangle can be positioned over it, and
  // (2) ease-scroll it into view. The overlay is an absolutely-positioned
  // sibling of the table — never inset into the row — so the row keeps its
  // natural size and the border can't visually shrink anything. Recomputes
  // on resize. Dismiss-on-anywhere-click runs a fade-out before clearing.
  useEffect(() => {
    if (!highlightName) {
      setOverlayRect(null);
      return;
    }
    const measure = () => {
      const row = highlightRowRef.current;
      const wrap = tableWrapperRef.current;
      if (!row || !wrap) return;
      const r = row.getBoundingClientRect();
      const w = wrap.getBoundingClientRect();
      setOverlayRect({
        top: r.top - w.top + wrap.scrollTop,
        height: r.height,
      });
    };
    measure();

    let rafId = 0;
    if (highlightRowRef.current) {
      const rect = highlightRowRef.current.getBoundingClientRect();
      const target =
        rect.top + window.scrollY - window.innerHeight / 2 + rect.height / 2;
      const start = window.scrollY;
      const distance = target - start;
      const duration = 900;
      const ease = (t: number) =>
        t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
      const t0 = performance.now();
      const step = (now: number) => {
        const t = Math.min(1, (now - t0) / duration);
        window.scrollTo(0, start + distance * ease(t));
        if (t < 1) rafId = window.requestAnimationFrame(step);
      };
      rafId = window.requestAnimationFrame(step);
    }

    const onResize = () => measure();
    window.addEventListener("resize", onResize);

    // Dismiss on any user interaction — click, keypress, touch, wheel, or
    // a real (user-driven) scroll. Listener registration is delayed past the
    // RAF-scroll duration so the programmatic scroll doesn't self-dismiss.
    const dismissEvents: (keyof WindowEventMap)[] = [
      "mousedown",
      "click",
      "keydown",
      "touchstart",
      "wheel",
      "scroll",
    ];
    let dismissed = false;
    const handleInteraction = () => {
      if (dismissed) return;
      dismissed = true;
      dismissEvents.forEach((e) =>
        window.removeEventListener(e, handleInteraction)
      );
      setIsDismissing(true);
      window.setTimeout(() => {
        setHighlightName("");
        setIsDismissing(false);
        const params = new URLSearchParams(searchParams.toString());
        if (params.has("highlight")) {
          params.delete("highlight");
          const qs = params.toString();
          router.replace(qs ? `${pathname}?${qs}` : pathname, {
            scroll: false,
          });
        }
      }, 450);
    };
    const id = window.setTimeout(() => {
      dismissEvents.forEach((e) =>
        window.addEventListener(e, handleInteraction, { passive: true })
      );
    }, 1100);
    return () => {
      window.clearTimeout(id);
      window.cancelAnimationFrame(rafId);
      window.removeEventListener("resize", onResize);
      dismissEvents.forEach((e) =>
        window.removeEventListener(e, handleInteraction)
      );
    };
  }, [highlightName, data, pathname, router, searchParams]);

  // handle source filter change
  const handleFilterChange = (sources: string[]) => {
    setTablePaginations("food-composition-table", 1, 20);
    setSourceFilters(sources);
  };

  // Default state per the useState initializers above. `isFiltersDirty`
  // is true when the current view differs from a fresh page load; the
  // Reset button only renders in that case so it's not just visual noise.
  const isFiltersDirty =
    searchTerm !== "" ||
    // Compare against the real default rather than a literal source list.
    // This was hardcoded to fdc+foodatlas, so once PTFI joined the default
    // the logic inverted: the untouched panel counted as dirty, and
    // deselecting PTFI counted as clean.
    sourceFilters.length !== ALL_SOURCE_VALUES.length ||
    ALL_SOURCE_VALUES.some((v) => !sourceFilters.includes(v)) ||
    classificationFilter.length > 0 ||
    !showAllConcentrations ||
    showLowTrust;

  const resetAllFilters = () => {
    setSearchTerm("");
    setSourceFilters(ALL_SOURCE_VALUES);
    setClassificationFilter([]);
    setShowAllConcentrations(true);
    setShowLowTrust(false);
    setTablePaginations("food-composition-table", 1, 20);
  };

  // Empty-state body shared between desktop table + mobile card list.
  // When filters are active, we surface a filter-aware message + inline
  // "clear filters" button so the reader doesn't confuse "your filters
  // returned nothing" with "this food has no composition data at all".
  const emptyStateBody = isFiltersDirty ? (
    <div className="flex flex-col items-center gap-2 text-light-300">
      <div className="flex items-center gap-2 text-sm">
        <MdInfoOutline />
        No associations match your filters
      </div>
      <button
        type="button"
        onClick={resetAllFilters}
        className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
      >
        clear filters
      </button>
    </div>
  ) : (
    <div className="flex items-center gap-2 text-light-300 text-sm">
      <MdInfoOutline /> No associations found
    </div>
  );

  // handle evidence button click
  const handleEvidenceButtonClick = (
    event: React.MouseEvent<HTMLButtonElement>,
    name: string
  ) => {
    event.preventDefault();
    event.stopPropagation();
    setEvidenceFilter("all");
    setSelectedEvidenceName(name);
  };

  // The evidence modal no longer supports an ambiguity filter (per
  // 2026-07-24 UX call — see FoodCompositionEvidenceModal), so this
  // just opens the modal unfiltered. The badge still communicates
  // "there's chemical ambiguity here" via its count.
  const handleAmbiguityBadgeClick = (
    event: React.MouseEvent<HTMLButtonElement>,
    name: string
  ) => {
    event.preventDefault();
    event.stopPropagation();
    setEvidenceFilter("all");
    setSelectedEvidenceName(name);
  };

  // handle trust badge click (opens modal pre-filtered to low-trust)
  const handleTrustBadgeClick = (
    event: React.MouseEvent<HTMLButtonElement>,
    name: string
  ) => {
    event.preventDefault();
    event.stopPropagation();
    setEvidenceFilter("low-trust");
    setSelectedEvidenceName(name);
  };

  // count evidences (= "data points") that contain ≥1 chemical-ambiguous
  // extraction. Food-side ambiguity is surfaced by the page banner instead,
  // so we intentionally exclude it from the row-level count.
  const getRowAmbiguousCount = (row: FoodCompositionData) => {
    const all = [
      ...(row.foodatlas_evidences ?? []),
      ...(row.fdc_evidences ?? []),
      ...(row.ptfi_evidences ?? []),
    ];
    return all.filter((ev) =>
      ev.extraction.some((ex) => (ex.chemical_candidates?.length ?? 0) > 1)
    ).length;
  };

  // count evidences (= "data points") that contain ≥1 low-trust extraction.
  // Only meaningful when showLowTrust is on (otherwise the API filtered them
  // server-side and the annotation isn't on the response).
  const getRowLowTrustCount = (row: FoodCompositionData) => {
    const all = [
      ...(row.foodatlas_evidences ?? []),
      ...(row.fdc_evidences ?? []),
      ...(row.ptfi_evidences ?? []),
    ];
    return all.filter((ev) => ev.extraction.some((ex) => ex.trust_low === true))
      .length;
  };

  // handle search
  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(() => {
      setTablePaginations("food-composition-table", 1, 20);
      return e.target.value.toLowerCase();
    });
  };

  const handleSearchClear = () => {
    setSearchTerm("");
    setTablePaginations("food-composition-table", 1, 20);
  };

  // handle sort column click
  const handleSortClick = (sortName: string) => {
    setSort((prevSort: { column: string; direction: string }) => {
      setTablePaginations("food-composition-table", 1, 20);
      const isSameColumn = prevSort.column === sortName;
      return {
        column: sortName,
        direction:
          isSameColumn && prevSort.direction === "asc" ? "desc" : "asc",
      };
    });
  };

  const handleConcentrationSwitchChange = () => {
    setShowAllConcentrations((prev) => !prev);
    setTablePaginations("food-composition-table", 1, 20);
  };

  const handleLowTrustSwitchChange = () => {
    setShowLowTrust((prev) => !prev);
    setTablePaginations("food-composition-table", 1, 20);
  };

  const getRowEvidenceCount = (row: FoodCompositionData) =>
    (row.foodatlas_evidences?.length || 0) +
    (row.fdc_evidences?.length || 0) +
    (row.ptfi_evidences?.length || 0);

  // number of placeholder rows to make up for the total of 20 rows
  const placeholderRowsCount = data ? 20 - data?.length : 20;


  const toggleClassification = (cls: string) => {
    setTablePaginations("food-composition-table", 1, 20);
    setClassificationFilter((prev) =>
      prev.includes(cls) ? prev.filter((c) => c !== cls) : [...prev, cls]
    );
  };

  const toggleSource = (source: string) => {
    setTablePaginations("food-composition-table", 1, 20);
    setSourceFilters((prev) =>
      prev.includes(source)
        ? prev.filter((s) => s !== source)
        : [...prev, source]
    );
  };

  // Render every classification option so the sidebar space is stable;
  // rows with count=0 render disabled (see FilterListItem).
  const visibleClassOptions = CLASSIFICATION_OPTIONS;

  // Search field is used in three places: inside the sidebar (with
  // filters), inside the mobile drawer's sidebar copy, and on its own
  // as a standalone left-of-Filters affordance below 1440. Extract it
  // so all three stay in sync.
  const searchInput = (
    <div className="relative flex items-center">
      <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
      <input
        className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none"
        type="text"
        placeholder="Search…"
        value={searchTerm}
        onChange={handleSearch}
      />
      {searchTerm && (
        <button
          type="button"
          aria-label="Clear search"
          onClick={handleSearchClear}
          className="absolute right-2 flex items-center justify-center w-4 h-4 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
        >
          <MdClose className="w-3 h-3" />
        </button>
      )}
    </div>
  );

  // Non-search filter controls — options + source + class. Drawer on
  // small viewports uses this alone (search stays visible outside the
  // drawer per user request).
  const filtersOnlyPanel = (
    <div className="flex flex-col gap-5">
      {/* Binary switches, not a multi-select — each one ADDS its category
       * of row to the table rather than selecting among a partition. The
       * distinction matters: unlike Source, where every row has exactly
       * one and deselecting all correctly yields nothing, a row can
       * belong to neither of these categories. Turning both off is
       * therefore not "show nothing", it's "show only rows that are
       * neither" — which is exactly what it does. The labels lead with
       * "Include" so that subtractive behaviour is legible. */}
      <FilterGroup label="Include">
        <div className="flex flex-col gap-2 pt-0.5">
          <ToggleSwitch
            label="Without concentration"
            count={noConcentrationCount}
            checked={showAllConcentrations}
            onChange={handleConcentrationSwitchChange}
          />
          <ToggleSwitch
            label="Low-trust data points"
            count={lowTrustCount}
            checked={showLowTrust}
            onChange={handleLowTrustSwitchChange}
          />
        </div>
      </FilterGroup>

      {/* source — checkbox list, one row per source */}
      <FilterGroup label="Source">
        <FilterList>
          {SOURCE_OPTIONS.map((opt) => {
            const c = sourceCounts[opt.value];
            return (
              <FilterListItem
                key={opt.value}
                label={opt.label}
                count={c}
                selected={sourceFilters.includes(opt.value)}
                onClick={() => toggleSource(opt.value)}
                disabled={c === 0}
              />
            );
          })}
        </FilterList>
      </FilterGroup>

      {/* nutrient classification — same checklist chrome; 15+ options
       * scrolls internally if the list would push the sticky sidebar
       * past the viewport. */}
      <FilterGroup
        label="Class"
        action={
          classificationFilter.length > 0 ? (
            <button
              type="button"
              onClick={() => {
                setTablePaginations("food-composition-table", 1, 20);
                setClassificationFilter([]);
              }}
              className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
            >
              clear
            </button>
          ) : null
        }
      >
        <FilterList maxHeightClass="max-h-72">
          {visibleClassOptions.map((cls) => {
            const c = classificationCounts[cls] ?? 0;
            return (
              <FilterListItem
                key={cls}
                label={cls === "n/a" ? "unclassified" : cls}
                count={c}
                selected={classificationFilter.includes(cls)}
                onClick={() => toggleClassification(cls)}
                disabled={c === 0}
              />
            );
          })}
        </FilterList>
      </FilterGroup>

      {/* Panel-level action, last so it reads as "undo everything above".
       * Clears search + every filter and snaps pagination to page 1. */}
      <ResetFiltersButton
        isDirty={isFiltersDirty}
        onReset={resetAllFilters}
      />
    </div>
  );

  // Full sidebar (search on top + filters underneath). Used both in the
  // desktop absolute-positioned aside and as the mobile-view content
  // that pairs a visible search input with a drawer of the remaining
  // controls.
  const filterPanel = (
    <div className="flex flex-col gap-5">
      {searchInput}
      {filtersOnlyPanel}
    </div>
  );

  return (
    <>
      <div id="composition" className="relative scroll-mt-8">
          {/* Desktop sidebar sits OUTSIDE the table's flow — absolutely
           * positioned to the left of the composition wrapper via
           * `right-full`, so the table keeps its full centered max-width.
           * top-0 bottom-0 stretches the aside to the section's height so
           * the inner `sticky top-4` div can trail the scroll until the
           * section ends. Only shown at min-[1440px]+ where the outer
           * max-w-5xl gutter has enough room for the w-48 aside plus
           * mr-4 gap; the drawer covers narrower widths. */}
          <aside className="hidden min-[1440px]:block absolute right-full mr-10 -top-[17px] bottom-0 w-48">
            {/* -top-[17px] sits halfway between -top-4 (16) and -top-5
             * (20) so the aside lines up with the Card border-top;
             * mr-10 (40px) gives a clear gap from the Card frame.
             * Wrapping the inner box in <Card> matches the tab card's
             * exact border/shadow/rounded so the sidebar and the
             * table frame read as siblings. */}
            <div className="sticky top-4">
              <Card>{filterPanel}</Card>
            </div>
          </aside>

          {/* Sub-1440 row: search visible on the left, Filters trigger
           * on the right. Search stays outside the drawer so the user
           * doesn't have to open a modal to type. */}
          <div className="min-[1440px]:hidden mb-1 flex items-center gap-3">
            <div className="flex-1 min-w-0 max-w-xs">{searchInput}</div>
            <button
              type="button"
              onClick={() => setMobileFiltersOpen(true)}
              className="inline-flex items-center gap-2 rounded-md border border-light-700/60 bg-light-900/60 px-3 py-1.5 text-xs font-mono italic text-light-300 hover:text-light-100 hover:border-light-500 transition-colors"
            >
              <MdTune className="w-4 h-4" />
              Filters
            </button>
          </div>

          <div className="flex flex-col gap-7">
          <div>
          {/* Row-count line dropped — the Composition tab badge now
           * reflects the filtered total via usePublishTabCount. Mobile
           * sort stays here (no column headers to click on card view). */}
          {!isLoading && numberOfRows > 0 && (
            <div className="mb-1.5 mt-1 md:hidden flex justify-end items-center gap-2">
              <span className="font-mono italic text-[11px] text-light-500">
                sort
              </span>
              <SortListbox
                value={`${sort.column}|${sort.direction}`}
                options={MOBILE_SORT_OPTIONS}
                onChange={(value) => {
                  const opt = MOBILE_SORT_OPTIONS.find(
                    (o) => o.value === value
                  );
                  if (!opt) return;
                  setSort({ column: opt.column, direction: opt.direction });
                  setTablePaginations("food-composition-table", 1, 20);
                }}
              />
            </div>
          )}
          {/* table — desktop only. Card list below covers mobile. */}
          <div
            ref={tableWrapperRef}
            className="hidden md:block overflow-x-auto relative"
          >
            {highlightName && overlayRect && (
              <div
                aria-hidden
                className={
                  isDismissing
                    ? "row-highlight-overlay is-dismissing"
                    : "row-highlight-overlay"
                }
                style={{
                  top: overlayRect.top - 3,
                  height: overlayRect.height + 6,
                }}
              />
            )}
            <table className="w-full table-fixed">
              <colgroup>
                {TABLE_HEADERS.map((h) => (
                  <col key={h.key} className={h.width} />
                ))}
              </colgroup>
              <thead className="text-light-400 text-left">
                <tr>
                  {/* table headers */}
                  {TABLE_HEADERS.map((header, index) => (
                    <th
                      key={index}
                      className={`h-9 border-b border-light-700 leading-none break-all md:break-normal py-1.5 ${
                        index === 0
                          ? "pr-4"
                          : index === TABLE_HEADERS.length - 1
                          ? "pl-4"
                          : "px-4"
                      } ${header.align === "right" ? "text-right" : "text-left"}`}
                    >
                      <div
                        className={`group flex gap-1 items-center flex-nowrap w-full ${
                          header.sortName
                            ? "cursor-pointer"
                            : "pointer-events-none"
                        } ${header.align === "right" ? "justify-end" : "justify-between"}`}
                        onClick={() =>
                          header.sortName && handleSortClick(header.sortName)
                        }
                      >
                        <span
                          className={`select-none uppercase text-xs font-medium group-hover:text-light-100 transition duration-300 ease-in-out ${
                            header.sortName === sort.column
                              ? "text-light-100"
                              : ""
                          }`}
                        >
                          {header.label}
                        </span>
                        {header.sortName &&
                          (header.sortName === sort.column ? (
                            sort.direction === "asc" ? (
                              <MdKeyboardArrowDown className="text-accent-600 group-hover:text-accent-300 transition duration-300 ease-in-out flex-shrink-0" />
                            ) : (
                              <MdKeyboardArrowUp className="text-accent-600 group-hover:text-accent-300 transition duration-300 ease-in-out flex-shrink-0" />
                            )
                          ) : (
                            <MdUnfoldMore className="text-light-400 group-hover:text-light-100 transition duration-300 ease-in-out flex-shrink-0" />
                          ))}
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="text-sm font-light">
                {isLoading ? (
                  <TableSkeletonRows columns={TABLE_HEADERS} />
                ) : isError ? (
                  // error message
                  <tr>
                    <td colSpan={TABLE_HEADERS.length}>
                      <div className="h-[10rem] flex items-center justify-center text-red-400 gap-2">
                        <MdErrorOutline /> An error occurred fetching data,
                        please refresh the page
                      </div>
                    </td>
                  </tr>
                ) : data.length > 0 ? (
                  data.map((row) => {
                    const isHighlighted =
                      !!highlightName &&
                      (row.name.toLowerCase() === highlightName ||
                        (row.id ?? "").toLowerCase() === highlightName);
                    const rowReportProps = reporter.getRowProps({
                      kind: "food-composition-row",
                      entityType: "food",
                      entitySlug: commonName,
                      chemicalId: row.id,
                      chemicalName: row.name,
                      dataPointCount: getRowEvidenceCount(row),
                    });
                    return (
                    <tr
                      key={row.id}
                      ref={isHighlighted ? highlightRowRef : null}
                      {...rowReportProps}
                    >
                      {/* name */}
                      <td className="py-1.5 pr-4">
                        <div className="flex min-h-9 capitalize items-center gap-2">
                          <Link
                            href={`/chemical/${encodeURIComponent(
                              encodeSpace(row.name)
                            )}`}
                            isExternal={false}
                          >
                            {row.name}
                          </Link>
                          <AmbiguityBadge
                            ambiguousCount={getRowAmbiguousCount(row)}
                            totalCount={getRowEvidenceCount(row)}
                            onClick={(e) =>
                              handleAmbiguityBadgeClick(e, row.name)
                            }
                          />
                          <TrustBadge
                            lowTrustCount={getRowLowTrustCount(row)}
                            totalCount={getRowEvidenceCount(row)}
                            onClick={(e) =>
                              handleTrustBadgeClick(e, row.name)
                            }
                          />
                        </div>
                      </td>
                      {/* classification */}
                      <td className="py-1.5 px-4">
                        <div className="flex min-h-9 capitalize items-center">
                          {row.chemical_classification.length > 0
                            ? row.chemical_classification.join(", ")
                            : "—"}
                        </div>
                      </td>
                      {/* median concentration — value + % of the food's mass
                       * (mg/100g → divide by 1000). Inline bar chart was
                       * removed; the column now holds only the numbers so
                       * it can sit at the same 25% width as siblings. */}
                      <td className="py-1.5 px-4">
                        <div className="flex min-h-9 items-center justify-end gap-3">
                          {(() => {
                            const v = row.median_concentration?.value;
                            if (v === null || v === undefined) {
                              return (
                                <span className="text-light-600">—</span>
                              );
                            }
                            const unit = row.median_concentration?.unit;
                            const isMgPer100g =
                              !!unit &&
                              unit.replace(/\s+/g, "").toLowerCase() ===
                                "mg/100g";
                            const pct = isMgPer100g ? v / 1000 : null;
                            const fmtPct =
                              pct === null
                                ? ""
                                : pct >= 10
                                ? `${pct.toFixed(0)}%`
                                : pct >= 1
                                ? `${pct.toFixed(1)}%`
                                : `${pct.toFixed(2)}%`;
                            return (
                              <>
                                <span className="font-mono text-xs text-light-200 whitespace-nowrap tabular-nums text-right min-w-[5rem]">
                                  {formatConcentrationValueAlt(v)}
                                </span>
                                {fmtPct && (
                                  <span
                                    className="font-mono text-xs text-light-500 whitespace-nowrap tabular-nums text-right min-w-[3.5rem]"
                                    title="Percentage of the food's mass"
                                  >
                                    {fmtPct}
                                    <span className="ml-1 text-light-600">
                                      by mass
                                    </span>
                                  </span>
                                )}
                              </>
                            );
                          })()}
                        </div>
                      </td>
                      {/* evidence */}
                      <td className="py-1.5 pl-4">
                        <div className="flex min-h-9 items-center justify-end">
                          <Chip
                            icon={<MdDescription className="size-3" />}
                            label={`${getRowEvidenceCount(row)} data point${
                              getRowEvidenceCount(row) === 1 ? "" : "s"
                            }`}
                            tone="outline"
                            size="md"
                            onClick={(event) =>
                              handleEvidenceButtonClick(event, row.name)
                            }
                            className="min-w-[9rem] justify-center"
                          />
                        </div>
                      </td>
                    </tr>
                    );
                  })
                ) : (
                  // no rows
                  <tr>
                    <td colSpan={TABLE_HEADERS.length}>
                      <div className="h-[10rem] flex items-center justify-center">
                        {emptyStateBody}
                      </div>
                    </td>
                  </tr>
                )}
                {/* add empty rows to make up for the total of 20 rows */}
                {numberOfPages > 1 &&
                  !isLoading &&
                  Array.from({ length: placeholderRowsCount }, (_, index) => (
                    <tr key={index}>
                      <td className="py-1.5" colSpan={TABLE_HEADERS.length}>
                        <div className="h-9" />
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>

          {/* Mobile card list — replaces the table below md:. Sort
           * control lives in the row-count header above (no column
           * headers to click in card view). */}
          <div className="md:hidden">
            {isLoading && <TableSkeletonCards columns={TABLE_HEADERS} />}
            <div
              className={
                isLoading
                  ? "hidden"
                  : "w-full flex flex-col divide-y divide-light-800"
              }
            >
              {isError ? (
                <div className="w-full py-6 flex items-center justify-center text-red-400 gap-2">
                  <MdErrorOutline /> An error occurred fetching data, please
                  refresh the page
                </div>
              ) : data.length > 0 ? (
                data.map((row) => {
                  const isHighlighted =
                    !!highlightName &&
                    (row.name.toLowerCase() === highlightName ||
                      (row.id ?? "").toLowerCase() === highlightName);
                  const v = row.median_concentration?.value;
                  const unit = row.median_concentration?.unit;
                  const isMgPer100g =
                    !!unit &&
                    unit.replace(/\s+/g, "").toLowerCase() === "mg/100g";
                  const pct = v != null && isMgPer100g ? v / 1000 : null;
                  const fmtPct =
                    pct === null
                      ? ""
                      : pct >= 10
                      ? `${pct.toFixed(0)}%`
                      : pct >= 1
                      ? `${pct.toFixed(1)}%`
                      : `${pct.toFixed(2)}%`;
                  const evidenceCount = getRowEvidenceCount(row);
                  const classifications =
                    row.chemical_classification.length > 0
                      ? row.chemical_classification.join(", ")
                      : "—";
                  const rowReportProps = reporter.getRowProps({
                    kind: "food-composition-row",
                    entityType: "food",
                    entitySlug: commonName,
                    chemicalId: row.id,
                    chemicalName: row.name,
                    dataPointCount: evidenceCount,
                  });
                  return (
                    <div
                      key={row.id}
                      {...rowReportProps}
                      className={twMerge(
                        `w-full py-3 flex flex-col gap-2 ${
                          isHighlighted
                            ? "bg-accent-500/5 -mx-2 px-2 rounded"
                            : ""
                        }`,
                        rowReportProps.className,
                      )}
                    >
                      {/* Name row — same on both variants */}
                      <div className="flex items-center gap-2 flex-wrap capitalize">
                        <Link
                          href={`/chemical/${encodeURIComponent(
                            encodeSpace(row.name)
                          )}`}
                          isExternal={false}
                        >
                          {row.name}
                        </Link>
                        <AmbiguityBadge
                          ambiguousCount={getRowAmbiguousCount(row)}
                          totalCount={evidenceCount}
                          onClick={(e) =>
                            handleAmbiguityBadgeClick(e, row.name)
                          }
                        />
                        <TrustBadge
                          lowTrustCount={getRowLowTrustCount(row)}
                          totalCount={evidenceCount}
                          onClick={(e) =>
                            handleTrustBadgeClick(e, row.name)
                          }
                        />
                      </div>

                      {/* Concentration line + inline classification &
                       * evidence row, both spanning full card width
                       * via justify-between. The "by mass" tag on the
                       * percentage matches the desktop table. */}
                      <div className="w-full flex items-baseline justify-between gap-2 font-mono text-sm text-light-100 tabular-nums">
                        {v == null ? (
                          <span className="text-light-600">—</span>
                        ) : (
                          <>
                            <span>
                              {formatConcentrationValueAlt(v)}
                              {unit && (
                                <span className="text-light-500 text-xs ml-1">
                                  {unit}
                                </span>
                              )}
                            </span>
                            {fmtPct && (
                              <span className="text-light-500 text-xs">
                                {fmtPct}
                                <span
                                  className="ml-1 not-italic text-light-600"
                                  title="Percentage of the food's mass"
                                >
                                  by mass
                                </span>
                              </span>
                            )}
                          </>
                        )}
                      </div>
                      <div className="w-full flex items-center justify-between gap-2 text-xs">
                        <span className="text-light-400 capitalize">
                          {classifications}
                        </span>
                        <Chip
                          icon={<MdDescription className="size-3" />}
                          label={`${evidenceCount} data point${
                            evidenceCount === 1 ? "" : "s"
                          }`}
                          tone="outline"
                          size="md"
                          onClick={(event) =>
                            handleEvidenceButtonClick(event, row.name)
                          }
                        />
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="w-full py-6 flex items-center justify-center">
                  {emptyStateBody}
                </div>
              )}
            </div>
          </div>
          </div>
          {/* pagination */}
          {(numberOfPages > 1 || isLoading) && (
            <div className="mt-8 max-w-xl w-full mx-auto">
              <Pagination
                tableId={"food-composition-table"}
                numberOfPages={numberOfPages}
                isLoading={isLoading}
              />
            </div>
          )}
          </div>

          {/* Drawer — slides in from the right, dark backdrop behind.
           * Same filterPanel as the sidebar. Esc/backdrop close. Shown
           * on every viewport that doesn't have the desktop sidebar. */}
          {mobileFiltersOpen && (
            <div
              className="fixed inset-0 z-50 min-[1440px]:hidden"
              role="dialog"
              aria-modal="true"
              aria-label="Filters"
            >
              <button
                type="button"
                aria-label="Close filters"
                onClick={() => setMobileFiltersOpen(false)}
                className="absolute inset-0 bg-black/60 cursor-default"
              />
              <aside className="absolute right-0 top-0 h-full w-[85vw] max-w-sm bg-light-950 border-l border-light-700/50 overflow-y-auto flex flex-col gap-4 p-4">
                <div className="flex items-center justify-between">
                  <span className="font-mono italic text-sm text-light-300">
                    Filters
                  </span>
                  <button
                    type="button"
                    aria-label="Close filters"
                    onClick={() => setMobileFiltersOpen(false)}
                    className="w-8 h-8 rounded-full flex items-center justify-center text-light-400 hover:text-light-100 hover:bg-light-800 transition-colors"
                  >
                    <MdClose className="w-4 h-4" />
                  </button>
                </div>
                {filtersOnlyPanel}
              </aside>
            </div>
          )}
      </div>
      {/* evidence modal */}
      <Portal>
        <FoodCompositionEvidenceModal
          foodName={commonName}
          chemicalName={selectedEvidenceName}
          evidences={(() => {
            const selectedRow = data?.find(
              (row) => row.name === selectedEvidenceName
            );
            if (!selectedRow) return undefined;
            return [
              ...(selectedRow.fdc_evidences ?? []),
              ...(selectedRow.foodatlas_evidences ?? []),
              ...(selectedRow.ptfi_evidences ?? []),
            ];
          })()}
          isOpen={selectedEvidenceName !== ""}
          onClose={() => setSelectedEvidenceName("")}
          initialFilter={evidenceFilter}
        />
      </Portal>
    </>
  );
};

FoodCompositionSection.displayName = "FoodCompositionSection";

// -- Filter block chrome -----------------------------------------------------
// Small, purely-presentational helpers so the JSX above reads as a filter
// spec ("Options | Source | Class") rather than a wall of class strings.

const FilterRowLabel = ({ children }: { children: React.ReactNode }) => (
  <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400 min-w-[3.5rem]">
    {children}
  </span>
);

const ToggleSwitch = ({
  label,
  count,
  checked,
  onChange,
}: {
  label: string;
  // Count of rows the toggle governs (e.g. rows without concentration).
  // Rendered as a right-aligned mono badge; omitted when undefined.
  count?: number;
  checked: boolean;
  onChange: () => void;
}) => (
  <label className="flex items-center gap-2 cursor-pointer select-none">
    <Switch
      checked={checked}
      onChange={onChange}
      className="group inline-flex h-4 w-8 items-center rounded-full bg-light-700 data-[checked]:bg-accent-600 flex-shrink-0 transition-colors"
    >
      <span className="size-3 translate-x-0.5 rounded-full bg-white transition group-data-[checked]:translate-x-[1.125rem]" />
    </Switch>
    <span
      className={twMerge(
        "text-xs transition-colors flex-1 min-w-0 leading-tight",
        checked ? "text-light-100" : "text-light-400"
      )}
    >
      {label}
    </span>
    {typeof count === "number" && (
      <span
        className={twMerge(
          "tabular-nums text-[10px] flex-shrink-0",
          checked ? "text-light-400" : "text-light-500"
        )}
      >
        {count.toLocaleString()}
      </span>
    )}
  </label>
);

// A labelled section in the filter sidebar. The optional `action` sits in
// the label row (right-aligned) — used by Class for the "all / clear"
// button so it doesn't need its own row.
const FilterGroup = ({
  label,
  action,
  children,
}: {
  label: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) => (
  <div className="flex flex-col gap-1.5">
    <div className="flex items-baseline justify-between gap-2">
      <FilterRowLabel>{label}</FilterRowLabel>
      {action}
    </div>
    {children}
  </div>
);

// A vertical list of checkbox rows. Optionally caps its height + scrolls
// so Class (15+ items) doesn't push the sticky sidebar past the viewport.
const FilterList = ({
  maxHeightClass,
  children,
}: {
  maxHeightClass?: string;
  children: React.ReactNode;
}) => (
  <div
    className={twMerge(
      "flex flex-col -mx-1",
      maxHeightClass ? `${maxHeightClass} overflow-y-auto` : undefined
    )}
  >
    {children}
  </div>
);

// One row in the filter list. Full-width click target, checkbox affordance
// on the left, label in the middle, count right-aligned. Full-row hover
// state makes the whole thing feel tappable.
//
// When count is 0 the row renders `disabled` — visible but greyed and
// non-interactive — instead of being hidden, so the filter space keeps
// the same shape whatever the pivot entity has evidence for.
const FilterListItem = ({
  label,
  count,
  selected,
  onClick,
  disabled,
}: {
  label: string;
  count?: number;
  selected: boolean;
  onClick: () => void;
  disabled?: boolean;
}) => (
  <button
    type="button"
    onClick={onClick}
    disabled={disabled}
    aria-pressed={selected}
    aria-disabled={disabled || undefined}
    className={twMerge(
      "group w-full flex items-center gap-2 pl-1 pr-2 py-1 rounded transition-colors text-left",
      selected
        ? "text-light-100 hover:bg-light-900/70"
        : "text-light-400 hover:text-light-100 hover:bg-light-900/50",
      disabled && "opacity-40 cursor-not-allowed hover:bg-transparent hover:text-light-400"
    )}
  >
    <span
      aria-hidden
      className={twMerge(
        "w-3.5 h-3.5 rounded-[3px] border flex-shrink-0 flex items-center justify-center transition-colors",
        selected
          ? "border-accent-600 bg-accent-600/20 text-accent-600"
          : "border-light-700 group-hover:border-light-500",
        disabled && "group-hover:border-light-700"
      )}
    >
      {selected && <MdCheck className="w-3 h-3" />}
    </span>
    <span className="capitalize font-mono italic text-xs flex-1 min-w-0 truncate">
      {label}
    </span>
    {typeof count === "number" && (
      <span
        className={twMerge(
          "not-italic tabular-nums text-[10px] flex-shrink-0",
          selected ? "text-light-400" : "text-light-500"
        )}
      >
        {count}
      </span>
    )}
  </button>
);

export default FoodCompositionSection;
