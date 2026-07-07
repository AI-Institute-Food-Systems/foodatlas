"use client";

import { useEffect, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import {
  Listbox,
  ListboxButton,
  ListboxOption,
  ListboxOptions,
  Popover,
  PopoverButton,
  PopoverPanel,
  Portal,
  Switch,
} from "@headlessui/react";
import {
  MdCheck,
  MdClose,
  MdDescription,
  MdErrorOutline,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdSearch,
  MdUnfoldMore,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Button from "@/components/basic/Button";
import Link from "@/components/basic/Link";
import Pagination from "@/components/basic/Pagination";
import LoadingCard from "@/components/basic/LoadingCard";
import { AmbiguityBadge } from "@/components/basic/Ambiguity";
import { TrustBadge } from "@/components/basic/TrustBadge";
import FoodCompositionEvidenceModal, {
  EvidenceFilter,
} from "@/components/entities/food/FoodCompositionEvidenceModal";
import { usePaginations } from "@/context/paginationsContext";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import {
  getFoodCompositionCounts,
  getFoodCompositionData,
} from "@/utils/fetching";
import { FoodCompositionData } from "@/types";

// headers for table
const TABLE_HEADERS = [
  { label: "Chemical", sortName: "common_name", align: "left" as const },
  { label: "Classification", align: "left" as const, filterable: true },
  {
    label: "Concentration (mg/100g)",
    sortName: "median_concentration",
    align: "right" as const,
  },
  { label: "Evidence", align: "right" as const },
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
];

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
  const [searchTerm, setSearchTerm] = useState(
    searchParams.get("search") ?? ""
  );
  const [sourceFilters, setSourceFilters] = useState<string[]>([
    "fdc",
    "foodatlas",
  ]);
  const [sort, setSort] = useState({
    column: "median_concentration",
    direction: "desc",
  });
  const [showAllConcentrations, setShowAllConcentrations] = useState(true);
  const [showLowTrust, setShowLowTrust] = useState(false);
  const [selectedEvidenceName, setSelectedEvidenceName] = useState("");
  const [evidenceFilter, setEvidenceFilter] =
    useState<EvidenceFilter>("all");
  const [sourceCounts, setSourceCounts] = useState<Record<string, number>>({});
  const [classificationFilter, setClassificationFilter] = useState<
    string[]
  >([...CLASSIFICATION_OPTIONS]);
  const [classificationCounts, setClassificationCounts] = useState<
    Record<string, number>
  >({});

  // fetch source + classification counts in one call
  useEffect(() => {
    const fetchCounts = async () => {
      try {
        const counts = await getFoodCompositionCounts(commonName);
        setSourceCounts(counts.source_counts);
        setClassificationCounts(counts.classification_counts);
        // Initialize filter to only classes that have results
        setClassificationFilter(
          CLASSIFICATION_OPTIONS.filter(
            (cls) => (counts.classification_counts[cls] ?? 0) > 0
          )
        );
      } catch {
        setSourceCounts({});
        setClassificationCounts({});
      }
    };
    fetchCounts();
  }, [commonName]);

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
        const visibleCount = CLASSIFICATION_OPTIONS.filter(
          (cls) => (classificationCounts[cls] ?? 0) > 0
        ).length;
        const activeClsFilter =
          classificationFilter.length >= visibleCount
            ? []
            : classificationFilter;
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
    classificationCounts,
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

  // handle ambiguity badge click (opens modal pre-filtered to ambiguous)
  const handleAmbiguityBadgeClick = (
    event: React.MouseEvent<HTMLButtonElement>,
    name: string
  ) => {
    event.preventDefault();
    event.stopPropagation();
    setEvidenceFilter("ambiguous");
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
    (row.fdc_evidences?.length || 0);

  // number of placeholder rows to make up for the total of 20 rows
  const placeholderRowsCount = data ? 20 - data?.length : 20;


  return (
    <>
      <div id="composition" className="flex flex-col gap-7 scroll-mt-8">
          {/* table controls */}
          <div className="w-full flex flex-col lg:flex-row justify-between">
            {/* search */}
            <div className="relative flex items-center">
              <MdSearch className="absolute left-2.5 w-5 h-5 text-light-400" />
              <input
                className="pl-9 pr-9 w-full lg:w-72 h-9 text-sm rounded-lg border border-light-50/5 bg-light-900 focus:bg-light-400/20 hover:bg-light-400/20 text-light-100 placeholder-light-400 transition duration-100 ease-in-out outline-light-50/60"
                type="text"
                placeholder="Search for a chemical"
                value={searchTerm}
                onChange={handleSearch}
              />
              {searchTerm && (
                <button
                  type="button"
                  aria-label="Clear search"
                  onClick={handleSearchClear}
                  className="absolute right-2 flex items-center justify-center w-5 h-5 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
                >
                  <MdClose className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
            {/* switch and filters — compact track + shorter label copy */}
            <div className="mt-4 lg:mt-0 flex gap-3 lg:gap-6 justify-between flex-col md:flex-row md:items-center">
              {/* switch to remove n/a concentrations */}
              <div className="flex gap-2 items-center justify-between">
                <span className="uppercase text-[10px] tracking-wider text-light-400 md:max-w-[8rem] lg:text-right leading-tight">
                  include without concentration
                </span>
                <Switch
                  checked={showAllConcentrations}
                  onChange={handleConcentrationSwitchChange}
                  className="group inline-flex h-4 w-8 items-center rounded-full bg-light-700 data-[checked]:bg-accent-600 data-[disabled]:cursor-not-allowed data-[disabled]:opacity-50 flex-shrink-0 transition-colors"
                >
                  <span className="size-3 translate-x-0.5 rounded-full bg-white transition group-data-[checked]:translate-x-[1.125rem]" />
                </Switch>
              </div>
              {/* switch to surface low-trust data points */}
              <div className="flex gap-2 items-center justify-between">
                <span className="uppercase text-[10px] tracking-wider text-light-400 md:max-w-[8rem] lg:text-right leading-tight">
                  include low-trust points
                </span>
                <Switch
                  checked={showLowTrust}
                  onChange={handleLowTrustSwitchChange}
                  className="group inline-flex h-4 w-8 items-center rounded-full bg-light-700 data-[checked]:bg-accent-600 data-[disabled]:cursor-not-allowed data-[disabled]:opacity-50 flex-shrink-0 transition-colors"
                >
                  <span className="size-3 translate-x-0.5 rounded-full bg-white transition group-data-[checked]:translate-x-[1.125rem]" />
                </Switch>
              </div>
              {/* source filter */}
              <div className="flex gap-3 items-center justify-between">
                <span className="text-xs text-light-400 uppercase">Source</span>
                <div className="w-52">
                  <Listbox
                    value={sourceFilters}
                    onChange={handleFilterChange}
                    multiple
                  >
                    <ListboxButton
                      className={twMerge(
                        "h-9 relative block w-full rounded-lg bg-light-900 py-1.5 pr-8 pl-4 text-left text-sm/6 text-white truncate",
                        "focus:outline-none data-[focus]:outline-2 data-[focus]:-outline-offset-2 data-[focus]:outline-white/25"
                      )}
                    >
                      {sourceFilters.length > 0
                        ? sourceFilters
                            .map(
                              (filter) =>
                                SOURCE_OPTIONS.find(
                                  (opt) => opt.value === filter
                                )?.label
                            )
                            .join(", ")
                        : "None selected"}
                      <MdKeyboardArrowDown
                        className="group pointer-events-none absolute top-2.5 right-2.5 size-4 fill-white/60"
                        aria-hidden="true"
                      />
                    </ListboxButton>
                    <ListboxOptions
                      anchor="bottom"
                      transition
                      className={twMerge(
                        "w-[var(--button-width)] rounded-xl border border-white/5 bg-white/5 backdrop-blur-lg p-1 [--anchor-gap:var(--spacing-1)] focus:outline-none",
                        "transition duration-100 ease-in data-[leave]:data-[closed]:opacity-0"
                      )}
                    >
                      {SOURCE_OPTIONS.map((option, id) => (
                        <ListboxOption
                          key={id}
                          value={option.value}
                          className="group flex cursor-default items-center gap-2 rounded-lg py-1.5 px-4 select-none data-[focus]:bg-white/10"
                        >
                          <MdCheck className="invisible size-4 fill-white group-data-[selected]:visible flex-shrink-0" />
                          <span className="flex-1">{option.label}</span>
                          {sourceCounts[option.value] != null && (
                            <span className="text-xs text-light-400">
                              {sourceCounts[option.value]}
                            </span>
                          )}
                        </ListboxOption>
                      ))}
                    </ListboxOptions>
                  </Listbox>
                </div>
              </div>
            </div>
          </div>
          {/* table */}
          <div
            ref={tableWrapperRef}
            className="mt-3 overflow-x-auto relative"
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
                <col className="w-[28%]" />
                <col className="w-[15%]" />
                <col className="w-[37%]" />
                <col className="w-[20%]" />
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
                      {header.filterable ? (
                        <Popover className="relative">
                          <PopoverButton className="group flex gap-1 items-center cursor-pointer focus:outline-none">
                            {(() => {
                              const visibleCls = CLASSIFICATION_OPTIONS.filter(
                                (cls) => (classificationCounts[cls] ?? 0) > 0
                              );
                              const isFiltered =
                                classificationFilter.length < visibleCls.length;
                              return (
                                <>
                                  <span
                                    className={`select-none uppercase text-xs font-medium transition duration-300 ease-in-out ${
                                      isFiltered
                                        ? "text-accent-600"
                                        : "text-light-400 group-hover:text-light-100"
                                    }`}
                                  >
                                    {header.label}
                                    {isFiltered &&
                                      ` (${classificationFilter.length})`}
                                  </span>
                                  <MdKeyboardArrowDown
                                    className={`transition duration-300 ease-in-out flex-shrink-0 ${
                                      isFiltered
                                        ? "text-accent-600"
                                        : "text-light-400 group-hover:text-light-100"
                                    }`}
                                  />
                                </>
                              );
                            })()}
                          </PopoverButton>
                          <PopoverPanel
                            anchor="bottom start"
                            className="w-56 rounded-xl border border-white/5 bg-neutral-900 backdrop-blur-lg p-1 z-50 shadow-lg"
                          >
                            {/* select all / deselect all */}
                            {(() => {
                              const visibleOpts =
                                CLASSIFICATION_OPTIONS.filter(
                                  (cls) =>
                                    (classificationCounts[cls] ?? 0) > 0
                                );
                              const allChecked =
                                classificationFilter.length >=
                                visibleOpts.length;
                              return (
                                <button
                                  type="button"
                                  className="w-full text-left text-xs text-light-400 hover:text-light-100 px-4 py-1.5"
                                  onClick={() => {
                                    setTablePaginations(
                                      "food-composition-table",
                                      1,
                                      20
                                    );
                                    setClassificationFilter(
                                      allChecked ? [] : [...visibleOpts]
                                    );
                                  }}
                                >
                                  {allChecked
                                    ? "Deselect all"
                                    : "Select all"}
                                </button>
                              );
                            })()}
                            <div className="border-b border-white/5 my-1" />
                            {CLASSIFICATION_OPTIONS.filter(
                              (cls) =>
                                (classificationCounts[cls] ?? 0) > 0
                            ).map((cls) => (
                              <label
                                key={cls}
                                className="flex cursor-pointer items-center gap-2 rounded-lg py-1.5 px-4 hover:bg-white/10 capitalize"
                              >
                                <input
                                  type="checkbox"
                                  className="size-4 rounded border-white/20 bg-transparent accent-accent-600"
                                  checked={classificationFilter.includes(
                                    cls
                                  )}
                                  onChange={() => {
                                    setTablePaginations(
                                      "food-composition-table",
                                      1,
                                      20
                                    );
                                    setClassificationFilter((prev) =>
                                      prev.includes(cls)
                                        ? prev.filter((c) => c !== cls)
                                        : [...prev, cls]
                                    );
                                  }}
                                />
                                <span className="flex-1 text-sm">
                                  {cls === "n/a"
                                    ? "Unclassified"
                                    : cls}
                                </span>
                                <span className="text-xs text-light-400">
                                  {classificationCounts[cls]}
                                </span>
                              </label>
                            ))}
                          </PopoverPanel>
                        </Popover>
                      ) : (
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
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="text-sm font-light">
                {isLoading ? (
                  // loading skeleton
                  Array.from({ length: 20 }, (_, index) => (
                    <tr key={index}>
                      <td
                        className="w-full py-1.5"
                        colSpan={TABLE_HEADERS.length}
                      >
                        <div className="h-9 flex items-center">
                          <LoadingCard className="h-5" />
                        </div>
                      </td>
                    </tr>
                  ))
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
                    return (
                    <tr
                      key={row.id}
                      ref={isHighlighted ? highlightRowRef : null}
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
                      {/* median concentration — bar + value + % of 100g by
                       * mass (mg/100g → divide by 1000). Mirrors nutrition. */}
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
                            const barPct =
                              pct === null
                                ? 0
                                : Math.max(2, Math.min(100, pct));
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
                                <span
                                  aria-hidden
                                  className="relative h-1.5 w-32 shrink-0 rounded-full bg-light-800/70 overflow-hidden"
                                >
                                  {pct !== null && (
                                    <span
                                      className="absolute inset-y-0 left-0 rounded-full bg-accent-600/80"
                                      style={{ width: `${barPct}%` }}
                                    />
                                  )}
                                </span>
                                <span className="font-mono text-xs text-light-200 whitespace-nowrap tabular-nums text-right min-w-[5rem]">
                                  {formatConcentrationValueAlt(v)}
                                </span>
                                <span className="font-mono text-xs text-light-500 whitespace-nowrap tabular-nums text-right min-w-[3.5rem]">
                                  {fmtPct}
                                </span>
                              </>
                            );
                          })()}
                        </div>
                      </td>
                      {/* evidence */}
                      <td className="py-1.5 pl-4">
                        <div className="flex min-h-9 capitalize items-center justify-end">
                          <Button
                            className="border-light-500 text-light-500 w-36"
                            variant="outlined"
                            size="sm"
                            onClick={(event) =>
                              handleEvidenceButtonClick(event, row.name)
                            }
                          >
                            <MdDescription className="size-4" />{" "}
                            {getRowEvidenceCount(row)} Data Point
                            {getRowEvidenceCount(row) === 1 ? "" : "s"}
                          </Button>
                        </div>
                      </td>
                    </tr>
                    );
                  })
                ) : (
                  // no rows
                  <tr>
                    <td colSpan={TABLE_HEADERS.length}>
                      <div className="h-[10rem] flex items-center justify-center text-light-300">
                        <>No associations found</>
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

export default FoodCompositionSection;
