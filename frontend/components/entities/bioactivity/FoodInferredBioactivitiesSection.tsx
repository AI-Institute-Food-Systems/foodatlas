// Foods don't directly measure bioactivity in the lab; they contain
// chemicals that do. This section surfaces the transitive inference:
// one row per (chemical-in-food, bioactivity-of-chemical) pair, with
// the food-level concentration of that chemical alongside the chemical's
// measurement counts + top measurement against the bioactivity.
//
// Sits below FoodBioactivitiesSection on the food page's Bioactivities
// tab. "View assays" opens the same measurements modal as the direct
// table, anchored on the row's chemical (not the food).

"use client";

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  MdClose,
  MdDescription,
  MdInfoOutline,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdSearch,
  MdUnfoldMore,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import Chip from "@/components/basic/Chip";
import Link from "@/components/basic/Link";
import LoadingCard from "@/components/basic/LoadingCard";
import Pagination from "@/components/basic/Pagination";
import SortListbox from "@/components/basic/SortListbox";
import { Tooltip } from "@/components/basic/Tooltip";
import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import { formatEfficacyFraction } from "@/components/entities/bioactivity/efficacy";
import { useReportRows } from "@/context/reportModeContext";
import { usePaginations } from "@/context/paginationsContext";
import { useLoadingGate } from "@/context/pageReadyContext";
import {
  getChemicalBioactivities,
  getFoodEfficacy,
} from "@/utils/fetching";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import type { BioactivityMeasurement, FoodEfficacyRow } from "@/types";

// The inferred table row shape now derives from /food/efficacy (the
// former /food/inferred-bioactivities endpoint was removed on the
// ptfi-bioactivity-staging refresh). Each row is one (chemical ×
// bioactivity) evaluated against a Hill fit — the efficacy math lives
// on `efficacy`; `n_curves` is the count of qualifying Hill curves
// (the closest analog to the old `measurement_count` for the "N curves"
// column + the View-measurements modal button).
interface InferredRow {
  bioactivity: string;
  bioactivity_id: string;
  chemical: string;
  chemical_id: string;
  median_concentration: { value: number | null; unit: string } | null;
  n_curves: number;
  // Total assays backing the pair (from mv_chemical_bioactivity). Null on
  // older API deployments — chip label falls back to "View assays" in that
  // case. Once every environment ships the backend change, this can be
  // required.
  n_measurements_total: number | null;
  efficacy: FoodEfficacyRow;
}

const PAGE_SIZE = 20;

const efficacyToInferredRow = (e: FoodEfficacyRow): InferredRow => ({
  bioactivity: e.bioactivity_name,
  bioactivity_id: e.bioactivity_foodatlas_id,
  chemical: e.chemical_name,
  chemical_id: e.chemical_foodatlas_id,
  median_concentration:
    e.food_conc_mg_per_100g != null
      ? { value: e.food_conc_mg_per_100g, unit: "mg/100g" }
      : null,
  n_curves: e.n_curves ?? 0,
  n_measurements_total: e.n_measurements_total ?? null,
  efficacy: e,
});

const compare = (a: number | null, b: number | null): number => {
  if (a == null && b == null) return 0;
  if (a == null) return 1; // nulls last
  if (b == null) return -1;
  return a - b;
};

const sortInferred = (
  rows: InferredRow[],
  by: string,
  dir: "asc" | "desc"
): InferredRow[] => {
  const mult = dir === "asc" ? 1 : -1;
  const copy = [...rows];
  copy.sort((r1, r2) => {
    switch (by) {
      case "bioactivity":
        return mult * r1.bioactivity.localeCompare(r2.bioactivity);
      case "chemical":
        return mult * r1.chemical.localeCompare(r2.chemical);
      case "concentration":
        return (
          mult *
          compare(
            r1.median_concentration?.value ?? null,
            r2.median_concentration?.value ?? null
          )
        );
      case "efficacy":
        return (
          mult *
          compare(
            r1.efficacy.efficacy_fraction,
            r2.efficacy.efficacy_fraction
          )
        );
      case "n_curves":
        // Sort by the total shown on the chip when available, falling back
        // to the contributed count on older API deployments.
        return (
          mult *
          compare(
            r1.n_measurements_total ?? r1.n_curves,
            r2.n_measurements_total ?? r2.n_curves
          )
        );
      default:
        return 0;
    }
  });
  return copy;
};

type SortDir = "asc" | "desc";

interface Props {
  commonName: string;
  // Set when a parent (e.g. FoodBioactivitiesTab) hosts the shared
  // search/filter chrome and drives BOTH the direct + inferred tables
  // from one sidebar. `hideChrome` suppresses this section's own
  // aside + search input row, `externalSearch` / `externalSourceKind`
  // override the internal state that would otherwise be uncontrolled.
  externalSearch?: string;
  externalSourceKind?: string;
  externalUnit?: string;
  externalEvidenceType?: string;
  hideChrome?: boolean;
  // Fires whenever the filtered totalRows changes so a wrapper (the
  // Food Bioactivities tab) can sum direct + inferred for its tab badge.
  onTotalRowsChange?: (total: number) => void;
  // When externally driven, this callback lets the table's empty-state
  // "clear filters" button reset the parent's sidebar too.
  onResetFilters?: () => void;
}

const TABLE_ID_PREFIX = "food-inferred-bioact";

const FoodInferredBioactivitiesSection = ({
  commonName,
  externalSearch,
  externalSourceKind,
  externalUnit,
  externalEvidenceType,
  hideChrome = false,
  onTotalRowsChange,
  onResetFilters,
}: Props) => {
  const tableId = `${TABLE_ID_PREFIX}-${commonName}`;
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);

  const [searchTerm, setSearchTerm] = useState("");
  const effectiveSearchTerm =
    externalSearch !== undefined ? externalSearch : searchTerm;
  const effectiveSourceKind = externalSourceKind ?? "";
  const effectiveUnit = externalUnit ?? "";
  const effectiveEvidenceType = externalEvidenceType ?? "";
  const [sort, setSort] = useState<{ by: string; dir: SortDir }>({
    by: "concentration",
    dir: "desc",
  });

  // /food/efficacy returns every (chemical × bioactivity) row for the
  // food in a single response (typical: 0–200 rows), so all filtering,
  // sorting, and pagination is done client-side against `allRows`.
  const [allRows, setAllRows] = useState<InferredRow[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  useLoadingGate(isLoading);
  // Modal state. The efficacy endpoint doesn't carry raw measurements,
  // so when a user clicks "View N curves" we lazy-fetch the chemical's
  // full bioactivity list (which does carry the per-bioactivity
  // measurement sample) and pull out the entry matching this row's
  // bioactivity_id. Cached per chemical for cheap re-opens.
  const [selected, setSelected] = useState<{
    row: InferredRow;
    measurements: BioactivityMeasurement[];
  } | null>(null);
  const [pendingKey, setPendingKey] = useState<string | null>(null);
  const chemMeasurementsCache = useRef<
    Map<string, Map<string, BioactivityMeasurement[]>>
  >(new Map());

  const openModal = async (row: InferredRow) => {
    const rowKey = `${row.chemical_id}::${row.bioactivity_id}`;
    let byBio = chemMeasurementsCache.current.get(row.chemical);
    if (byBio?.has(row.bioactivity_id)) {
      setSelected({ row, measurements: byBio.get(row.bioactivity_id) ?? [] });
      return;
    }
    setPendingKey(rowKey);
    const payload = await getChemicalBioactivities(row.chemical);
    // Bucket every bio row's measurements into the cache so subsequent
    // clicks on other bioactivities for the same chemical are instant.
    byBio = new Map();
    for (const bio of (payload?.data ?? []) as Array<{
      id: string;
      measurements?: BioactivityMeasurement[];
    }>) {
      byBio.set(bio.id, bio.measurements ?? []);
    }
    chemMeasurementsCache.current.set(row.chemical, byBio);
    setSelected({ row, measurements: byBio.get(row.bioactivity_id) ?? [] });
    setPendingKey(null);
  };
  const reporter = useReportRows();

  const buildRowContext = (row: InferredRow) => ({
    kind: "food-inferred-bioactivity" as const,
    entityType: "food" as const,
    entitySlug: commonName,
    bioactivityId: row.bioactivity_id,
    bioactivityName: row.bioactivity,
    chemicalId: row.chemical_id,
    chemicalName: row.chemical,
    concentration:
      row.median_concentration?.value != null
        ? `${row.median_concentration.value} ${row.median_concentration.unit ?? ""}`.trim()
        : undefined,
  });

  // Single fetch per commonName — the efficacy endpoint returns
  // everything for the food in one shot. External filter props
  // (effectiveSourceKind/Unit/EvidenceType) don't apply to efficacy
  // rows (those concepts live on raw bioactivity measurements, not on
  // Hill-fit efficacy) so they're intentionally ignored here.
  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const payload = await getFoodEfficacy(commonName);
      if (cancelled) return;
      const eff = (payload?.data as FoodEfficacyRow[] | undefined) ?? [];
      // UNCLASSIFIED rows (bioactivity_id_raw === "UNCLASSIFIED") have no
      // canonical bioactivity target, so /chemical/bioactivities has no
      // bucket for them and the assays modal can't drill in. Per Pranav
      // 2026-08-04: drop them until upstream labels the underlying targets.
      const classified = eff.filter(
        (r) => r.bioactivity_id_raw !== "UNCLASSIFIED"
      );
      setAllRows(classified.map(efficacyToInferredRow));
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName]);

  // Client-side filter + sort + paginate.
  const filteredRows = useMemo(() => {
    if (!effectiveSearchTerm) return allRows;
    const q = effectiveSearchTerm.trim().toLowerCase();
    if (!q) return allRows;
    return allRows.filter(
      (r) =>
        r.bioactivity.toLowerCase().includes(q) ||
        r.chemical.toLowerCase().includes(q)
    );
  }, [allRows, effectiveSearchTerm]);
  const sortedRows = useMemo(
    () => sortInferred(filteredRows, sort.by, sort.dir),
    [filteredRows, sort]
  );
  const totalRows = sortedRows.length;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const rows = useMemo(
    () =>
      sortedRows.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE),
    [sortedRows, currentPage]
  );

  useEffect(() => {
    if (onTotalRowsChange && !isLoading) onTotalRowsChange(totalRows);
  }, [onTotalRowsChange, totalRows, isLoading]);

  const handleSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchTerm(e.target.value.toLowerCase());
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

  const showingPaginator = totalPages > 1 || isLoading;
  const showEmpty = !isLoading && rows.length === 0;

  // Effective-filter dirtiness — accounts for both internal search
  // state and parent-driven overrides so the empty-state copy can
  // distinguish "no data at all" from "your filters filtered it to 0".
  const hasActiveFilters =
    effectiveSearchTerm !== "" ||
    effectiveSourceKind !== "" ||
    effectiveUnit !== "" ||
    effectiveEvidenceType !== "";
  const resetForEmptyState =
    onResetFilters ??
    (() => {
      setSearchTerm("");
      setTablePaginations(tableId, 1, 20);
    });
  const emptyStateBody = hasActiveFilters ? (
    <div className="flex flex-col items-center gap-2 text-light-300">
      <div className="flex items-center gap-2 text-sm">
        <MdInfoOutline />
        No inferred bioactivities match your filters
      </div>
      <button
        type="button"
        onClick={resetForEmptyState}
        className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
      >
        clear filters
      </button>
    </div>
  ) : (
    <div className="flex items-center gap-2 text-light-300 text-sm">
      <MdInfoOutline /> No inferred bioactivities recorded for this food yet
    </div>
  );

  const searchInput = (
    <div className="relative flex items-center">
      <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
      <input
        className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none"
        type="text"
        placeholder="Search…"
        aria-label="Search bioactivity or chemical"
        value={searchTerm}
        onChange={handleSearchChange}
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

  return (
    <div className="relative flex flex-col gap-7">
      {/* Desktop sidebar + sub-1440 search input — hidden when a
       * parent (FoodBioactivitiesTab) hosts the shared chrome. */}
      {!hideChrome && (
        <aside className="hidden min-[1440px]:block absolute right-full mr-10 -top-[17px] bottom-0 w-48">
          <div className="sticky top-4">
            <Card>{searchInput}</Card>
          </div>
        </aside>
      )}

      {/* Heading + provenance disclaimer — same chip vocabulary as the
       * card-catalog sections. The italic line frames the data as
       * inferred, not directly observed in the food. */}
      <div className="flex flex-col gap-2">
        <span className="self-start bg-light-200 shadow-inner shadow-light-50 rounded-r-md px-2.5 py-0.5 font-mono italic font-medium text-light-900 text-[10px] tracking-[0.12em] uppercase -ml-3">
          Inferred via composition
        </span>
        <p className="font-serif italic text-light-400 text-sm">
          Bioactivities of chemicals found in {commonName}. The chemical
          was measured against the activity directly — {commonName} itself
          was not the test material. Concentration is the food-level
          median of that chemical.
        </p>
      </div>

      {!hideChrome && (
        <div className="min-[1440px]:hidden w-full max-w-xs">
          {searchInput}
        </div>
      )}

      {/* Row-count caption dropped — the tab badge is the canonical
       * total via the wrapper's onTotalRowsChange. Mobile sort listbox
       * stays here (no clickable column headers on card view). */}
      {!isLoading && totalRows > 0 && (
        <div className="mb-1.5 md:hidden flex justify-end items-center gap-2">
          <span className="font-mono italic text-[11px] text-light-500">
            sort
          </span>
          <SortListbox
            value={`${sort.by}|${sort.dir}`}
            options={[
              { value: "bioactivity|asc", label: "Bioactivity A–Z" },
              { value: "bioactivity|desc", label: "Bioactivity Z–A" },
              { value: "chemical|asc", label: "Chemical A–Z" },
              { value: "chemical|desc", label: "Chemical Z–A" },
              { value: "concentration|desc", label: "Highest concentration" },
              { value: "concentration|asc", label: "Lowest concentration" },
              { value: "efficacy|desc", label: "Highest efficacy" },
              { value: "efficacy|asc", label: "Lowest efficacy" },
              { value: "n_curves|desc", label: "Most assays" },
              { value: "n_curves|asc", label: "Fewest assays" },
            ]}
            onChange={(value) => {
              const [by, dir] = value.split("|");
              setSort({ by, dir: dir as SortDir });
              setTablePaginations(tableId, 1, 20);
            }}
          />
        </div>
      )}
      <div className="hidden md:block overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            <col className="w-[24%]" />
            <col className="w-[20%]" />
            <col className="w-[18%]" />
            <col className="w-[22%]" />
            <col className="w-[16%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <SortableTh
                label="Bioactivity"
                sortKey="bioactivity"
                sort={sort}
                onClick={handleSortClick}
                align="left"
                first
              />
              <SortableTh
                label="Via chemical"
                sortKey="chemical"
                sort={sort}
                onClick={handleSortClick}
                align="left"
              />
              <SortableTh
                label="Concentration"
                sortKey="concentration"
                sort={sort}
                onClick={handleSortClick}
                align="right"
              />
              <SortableTh
                label="Efficacy"
                sortKey="efficacy"
                sort={sort}
                onClick={handleSortClick}
                align="right"
                help={
                  <div className="whitespace-normal w-[28rem] max-w-[calc(100vw-3rem)]">
                    <p className="mb-2 text-light-400">
                      <span className="font-medium text-amber-300">
                        Caveat
                      </span>
                      &nbsp;— Bioactivity values are based on in vitro data
                      only and total food content only; they don&apos;t
                      account for individual genetics, the microbiome, or
                      the exposome. Work on bioaccessibility and
                      bioavailability is ongoing.
                    </p>
                    <p className="mb-2">
                      <span className="font-medium text-light-100">
                        Above / Below
                      </span>
                      &nbsp;— is the food&apos;s dose above the AC50 (the
                      concentration at 50% of the chemical&apos;s maximal
                      response)?
                    </p>
                    <p>
                      <span className="font-medium text-light-100">%</span>
                      &nbsp;— fraction of that maximal response reached at
                      the food&apos;s dose, read off the chemical&apos;s
                      Hill curve for this bioactivity. Higher = stronger
                      inferred effect.
                    </p>
                  </div>
                }
              />
              <SortableTh
                label="Assays"
                sortKey="n_curves"
                sort={sort}
                onClick={handleSortClick}
                align="right"
              />
            </tr>
          </thead>
          <tbody className="text-sm font-light">
            {isLoading ? (
              Array.from({ length: 20 }).map((_, i) => (
                <tr key={`l-${i}`}>
                  <td className="w-full py-1.5" colSpan={5}>
                    <div className="h-9 flex items-center">
                      <LoadingCard className="h-5" />
                    </div>
                  </td>
                </tr>
              ))
            ) : showEmpty ? (
              <tr>
                <td colSpan={5}>
                  <div className="h-[10rem] flex items-center justify-center">
                    {emptyStateBody}
                  </div>
                </td>
              </tr>
            ) : (
              rows.map((row, idx) => (
                <Row
                  key={`${row.chemical_id}-${row.bioactivity_id}-${idx}`}
                  row={row}
                  isPending={
                    pendingKey ===
                    `${row.chemical_id}::${row.bioactivity_id}`
                  }
                  onOpen={() => openModal(row)}
                  rowReportProps={reporter.getRowProps(buildRowContext(row))}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Card list — mobile. Primary line pairs Bioactivity → Chemical
       * (the inference chain). Concentration + Assays + Top + View
       * button sit below as label:value rows. */}
      <div className="md:hidden w-full flex flex-col divide-y divide-light-800">
        {isLoading ? (
          Array.from({ length: 8 }).map((_, i) => (
            <div key={`l-${i}`} className="w-full py-3">
              <LoadingCard className="h-5" />
            </div>
          ))
        ) : showEmpty ? (
          <div className="w-full py-6 flex items-center justify-center">
            {emptyStateBody}
          </div>
        ) : (
          rows.map((row, idx) => {
            const conc = row.median_concentration;
            const rowReportProps = reporter.getRowProps(buildRowContext(row));
            return (
              <div
                key={`${row.chemical_id}-${row.bioactivity_id}-${idx}`}
                {...rowReportProps}
                className={twMerge(
                  "w-full py-3 flex flex-col gap-2 text-sm",
                  rowReportProps.className,
                )}
              >
                <div className="w-full flex items-baseline gap-2 flex-wrap capitalize">
                  <Link
                    href={`/bioactivity/${encodeURIComponent(
                      encodeSpace(row.bioactivity)
                    )}`}
                    isExternal={false}
                  >
                    {row.bioactivity}
                  </Link>
                  <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                    via
                  </span>
                  <Link
                    href={`/chemical/${encodeURIComponent(
                      encodeSpace(row.chemical)
                    )}`}
                    isExternal={false}
                  >
                    {row.chemical}
                  </Link>
                </div>
                <div className="w-full flex items-baseline justify-between gap-2">
                  <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                    Concentration
                  </span>
                  <span className="font-mono tabular-nums text-light-200 text-right">
                    {conc?.value == null ? (
                      <span className="text-light-600">—</span>
                    ) : (
                      <>
                        {formatConcentrationValueAlt(conc.value)}
                        <span className="ml-1 text-light-500">
                          {conc.unit ?? ""}
                        </span>
                      </>
                    )}
                  </span>
                </div>
                <div className="w-full flex items-baseline justify-between gap-2">
                  <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                    Efficacy
                  </span>
                  <span className="text-right">
                    <EfficacyCell efficacy={row.efficacy} />
                  </span>
                </div>
                <div className="w-full flex justify-end">
                  <Chip
                    icon={<MdDescription className="size-3" />}
                    label={
                      pendingKey ===
                      `${row.chemical_id}::${row.bioactivity_id}`
                        ? "Loading…"
                        : row.n_measurements_total != null
                          ? `${row.n_measurements_total.toLocaleString()} assay${
                              row.n_measurements_total === 1 ? "" : "s"
                            }`
                          : "View assays"
                    }
                    tone="outline"
                    size="md"
                    onClick={() => openModal(row)}
                    disabled={
                      (row.n_measurements_total ?? row.n_curves) === 0 ||
                      pendingKey !== null
                    }
                  />
                </div>
              </div>
            );
          })
        )}
      </div>

      {showingPaginator && (
        <div className="mt-2 max-w-xl w-full mx-auto">
          <Pagination
            tableId={tableId}
            numberOfPages={totalPages}
            isLoading={isLoading}
          />
        </div>
      )}

      {/* Measurements sample is loaded via openModal → getChemicalBioactivities
       * (the efficacy endpoint doesn't carry the raw sample, and the
       * /bioactivity/measurements route was removed on the staging refresh).
       * Cached per chemical so subsequent row opens are instant. */}
      <BioactivityMeasurementsModal
        isOpen={selected !== null}
        onClose={() => setSelected(null)}
        headLabel={selected?.row.chemical ?? ""}
        tailLabel={selected?.row.bioactivity ?? ""}
        initialMeasurements={selected?.measurements ?? []}
        contributedCount={selected?.row.n_curves}
        anchorId={selected?.row.chemical_id ?? null}
        selectedId={selected?.row.bioactivity_id ?? null}
        relationship="r6"
        headIsRow={false}
      />
    </div>
  );
};

const SortableTh = ({
  label,
  sortKey,
  sort,
  onClick,
  align,
  first,
  help,
}: {
  label: string;
  sortKey: string;
  sort: { by: string; dir: SortDir };
  onClick: (k: string) => void;
  align: "left" | "right";
  first?: boolean;
  help?: ReactNode;
}) => {
  const active = sort.by === sortKey;
  return (
    <th
      className={`h-9 border-b border-light-700 leading-none py-1.5 ${
        first ? "pr-4" : "px-4"
      } ${align === "right" ? "text-right" : "text-left"}`}
    >
      <div className={`inline-flex items-center gap-1 ${align === "right" ? "ml-auto" : ""}`}>
        <button
          type="button"
          onClick={() => onClick(sortKey)}
          className="group flex items-center gap-1 cursor-pointer focus:outline-none"
        >
          <span
            className={`select-none uppercase text-xs font-medium transition duration-300 ease-in-out ${
              active ? "text-light-100" : "text-light-400 group-hover:text-light-100"
            }`}
          >
            {label}
          </span>
          {active ? (
            sort.dir === "asc" ? (
              <MdKeyboardArrowUp className="text-accent-600 group-hover:text-accent-300 flex-shrink-0" />
            ) : (
              <MdKeyboardArrowDown className="text-accent-600 group-hover:text-accent-300 flex-shrink-0" />
            )
          ) : (
            <MdUnfoldMore className="text-light-400 group-hover:text-light-100 flex-shrink-0" />
          )}
        </button>
        {help && (
          <Tooltip content={help}>
            <MdInfoOutline
              className="w-3.5 h-3.5 text-light-500 hover:text-light-100 transition-colors"
              aria-label={`About the ${label} column`}
            />
          </Tooltip>
        )}
      </div>
    </th>
  );
};

// Rendered in the desktop table's Efficacy column and the mobile card's
// Efficacy label row. Shows the fraction of maximal response (0–100%)
// at the food's in-food concentration — the primary metric per the
// food_chemical_efficacy.csv dictionary. `conc_vs_ac50` chip is the
// categorical above/below indicator (fraction > 0.5 ⇔ "above"). Renders
// "—" when the row's efficacy_fraction is null.
const EfficacyCell = ({ efficacy }: { efficacy: FoodEfficacyRow }) => {
  if (efficacy.efficacy_fraction == null) {
    return <span className="text-light-600">—</span>;
  }
  const above = efficacy.conc_vs_ac50 === "above";
  return (
    <span className="inline-flex items-baseline gap-2">
      <span
        className={twMerge(
          "font-mono italic uppercase tracking-wider text-[10px] px-1.5 py-[1px] rounded-full border",
          above
            ? "text-emerald-300 border-emerald-500/70 bg-emerald-500/10"
            : "text-light-400 border-light-700 bg-light-800/40"
        )}
      >
        {efficacy.conc_vs_ac50 ?? "—"}
      </span>
      <span className="font-mono tabular-nums text-xs text-light-300">
        {formatEfficacyFraction(efficacy.efficacy_fraction)}
      </span>
    </span>
  );
};

const Row = ({
  row,
  isPending,
  onOpen,
  rowReportProps,
}: {
  row: InferredRow;
  isPending: boolean;
  onOpen: () => void;
  rowReportProps?: Record<string, unknown>;
}) => {
  const conc = row.median_concentration;
  return (
    <tr {...rowReportProps}>
      <td className="py-1.5 pr-4">
        <div className="flex min-h-9 items-center capitalize">
          <Link
            href={`/bioactivity/${encodeURIComponent(
              encodeSpace(row.bioactivity)
            )}`}
            isExternal={false}
          >
            {row.bioactivity}
          </Link>
        </div>
      </td>
      <td className="py-1.5 px-4">
        <div className="flex min-h-9 items-center capitalize">
          <Link
            href={`/chemical/${encodeURIComponent(encodeSpace(row.chemical))}`}
            isExternal={false}
          >
            {row.chemical}
          </Link>
        </div>
      </td>
      <td className="py-1.5 px-4 text-right">
        <div className="flex min-h-9 items-center justify-end font-mono text-xs text-light-200 tabular-nums">
          {conc?.value == null ? (
            <span className="text-light-600">—</span>
          ) : (
            <>
              {formatConcentrationValueAlt(conc.value)}
              <span className="ml-1 text-light-500">{conc.unit ?? ""}</span>
            </>
          )}
        </div>
      </td>
      <td className="py-1.5 px-4 text-right">
        <div className="flex min-h-9 items-center justify-end font-mono text-xs text-light-200">
          <EfficacyCell efficacy={row.efficacy} />
        </div>
      </td>
      <td className="py-1.5 pl-4 text-right">
        <div className="flex min-h-9 items-center justify-end">
          <Chip
            icon={<MdDescription className="size-3" />}
            label={
              isPending
                ? "Loading…"
                : row.n_measurements_total != null
                  ? `${row.n_measurements_total.toLocaleString()} assay${
                      row.n_measurements_total === 1 ? "" : "s"
                    }`
                  : "View assays"
            }
            tone="outline"
            size="md"
            onClick={onOpen}
            disabled={
              (row.n_measurements_total ?? row.n_curves) === 0 || isPending
            }
          />
        </div>
      </td>
    </tr>
  );
};

FoodInferredBioactivitiesSection.displayName =
  "FoodInferredBioactivitiesSection";
export default FoodInferredBioactivitiesSection;
