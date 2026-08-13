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
  MdWarningAmber,
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
import {
  getChemicalBioactivities,
  getFoodInferredBioactivities,
} from "@/utils/fetching";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import type { BioactivityMeasurement } from "@/types";

// One row per (chemical in this food × bioactivity that chemical was
// measured against), served by /food/inferred-bioactivities. That endpoint
// LEFT JOINs the Hill-fit efficacy columns, so a single call backs the whole
// table: rows without a fittable curve come back with null efficacy and
// render an em-dash, and search / sort / filters / pagination all happen
// server-side. Critically that includes the Unit, Evidence and Source
// filters, which the shared sidebar applies to this table and the direct
// one alike — see FoodBioactivitiesTab.
export interface InferredRow {
  bioactivity: string;
  bioactivity_id: string;
  chemical: string;
  chemical_id: string;
  median_concentration: { value: number | null; unit: string } | null;
  // Hill curves behind the efficacy figure; the assay chip shows
  // `n_measurements_total`, which counts every measurement for the pair.
  n_curves: number;
  n_measurements_total: number | null;
  // "suspect_high" when the upstream pipeline measured the chemical at
  // >10% of the food by mass — implausible for most chemistry, and the
  // efficacy figure is derived from it, so the row is flagged rather
  // than silently trusted. 1,560 of 61,119 efficacy rows carry it.
  conc_quality_flag: string | null;
  efficacy: {
    efficacy_fraction: number | null;
    conc_vs_ac50: string | null;
  };
}

// Shape of a row as the API returns it, before we normalise it.
interface InferredApiRow {
  bioactivity?: string;
  bioactivity_id?: string;
  chemical?: string;
  chemical_id?: string;
  median_concentration?: { value: number | null; unit: string } | null;
  conc_quality_flag?: string | null;
  measurement_count?: number | null;
  n_curves?: number | null;
  efficacy_fraction?: number | null;
  conc_vs_ac50?: string | null;
}

const apiRowToInferredRow = (r: InferredApiRow): InferredRow => ({
  bioactivity: r.bioactivity ?? "",
  bioactivity_id: r.bioactivity_id ?? "",
  chemical: r.chemical ?? "",
  chemical_id: r.chemical_id ?? "",
  median_concentration: r.median_concentration ?? null,
  conc_quality_flag: r.conc_quality_flag ?? null,
  n_curves: r.n_curves ?? 0,
  // measurement_count comes from mv_chemical_bioactivity — the same source
  // the efficacy endpoint exposed as n_measurements_total.
  n_measurements_total: r.measurement_count ?? null,
  efficacy: {
    efficacy_fraction: r.efficacy_fraction ?? null,
    conc_vs_ac50: r.conc_vs_ac50 ?? null,
  },
});

// Sorting is server-side (see _INFERRED_SORT in the API's bioactivity
// repository). The nulls-last rule and the efficacy tie-break on
// dose_over_ac50_log both live in SQL now, so there is no client-side
// comparator to keep in sync.
//
// Sort keys the table exposes, mapped to what the API accepts.
const SORT_KEYS = {
  bioactivity: "bioactivity",
  chemical: "chemical",
  concentration: "concentration",
  efficacy: "efficacy",
  n_curves: "measurement_count",
} as const;

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

  // Server-driven: the endpoint applies search, filters, sort and paging,
  // so `rows` is exactly the page to render and `totalRows` comes from the
  // response metadata.
  const [rows, setRows] = useState<InferredRow[]>([]);
  const [totalRows, setTotalRows] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [isLoading, setIsLoading] = useState(true);
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

  // One request per (food, page, search, sort, filter) tuple. The three
  // filter props are forwarded so this table narrows in step with the
  // direct table above it — the shared sidebar drives both.
  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const payload = await getFoodInferredBioactivities(commonName, {
        page: currentPage,
        search: effectiveSearchTerm || undefined,
        sortBy: SORT_KEYS[sort.by as keyof typeof SORT_KEYS] ?? "concentration",
        sortDir: sort.dir,
        filterSourceKind: effectiveSourceKind || undefined,
        filterUnit: effectiveUnit || undefined,
        filterEvidenceType: effectiveEvidenceType || undefined,
      });
      if (cancelled) return;
      const data = (payload?.data as InferredApiRow[] | undefined) ?? [];
      const meta = payload?.metadata as
        | { total_rows?: number; total_pages?: number }
        | undefined;
      setRows(data.map(apiRowToInferredRow));
      setTotalRows(meta?.total_rows ?? 0);
      setTotalPages(Math.max(1, meta?.total_pages ?? 1));
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [
    commonName,
    currentPage,
    effectiveSearchTerm,
    sort,
    effectiveSourceKind,
    effectiveUnit,
    effectiveEvidenceType,
  ]);

  useEffect(() => {
    if (onTotalRowsChange && !isLoading) onTotalRowsChange(totalRows);
  }, [onTotalRowsChange, totalRows, isLoading]);

  // Snap back to page 1 when the row set shrinks below the current page.
  // The search box lives on the parent (FoodBioactivitiesTab) whenever
  // `hideChrome` is set, so the in-component handlers that reset the page
  // are unreachable — without this, filtering while on a later page slices
  // past the end and renders "no matches" over real results, with the
  // paginator unmounted (totalPages === 1) and no way back.
  //
  // Same shape as EvidenceTable / BioactivityMeasurementsModal, adapted to
  // PaginationsContext. `setTablePaginations` isn't referentially stable,
  // so this effect re-runs often — the guard is what keeps that safe.
  // The `!isLoading` guard matters: rows are empty on the first render, so
  // totalPages is 1 before the fetch resolves and an unguarded clamp would
  // discard a legitimately persisted page on every mount.
  useEffect(() => {
    if (!isLoading && currentPage > totalPages) {
      setTablePaginations(tableId, 1, 20);
    }
  }, [isLoading, currentPage, totalPages, tableId, setTablePaginations]);

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
                  anyPending={pendingKey !== null}
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
                    {row.conc_quality_flag === "suspect_high" && (
                      <MdWarningAmber
                        className="ml-1 inline size-3 text-amber-500"
                        aria-label="Concentration flagged as implausibly high"
                      />
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

      {/* openModal pre-loads a measurements sample via
       * getChemicalBioactivities (the efficacy endpoint doesn't carry raw
       * measurements), cached per chemical. That sample is only a
       * placeholder + fallback: the modal itself then fetches the
       * authoritative set from /bioactivity/measurements, which carries the
       * Hill-fit fields the sample lacks. The pre-fetch is therefore a
       * blocking round-trip we could drop — tracked as a follow-up, since
       * removing it also removes the fallback when that fetch fails. */}
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
const EfficacyCell = ({
  efficacy,
}: {
  efficacy: InferredRow["efficacy"];
}) => {
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
  anyPending,
  onOpen,
  rowReportProps,
}: {
  row: InferredRow;
  isPending: boolean;
  anyPending: boolean;
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
          {/* Outside the null branch on purpose. The flag comes from the
           * efficacy row (which has its own concentration) while the value
           * shown here comes from the composition row, and the two disagree
           * often — 468 of the flagged rows that reach this table have no
           * composition concentration at all. Nesting the warning inside
           * meant it stayed hidden on exactly those rows, which are the
           * ones where the user can least judge the number for themselves. */}
          {row.conc_quality_flag === "suspect_high" && (
            <MdWarningAmber
              className="ml-1 size-3 text-amber-500 flex-shrink-0"
              title="Upstream flagged the concentration behind this row as implausibly high (>10% of the food by mass). The efficacy figure is derived from it."
              aria-label="Concentration flagged as implausibly high"
            />
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
            // `anyPending`, not just this row's `isPending`: two overlapping
            // fetches would both resolve and the second would swap the open
            // modal's chemical out from under the user. Matches the mobile
            // card's guard.
            disabled={
              (row.n_measurements_total ?? row.n_curves) === 0 ||
              isPending ||
              anyPending
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
