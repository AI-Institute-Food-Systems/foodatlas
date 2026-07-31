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

import { useEffect, useState } from "react";
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
import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import {
  efficacyKey,
  formatDoseOverAc50Log,
  indexEfficacy,
} from "@/components/entities/bioactivity/efficacy";
import { useReportRows } from "@/context/reportModeContext";
import { usePaginations } from "@/context/paginationsContext";
import { useLoadingGate } from "@/context/pageReadyContext";
import {
  getFoodEfficacy,
  getFoodInferredBioactivities,
} from "@/utils/fetching";
import { encodeSpace, formatConcentrationValueAlt } from "@/utils/utils";
import type {
  BioactivityMeasurement,
  BioactivityTopMeasurement,
  FoodEfficacyRow,
} from "@/types";

interface InferredRow {
  bioactivity: string;
  bioactivity_id: string;
  chemical: string;
  chemical_id: string;
  median_concentration: { value: number | null; unit: string } | null;
  measurement_count: number;
  active_count: number;
  inactive_count: number;
  measurements: BioactivityMeasurement[];
  top_measurement: BioactivityTopMeasurement | null;
}

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

  const [rows, setRows] = useState<InferredRow[]>([]);
  const [totalPages, setTotalPages] = useState(0);
  const [totalRows, setTotalRows] = useState(0);
  const [isLoading, setIsLoading] = useState(true);
  // /food/efficacy is per-food (not per-page), so we fetch it once per
  // commonName and index by (chemical_foodatlas_id, bioactivity_foodatlas_id)
  // for O(1) join onto inferred rows in the render pass. Rows without a
  // matching efficacy entry render "—" in the Efficacy column.
  const [efficacyIndex, setEfficacyIndex] = useState<
    Map<string, FoodEfficacyRow>
  >(new Map());
  useLoadingGate(isLoading);
  useEffect(() => {
    if (onTotalRowsChange && !isLoading) onTotalRowsChange(totalRows);
  }, [onTotalRowsChange, totalRows, isLoading]);
  const [selected, setSelected] = useState<InferredRow | null>(null);
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

  // Efficacy fetch — only re-runs when commonName changes (efficacy
  // rows aren't filtered by search/sort/pagination on the backend).
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const payload = await getFoodEfficacy(commonName);
      if (cancelled) return;
      setEfficacyIndex(
        indexEfficacy((payload?.data as FoodEfficacyRow[] | undefined) ?? [])
      );
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName]);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const payload = await getFoodInferredBioactivities(commonName, {
        page: currentPage,
        search: effectiveSearchTerm,
        sortBy: sort.by,
        sortDir: sort.dir,
        filterSourceKind: effectiveSourceKind || undefined,
        filterUnit: effectiveUnit || undefined,
        filterEvidenceType: effectiveEvidenceType || undefined,
      });
      if (cancelled) return;
      setRows((payload?.data as InferredRow[] | undefined) ?? []);
      setTotalPages(payload?.metadata?.total_pages ?? 0);
      setTotalRows(payload?.metadata?.total_rows ?? 0);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [
    commonName,
    currentPage,
    effectiveSearchTerm,
    effectiveSourceKind,
    effectiveUnit,
    effectiveEvidenceType,
    sort,
  ]);

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
              { value: "measurement_count|desc", label: "Most assays" },
              { value: "measurement_count|asc", label: "Least assays" },
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
              <th className="h-9 border-b border-light-700 leading-none py-1.5 px-4 text-right">
                <span
                  className="select-none uppercase text-xs font-medium text-light-400"
                  title="Food's concentration of the chemical relative to the chemical's AC50 for this bioactivity. Above = dose clears the active threshold."
                >
                  Efficacy
                </span>
              </th>
              <SortableTh
                label="Assays"
                sortKey="measurement_count"
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
                  efficacy={
                    efficacyIndex.get(
                      efficacyKey(row.chemical_id, row.bioactivity_id)
                    ) ?? null
                  }
                  onOpen={() => setSelected(row)}
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
            const efficacy =
              efficacyIndex.get(
                efficacyKey(row.chemical_id, row.bioactivity_id)
              ) ?? null;
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
                    <EfficacyCell efficacy={efficacy} />
                  </span>
                </div>
                <div className="w-full flex justify-end">
                  <Chip
                    icon={<MdDescription className="size-3" />}
                    label={`${row.measurement_count.toLocaleString()} assay${
                      row.measurement_count === 1 ? "" : "s"
                    }`}
                    tone="outline"
                    size="md"
                    onClick={() => setSelected(row)}
                    disabled={row.measurement_count === 0}
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

      <BioactivityMeasurementsModal
        isOpen={selected !== null}
        onClose={() => setSelected(null)}
        headLabel={selected?.chemical ?? ""}
        tailLabel={selected?.bioactivity ?? ""}
        initialMeasurements={selected?.measurements ?? []}
        expectedCount={selected?.measurement_count}
        anchorId={selected?.chemical_id ?? null}
        selectedId={selected?.bioactivity_id ?? null}
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
}: {
  label: string;
  sortKey: string;
  sort: { by: string; dir: SortDir };
  onClick: (k: string) => void;
  align: "left" | "right";
  first?: boolean;
}) => {
  const active = sort.by === sortKey;
  return (
    <th
      className={`h-9 border-b border-light-700 leading-none py-1.5 ${
        first ? "pr-4" : "px-4"
      } ${align === "right" ? "text-right" : "text-left"}`}
    >
      <button
        type="button"
        onClick={() => onClick(sortKey)}
        className={`group flex items-center gap-1 cursor-pointer focus:outline-none ${
          align === "right" ? "justify-end ml-auto" : ""
        }`}
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
    </th>
  );
};

// Rendered in the desktop table's Efficacy column and the mobile card's
// Efficacy label row. Reads dose_over_ac50_log + conc_vs_ac50 from the
// joined efficacy row; renders "—" when no efficacy row matched
// (chemical/bioactivity not in the /food/efficacy response, or values
// null).
const EfficacyCell = ({ efficacy }: { efficacy: FoodEfficacyRow | null }) => {
  if (!efficacy || efficacy.dose_over_ac50_log == null) {
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
        {formatDoseOverAc50Log(efficacy.dose_over_ac50_log)}
      </span>
    </span>
  );
};

const Row = ({
  row,
  efficacy,
  onOpen,
  rowReportProps,
}: {
  row: InferredRow;
  efficacy: FoodEfficacyRow | null;
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
          <EfficacyCell efficacy={efficacy} />
        </div>
      </td>
      <td className="py-1.5 pl-4 text-right">
        <div className="flex min-h-9 items-center justify-end">
          <Chip
            icon={<MdDescription className="size-3" />}
            label={`${row.measurement_count.toLocaleString()} assay${
              row.measurement_count === 1 ? "" : "s"
            }`}
            tone="outline"
            size="md"
            onClick={onOpen}
            disabled={row.measurement_count === 0}
          />
        </div>
      </td>
    </tr>
  );
};

FoodInferredBioactivitiesSection.displayName =
  "FoodInferredBioactivitiesSection";
export default FoodInferredBioactivitiesSection;
