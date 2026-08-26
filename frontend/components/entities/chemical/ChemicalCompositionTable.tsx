"use client";

import { useEffect, useMemo, useState } from "react";
import { MdInfo, MdInfoOutline } from "react-icons/md";

import Pagination from "@/components/basic/Pagination";
import ResetFiltersButton from "@/components/basic/ResetFiltersButton";
import ChemicalCompositionCards from "@/components/entities/chemical/ChemicalCompositionCards";
import FoodCompositionEvidenceModal from "@/components/entities/food/FoodCompositionEvidenceModal";
import { getChemicalCompositionEvidence } from "@/utils/fetching";
import type { FoodEvidence } from "@/types/Evidence";
import ChemicalCompositionTableRow, {
  COLUMN_COUNT,
} from "@/components/entities/chemical/ChemicalCompositionRow";
import {
  CompositionFilterPanel,
  CompositionMobileSort,
} from "@/components/entities/chemical/ChemicalCompositionToolbar";
import ChemicalCompositionFilters from "@/components/entities/chemical/ChemicalCompositionFilters";
import ChemicalCompositionHead from "@/components/entities/chemical/ChemicalCompositionHead";
import { usePaginations } from "@/context/paginationsContext";
import { useReportRows } from "@/context/reportModeContext";
import {
  COLUMNS,
  ChemicalCompositionRow as Row,
  SortColumn,
  SortDirection,
  SourceKey,
  SOURCES,
  computeMaxValue,
  evidenceCountOf,
  filterRows,
  mergeBuckets,
  paginate,
  sortRows,
  sourceCountOf,
} from "@/utils/chemicalComposition";
import { encodeSpace } from "@/utils/utils";

// Pagination writes rowsPerPage=20 into the context on every click, so the
// page size is not ours to choose — matching it here keeps the slice and
// the page counter in agreement.
const ROWS_PER_PAGE = 20;
const TABLE_ID = "chemical-composition-table";

interface ChemicalCompositionTableProps {
  withConcentrations: Row[] | null | undefined;
  withoutConcentrations: Row[] | null | undefined;
  // The chemical this table is about — the evidence modal names the pair,
  // and the lazy evidence fetch is keyed on it.
  commonName: string;
  // The chemical's foodatlas_id — carried into the ?highlight= deep link
  // and the report context, exactly as the old bar chart did.
  chemicalId?: string;
}

const ChemicalCompositionTable = ({
  withConcentrations,
  withoutConcentrations,
  commonName,
  chemicalId,
}: ChemicalCompositionTableProps) => {
  const reporter = useReportRows();
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(TABLE_ID);

  const [search, setSearch] = useState("");
  const [sources, setSources] = useState<SourceKey[]>([]);
  // Defaults ON: both buckets render on this page today, so starting with
  // the unmeasured foods hidden would silently drop rows the user can
  // currently see — and desync the table from the tab's badge count.
  const [includeUnmeasured, setIncludeUnmeasured] = useState(true);
  // Evidence modal. The row only names the food; the records themselves are
  // fetched when the modal opens — see getChemicalCompositionEvidence for
  // why they are not part of the table payload.
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);
  const [evidenceFood, setEvidenceFood] = useState("");
  const [evidences, setEvidences] = useState<FoodEvidence[] | undefined>(
    undefined
  );

  useEffect(() => {
    if (!evidenceFood) return;
    let cancelled = false;
    setEvidences(undefined);
    (async () => {
      const rows = await getChemicalCompositionEvidence(
        commonName,
        evidenceFood
      );
      if (!cancelled) setEvidences(rows);
    })();
    return () => {
      cancelled = true;
    };
  }, [evidenceFood, commonName]);
  const [sort, setSort] = useState<{
    column: SortColumn;
    direction: SortDirection;
  }>({ column: "median_concentration", direction: "desc" });

  // Scale denominator: the measured bucket in full, independent of search,
  // source filter, sort and page. A bar's length therefore means the same
  // thing everywhere in the table.
  const maxValue = useMemo(
    () => computeMaxValue(withConcentrations ?? []),
    [withConcentrations]
  );

  const allRows = useMemo(
    () =>
      mergeBuckets(withConcentrations, withoutConcentrations, includeUnmeasured),
    [withConcentrations, withoutConcentrations, includeUnmeasured]
  );

  const visibleRows = useMemo(
    () => sortRows(filterRows(allRows, { search, sources }), sort.column, sort.direction),
    [allRows, search, sources, sort]
  );

  const numberOfPages = Math.max(
    1,
    Math.ceil(visibleRows.length / ROWS_PER_PAGE)
  );

  // Narrowing the result set can strand the user past the last page.
  useEffect(() => {
    if (currentPage > numberOfPages) {
      setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
    }
  }, [currentPage, numberOfPages, setTablePaginations]);

  const pageRows = paginate(visibleRows, currentPage, ROWS_PER_PAGE);

  const unmeasuredCount = (withoutConcentrations ?? []).length;
  const sourceCounts = useMemo(() => {
    const base = mergeBuckets(
      withConcentrations,
      withoutConcentrations,
      includeUnmeasured
    );
    return SOURCES.map(({ key, label }) => ({
      key,
      label,
      count: base.filter((r) => sourceCountOf(r, key) > 0).length,
    }));
  }, [withConcentrations, withoutConcentrations, includeUnmeasured]);

  const hrefFor = (row: Row) => {
    const qs = chemicalId
      ? `?highlight=${encodeURIComponent(chemicalId)}#composition`
      : "";
    return `/food/${encodeURIComponent(encodeSpace(row.name))}${qs}`;
  };

  const rowContextFor = (row: Row) => ({
    kind: "food-composition-row" as const,
    entityType: "chemical" as const,
    entitySlug: chemicalId,
    chemicalName: chemicalId,
    foodId: row.id,
    foodName: row.name,
    dataPointCount: evidenceCountOf(row),
  });

  const handleSortClick = (column: SortColumn) => {
    setSort((prev) =>
      prev.column === column
        ? { column, direction: prev.direction === "asc" ? "desc" : "asc" }
        : { column, direction: column === "name" ? "asc" : "desc" }
    );
    setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
  };

  const toggleSource = (key: SourceKey) => {
    setSources((prev) =>
      prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]
    );
    setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
  };

  const handleSearchChange = (value: string) => {
    setSearch(value);
    setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
  };

  // Every dimension the sidebar can narrow by, back to first-render
  // defaults. `includeUnmeasured` starts ON, so clearing turns it back on
  // rather than off.
  const resetAllFilters = () => {
    setSearch("");
    setSources([]);
    setIncludeUnmeasured(true);
    setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
  };

  // Sort is deliberately excluded: it reorders rows but hides none, and
  // the empty-state copy below keys off "are rows being hidden".
  const isFiltered = search.trim() !== "" || sources.length > 0;
  const isFiltersDirty = isFiltered || !includeUnmeasured;

  // One instance, rendered into either the sidebar or the drawer, so the
  // reset control belongs here rather than at either call site.
  const filterPanel = (
    <>
      <CompositionFilterPanel
        sourceCounts={sourceCounts}
        selectedSources={sources}
        onToggleSource={toggleSource}
        unmeasuredCount={unmeasuredCount}
        includeUnmeasured={includeUnmeasured}
        onToggleUnmeasured={() => {
          setIncludeUnmeasured((v) => !v);
          setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
        }}
      />
      <ResetFiltersButton isDirty={isFiltersDirty} onReset={resetAllFilters} />
    </>
  );

  return (
    <div className="flex flex-col gap-4">
      <ChemicalCompositionFilters
        search={search}
        onSearchChange={handleSearchChange}
        filterPanel={filterPanel}
        mobileOpen={mobileFiltersOpen}
        onMobileOpenChange={setMobileFiltersOpen}
      />

      <CompositionMobileSort
        sort={sort}
        onSortChange={(next) => {
          setSort(next);
          setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
        }}
      />

      {/* desktop table */}
      <div className="hidden md:block overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            {COLUMNS.map((c) => (
              <col key={c.key} className={c.width} />
            ))}
          </colgroup>
          <ChemicalCompositionHead sort={sort} onSortClick={handleSortClick} />
          <tbody className="text-sm font-light">
            {pageRows.length > 0 ? (
              pageRows.map((row) => (
                <ChemicalCompositionTableRow
                  key={row.id}
                  row={row}
                  maxValue={maxValue}
                  href={hrefFor(row)}
                  onEvidenceClick={setEvidenceFood}
                  rowProps={reporter.getRowProps(rowContextFor(row))}
                />
              ))
            ) : (
              <tr>
                <td colSpan={COLUMN_COUNT}>
                  <div className="h-[10rem] flex items-center justify-center text-light-300 gap-2">
                    <MdInfoOutline />
                    {isFiltered
                      ? "No foods match these filters"
                      : "No foods with a known concentration"}
                  </div>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* mobile cards */}
      <div className="md:hidden">
        {pageRows.length > 0 ? (
          <ChemicalCompositionCards
            rows={pageRows}
            maxValue={maxValue}
            hrefFor={hrefFor}
            onEvidenceClick={setEvidenceFood}
            rowPropsFor={(row) => reporter.getRowProps(rowContextFor(row))}
          />
        ) : (
          <div className="h-24 flex items-center justify-center text-light-300 gap-2">
            <MdInfoOutline />
            {isFiltered
              ? "No foods match these filters"
              : "No foods with a known concentration"}
          </div>
        )}
      </div>

      {numberOfPages > 1 && (
        <Pagination
          tableId={TABLE_ID}
          numberOfPages={numberOfPages}
          isLoading={false}
        />
      )}

      <div className="flex items-center gap-1.5 text-sm text-light-400">
        <MdInfo className="flex-shrink-0" />
        Bar length is relative to the highest concentration shown. All
        concentrations are measured in mg / 100g.
      </div>

      {/* The food page's modal, reused unchanged: a composition data point
        * is the same (food, chemical) pair whichever page you reached it
        * from, so it should read identically on both. */}
      <FoodCompositionEvidenceModal
        foodName={evidenceFood}
        chemicalName={commonName}
        evidences={evidences}
        isOpen={evidenceFood !== ""}
        onClose={() => setEvidenceFood("")}
      />
    </div>
  );
};

ChemicalCompositionTable.displayName = "ChemicalCompositionTable";

export default ChemicalCompositionTable;
