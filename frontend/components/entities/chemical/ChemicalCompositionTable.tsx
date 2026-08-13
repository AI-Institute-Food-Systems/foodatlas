"use client";

import { useEffect, useMemo, useState } from "react";
import {
  MdInfo,
  MdInfoOutline,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdSearch,
  MdUnfoldMore,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Chip from "@/components/basic/Chip";
import Pagination from "@/components/basic/Pagination";
import SortListbox from "@/components/basic/SortListbox";
import { cellPadding } from "@/components/basic/skeletonTokens";
import ChemicalCompositionCards from "@/components/entities/chemical/ChemicalCompositionCards";
import ChemicalCompositionTableRow, {
  COLUMN_COUNT,
} from "@/components/entities/chemical/ChemicalCompositionRow";
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

const SORT_OPTIONS: { value: string; label: string }[] = [
  { value: "median_concentration:desc", label: "Concentration (high → low)" },
  { value: "median_concentration:asc", label: "Concentration (low → high)" },
  { value: "name:asc", label: "Food (A → Z)" },
  { value: "name:desc", label: "Food (Z → A)" },
  { value: "evidence_count:desc", label: "Evidence (most first)" },
  { value: "evidence_count:asc", label: "Evidence (fewest first)" },
];

interface ChemicalCompositionTableProps {
  withConcentrations: Row[] | null | undefined;
  withoutConcentrations: Row[] | null | undefined;
  // The chemical's foodatlas_id — carried into the ?highlight= deep link
  // and the report context, exactly as the old bar chart did.
  chemicalId?: string;
}

const ChemicalCompositionTable = ({
  withConcentrations,
  withoutConcentrations,
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

  const isFiltered = search.trim() !== "" || sources.length > 0;

  return (
    <div className="flex flex-col gap-4">
      {/* toolbar */}
      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div className="flex items-center gap-2 rounded-full bg-light-800 py-1.5 px-3 md:w-64">
          <MdSearch className="text-light-400 flex-shrink-0" />
          <input
            type="search"
            value={search}
            onChange={(e) => {
              setSearch(e.target.value);
              setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
            }}
            placeholder="Search foods"
            aria-label="Search foods"
            className="w-full bg-transparent text-sm text-light-100 placeholder:text-light-500 focus:outline-none"
          />
        </div>

        <div className="flex items-center gap-2 flex-wrap">
          {sourceCounts.map(({ key, label, count }) => (
            <Chip
              key={key}
              label={label}
              count={count}
              tone={sources.includes(key) ? "cream" : "outline"}
              size="md"
              onClick={() => toggleSource(key)}
              aria-pressed={sources.includes(key)}
            />
          ))}
          {unmeasuredCount > 0 && (
            <Chip
              label="Include without concentration"
              count={unmeasuredCount}
              tone={includeUnmeasured ? "cream" : "outline"}
              size="md"
              aria-pressed={includeUnmeasured}
              onClick={() => {
                setIncludeUnmeasured((v) => !v);
                setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
              }}
            />
          )}
        </div>
      </div>

      {/* mobile sort — no column headers to click in card view */}
      <div className="md:hidden">
        <SortListbox
          ariaLabel="Sort foods"
          value={`${sort.column}:${sort.direction}`}
          options={SORT_OPTIONS}
          onChange={(value) => {
            const [column, direction] = value.split(":");
            setSort({
              column: column as SortColumn,
              direction: direction as SortDirection,
            });
            setTablePaginations(TABLE_ID, 1, ROWS_PER_PAGE);
          }}
        />
      </div>

      {/* desktop table */}
      <div className="hidden md:block overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            {COLUMNS.map((c) => (
              <col key={c.key} className={c.width} />
            ))}
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              {COLUMNS.map((c, i) => (
                <th
                  key={c.key}
                  className={twMerge(
                    "h-9 border-b border-light-700 leading-none py-1.5",
                    cellPadding(i, COLUMN_COUNT),
                    c.align === "right" ? "text-right" : "text-left"
                  )}
                >
                  <div
                    className={twMerge(
                      "group flex gap-1 items-center flex-nowrap w-full",
                      c.sort ? "cursor-pointer" : "pointer-events-none",
                      c.align === "right" ? "justify-end" : "justify-between"
                    )}
                    onClick={() => c.sort && handleSortClick(c.sort)}
                  >
                    <span
                      className={twMerge(
                        "select-none uppercase text-xs font-medium transition duration-300 ease-in-out group-hover:text-light-100",
                        c.sort === sort.column && "text-light-100"
                      )}
                    >
                      {c.label}
                    </span>
                    {c.sort &&
                      (c.sort === sort.column ? (
                        sort.direction === "asc" ? (
                          <MdKeyboardArrowUp className="text-accent-600 flex-shrink-0" />
                        ) : (
                          <MdKeyboardArrowDown className="text-accent-600 flex-shrink-0" />
                        )
                      ) : (
                        <MdUnfoldMore className="text-light-400 flex-shrink-0" />
                      ))}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="text-sm font-light">
            {pageRows.length > 0 ? (
              pageRows.map((row) => (
                <ChemicalCompositionTableRow
                  key={row.id}
                  row={row}
                  maxValue={maxValue}
                  href={hrefFor(row)}
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
    </div>
  );
};

ChemicalCompositionTable.displayName = "ChemicalCompositionTable";

export default ChemicalCompositionTable;
