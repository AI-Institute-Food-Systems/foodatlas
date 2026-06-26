// Shared table chrome for the four bioactivity views — mirrors the
// /food/composition table's toolbar + sortable headers + pagination so
// the user gets identical interactions across composition and bioactivity
// pages. Each consumer passes a fetcher, column spec, link builder, and a
// stable tableId; this component owns search/sort/page state, the
// /bioactivity/measurements modal, and the loading skeleton.

"use client";

import { ReactNode, useEffect, useMemo, useState } from "react";
import {
  MdClose,
  MdInfoOutline,
  MdKeyboardArrowDown,
  MdKeyboardArrowUp,
  MdSearch,
  MdUnfoldMore,
} from "react-icons/md";

import Link from "@/components/basic/Link";
import LoadingCard from "@/components/basic/LoadingCard";
import Pagination from "@/components/basic/Pagination";
import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import { formatTopMeasurement, topMeasurementOf } from "@/components/entities/bioactivity/format";
import { usePaginations } from "@/context/paginationsContext";
import { encodeSpace } from "@/utils/utils";
import type {
  BioactivityListParams,
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
  // Renders the cell content for a row.
  render: (row: BioactivityRow, ctx: ColumnContext) => ReactNode;
};

interface Props {
  // Stable identifier for pagination context — e.g. "food-bioact-foodId".
  tableId: string;
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
}

const BioactivityTable = ({
  tableId,
  fetcher,
  columns,
  defaultSortBy = "measurement_count",
  defaultSortDir = "desc",
  searchPlaceholder = "Search…",
  emptyMessage,
  modalConfig,
}: Props) => {
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);

  const [searchTerm, setSearchTerm] = useState("");
  const [sort, setSort] = useState<{ by: string; dir: SortDir }>({
    by: defaultSortBy,
    dir: defaultSortDir,
  });

  const [rows, setRows] = useState<BioactivityRow[]>([]);
  const [totalPages, setTotalPages] = useState(0);
  const [isLoading, setIsLoading] = useState(true);

  const [selected, setSelected] = useState<BioactivityRow | null>(null);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      // Fetcher returns null on staging blips; treat as empty so we render
      // the empty-state instead of a noisy "An error occurred" banner.
      const payload = await fetcher({
        page: currentPage,
        search: searchTerm,
        sortBy: sort.by,
        sortDir: sort.dir,
      });
      if (cancelled) return;
      setRows(payload?.data ?? []);
      setTotalPages(payload?.metadata?.total_pages ?? 0);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [fetcher, currentPage, searchTerm, sort]);

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

  const colSpan = columns.length;
  const showingPaginator = totalPages > 1 || isLoading;
  const showEmpty = !isLoading && rows.length === 0;

  return (
    <div className="flex flex-col gap-7">
      {/* toolbar — same shape as the composition table's search row */}
      <div className="w-full">
        <div className="relative flex items-center">
          <MdSearch className="absolute left-2.5 w-5 h-5 text-light-400" />
          <input
            className="pl-9 pr-9 w-full lg:w-72 h-9 text-sm rounded-lg border border-light-50/5 bg-light-900 focus:bg-light-400/20 hover:bg-light-400/20 text-light-100 placeholder-light-400 transition duration-100 ease-in-out outline-light-50/60"
            type="text"
            placeholder={searchPlaceholder}
            value={searchTerm}
            onChange={handleSearchChange}
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
      </div>

      <div className="overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            {columns.map((c) => (
              <col key={c.key} className={c.width} />
            ))}
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              {columns.map((c, idx) => (
                <th
                  key={c.key}
                  className={`h-9 border-b border-light-700 leading-none break-all md:break-normal py-1.5 ${
                    idx === 0
                      ? "pr-4"
                      : idx === columns.length - 1
                      ? "pl-4"
                      : "px-4"
                  } ${c.align === "right" ? "text-right" : "text-left"}`}
                >
                  {c.sortable ? (
                    <SortableHeader
                      label={c.label}
                      align={c.align}
                      active={sort.by === c.key}
                      dir={sort.dir}
                      onClick={() => handleSortClick(c.key)}
                    />
                  ) : (
                    <span className="select-none uppercase text-xs font-medium">
                      {c.label}
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="text-sm font-light">
            {isLoading ? (
              Array.from({ length: 20 }).map((_, i) => (
                <tr key={`l-${i}`}>
                  <td className="w-full py-1.5" colSpan={colSpan}>
                    <div className="h-9 flex items-center">
                      <LoadingCard className="h-5" />
                    </div>
                  </td>
                </tr>
              ))
            ) : showEmpty ? (
              <tr>
                <td colSpan={colSpan}>
                  <div className="h-[10rem] flex items-center justify-center text-light-300 gap-2">
                    <MdInfoOutline /> {emptyMessage}
                  </div>
                </td>
              </tr>
            ) : (
              rows.map((row) => (
                <BioactivityTableRow
                  key={row.id}
                  row={row}
                  columns={columns}
                  onOpen={() => setSelected(row)}
                />
              ))
            )}
          </tbody>
        </table>
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
    </div>
  );
};

const BioactivityTableRow = ({
  row,
  columns,
  onOpen,
}: {
  row: BioactivityRow;
  columns: SortableColumn[];
  onOpen: () => void;
}) => {
  const ctx: ColumnContext = { openModal: onOpen };
  return (
    <tr>
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

const SortableHeader = ({
  label,
  align,
  active,
  dir,
  onClick,
}: {
  label: string;
  align?: "left" | "right";
  active: boolean;
  dir: SortDir;
  onClick: () => void;
}) => (
  <button
    type="button"
    onClick={onClick}
    className={`group flex items-center gap-1 ${
      align === "right" ? "justify-end ml-auto" : ""
    } cursor-pointer focus:outline-none`}
  >
    <span
      className={`select-none uppercase text-xs font-medium transition duration-300 ease-in-out ${
        active ? "text-light-100" : "text-light-400 group-hover:text-light-100"
      }`}
    >
      {label}
    </span>
    {active ? (
      dir === "asc" ? (
        <MdKeyboardArrowUp className="text-accent-600 group-hover:text-accent-300 flex-shrink-0" />
      ) : (
        <MdKeyboardArrowDown className="text-accent-600 group-hover:text-accent-300 flex-shrink-0" />
      )
    ) : (
      <MdUnfoldMore className="text-light-400 group-hover:text-light-100 flex-shrink-0" />
    )}
  </button>
);

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
  <button
    type="button"
    onClick={ctx.openModal}
    disabled={row.measurement_count === 0}
    className="font-mono italic text-xs px-2.5 py-0.5 rounded-full border border-light-700/60 text-light-300 hover:text-light-100 hover:border-light-500 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
  >
    View {row.measurement_count.toLocaleString()} assay
    {row.measurement_count === 1 ? "" : "s"} →
  </button>
);

BioactivityTable.displayName = "BioactivityTable";

export default BioactivityTable;
