"use client";

// CTD literature correlations between a chemical and a disease, read
// from either end (`tableLocation`).
//
// One table carries BOTH directions, with r4/r3 rendered as an Improves/
// Worsens column and filtered from the sidebar. It used to be two tables
// stacked under two headings, which was tolerable when this owned a whole
// tab — but the merged Diseases/Chemicals tab also stacks the
// assay-inferred table underneath, and three tables on one tab read as a
// list of lists.
//
// Row rendering lives in shared/CorrelationRow.tsx; this file owns
// fetching, paging and the source-chemical decision.

import { useEffect, useMemo, useState } from "react";
import { MdErrorOutline, MdInfoOutline } from "react-icons/md";

import {
  TableSkeletonCards,
  TableSkeletonRows,
} from "@/components/basic/TableSkeleton";
import type { SkeletonColumn } from "@/components/basic/skeletonTokens";
import Pagination from "@/components/basic/Pagination";
import CorrelationEvidenceModal from "@/components/entities/CorrelationEvidenceModal";
import {
  CorrelationCard,
  CorrelationDesktopRow,
  hasDistinctSource,
  rowEvidences,
  type CorrelationDirection,
} from "@/components/entities/shared/CorrelationRow";
import {
  MobileSort,
  nextSort,
  Th,
  type SortableColumn,
  type SortDir,
} from "@/components/entities/shared/EvidenceTable";
import { useReportRows } from "@/context/reportModeContext";
import { usePaginations } from "@/context/paginationsContext";
import { getDiseaseData } from "@/utils/fetching";
import { ChemicalCorrelation } from "@/types";

// What the server can sort this table by — see backend _correlation.SORT_KEYS.
// Direction is a filter, not a sort: it is the sidebar's job.
export type CorrelationSortKey = "name" | "evidence_count";

interface CorrelationTableProps {
  commonName: string;
  tableLocation: string;
  // Direction filter. "all" is the merged tab's default.
  direction?: CorrelationDirection;
  // Server-side search over the peer entity's name.
  search?: string;
  // Fires whenever totalRows changes so the merged tab can sum this
  // table with the assay-inferred one for a single badge.
  onTotalRowsChange?: (total: number) => void;
}

const CorrelationTable = ({
  commonName,
  tableLocation,
  direction = "all",
  search = "",
  onTotalRowsChange,
}: CorrelationTableProps) => {
  const tableId = tableLocation + "-correlation-table";
  const peer = tableLocation === "chemical" ? "disease" : "chemical";

  const [data, setData] = useState<ChemicalCorrelation[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isError, setIsError] = useState(false);
  const [numberOfPages, setNumberOfPages] = useState(1);
  const [totalRows, setTotalRows] = useState<number | null>(null);
  const [selectedRowIdx, setSelectedRowIdx] = useState(-1);

  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);
  const reporter = useReportRows();

  // Server-side sort. Most evidence first by default — the order the
  // table always had — and a header click changes it for every page.
  const [sort, setSort] = useState<{ by: CorrelationSortKey; dir: SortDir }>({
    by: "evidence_count",
    dir: "desc",
  });
  const changeSort = (next: { by: CorrelationSortKey; dir: SortDir }) => {
    setSort(next);
    // Back to page 1: page 3 of the old order is nowhere in the new one.
    setTablePaginations(tableId, 1);
  };
  const sortBy = (key: CorrelationSortKey) =>
    changeSort(nextSort(sort, key, key === "name" ? "asc" : "desc"));

  useEffect(() => {
    if (onTotalRowsChange && totalRows !== null) onTotalRowsChange(totalRows);
  }, [onTotalRowsChange, totalRows]);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const payload = await getDiseaseData(
        commonName,
        currentPage,
        tableLocation,
        direction,
        search,
        sort
      );
      if (cancelled) return;
      if (!payload) {
        setIsError(true);
        setIsLoading(false);
        return;
      }
      setIsError(false);
      setData(payload.data.associations ?? []);
      setNumberOfPages(payload.metadata.total_pages);
      setTotalRows(Number(payload.metadata.total_rows ?? 0));
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [tableLocation, direction, search, currentPage, commonName, sort]);

  // Show the source-chemical column when ANY row on this page attributes
  // its evidence to a different chemical — true on ChEBI class pages,
  // false on the ~80% of pages that are leaves. Decided per page rather
  // than per row because a column cannot be conditional per row; rows
  // that do match then simply name the page's own chemical.
  const showSource = useMemo(
    () =>
      tableLocation === "chemical" &&
      data.some((row) => hasDistinctSource(row, commonName)),
    [data, tableLocation, commonName]
  );

  const peerLabel = peer === "disease" ? "Disease" : "Chemical";
  // Widths are literal class strings: Tailwind only emits what it can
  // see. Direction is one badge and one short word, Publications one
  // chip; the names are the longest content, so they take what is left.
  // With the attribution column the two names share it.
  const headers = useMemo<
    { label: string; sortKey?: CorrelationSortKey; width: string }[]
  >(
    () =>
      showSource
        ? [
            { label: "Direction", width: "w-[13%]" },
            // Peer first, attribution second: the row is about the
            // disease, and the descendant chemical the evidence came
            // through qualifies it.
            { label: peerLabel, sortKey: "name", width: "w-[34%]" },
            { label: "Via Chemical", width: "w-[31%]" },
            { label: "Publications", sortKey: "evidence_count", width: "w-[22%]" },
          ]
        : [
            { label: "Direction", width: "w-[15%]" },
            { label: peerLabel, sortKey: "name", width: "w-[61%]" },
            { label: "Publications", sortKey: "evidence_count", width: "w-[24%]" },
          ],
    [showSource, peerLabel]
  );
  const sortableColumns: SortableColumn<CorrelationSortKey>[] = [
    {
      key: "evidence_count",
      labels: { desc: "Most publications", asc: "Fewest publications" },
    },
    { key: "name", labels: { asc: `${peerLabel} A–Z`, desc: `${peerLabel} Z–A` } },
  ];

  // Skeleton grid derived from the same headers the <th>s render, so the
  // placeholder cells line up. Last column right-aligned, rest left.
  const skeletonColumns: SkeletonColumn[] = headers.map((h, i) => ({
    key: h.label,
    width: h.width,
    align: i === headers.length - 1 ? "right" : "left",
  }));

  const rowPropsFor = (row: ChemicalCorrelation) =>
    reporter.getRowProps({
      kind: "correlation-row",
      entityType: tableLocation as "chemical" | "disease",
      entitySlug: commonName,
      counterpartName: row.name,
      pmidCount: rowEvidences(row).length,
    });

  const emptyState = (
    <div className="h-[10rem] flex items-center justify-center text-light-300 gap-2">
      <MdInfoOutline /> No evidence found
    </div>
  );
  const errorState = (
    <div className="h-[10rem] flex items-center justify-center text-red-400 gap-2">
      <MdErrorOutline /> An error occurred fetching data, please refresh the
      page
    </div>
  );

  const selected = selectedRowIdx < 0 ? undefined : data[selectedRowIdx];

  return (
    <>
      <div>
        {!isLoading && data.length > 0 && (
          <MobileSort
            sort={sort}
            columns={sortableColumns}
            onChange={changeSort}
            ariaLabel={`Sort ${peerLabel.toLowerCase()}s`}
          />
        )}
        {/* table — desktop */}
        <div className="hidden md:block overflow-x-auto">
          <table className="w-full table-fixed">
            {/* `table-fixed` without a colgroup sizes from the header cells,
             * which made Direction (one short word) and Publications (one
             * chip) squeeze the name columns. Equal thirds were the first
             * fix, and gave a one-word column a third of the table. */}
            <colgroup>
              {headers.map((header) => (
                <col key={header.label} className={header.width} />
              ))}
            </colgroup>
            <thead className="text-light-400 text-left">
              <tr>
                {headers.map(({ label, sortKey }, index) => (
                  <Th
                    key={label}
                    align={index === headers.length - 1 ? "right" : undefined}
                    className={
                      index === 0
                        ? "pr-4 pl-0"
                        : index === headers.length - 1
                        ? "pl-4 pr-0"
                        : "px-4"
                    }
                    sort={
                      sortKey && {
                        active: sort.by === sortKey,
                        dir: sort.dir,
                        onClick: () => sortBy(sortKey),
                      }
                    }
                  >
                    {label}
                  </Th>
                ))}
              </tr>
            </thead>
            <tbody className="text-sm font-light">
              {isLoading ? (
                <TableSkeletonRows columns={skeletonColumns} />
              ) : isError || data.length === 0 ? (
                <tr>
                  <td colSpan={headers.length}>
                    {isError ? errorState : emptyState}
                  </td>
                </tr>
              ) : (
                data.map((row, rowIdx) => (
                  <CorrelationDesktopRow
                    key={`${row.id}-${rowIdx}`}
                    row={row}
                    peer={peer}
                    showSource={showSource}
                    commonName={commonName}
                    rowProps={rowPropsFor(row)}
                    onShowMore={() => setSelectedRowIdx(rowIdx)}
                  />
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* card list — mobile */}
        {isLoading ? (
          <TableSkeletonCards columns={skeletonColumns} />
        ) : (
          <div className="md:hidden w-full flex flex-col divide-y divide-light-800">
            {isError || data.length === 0 ? (
              <div className="w-full py-6">
                {isError ? errorState : emptyState}
              </div>
            ) : (
              data.map((row, rowIdx) => (
                <CorrelationCard
                  key={`${row.id}-${rowIdx}`}
                  row={row}
                  peer={peer}
                  showSource={showSource}
                  commonName={commonName}
                  rowProps={rowPropsFor(row)}
                  onShowMore={() => setSelectedRowIdx(rowIdx)}
                />
              ))
            )}
          </div>
        )}

        {/* pagination */}
        {(numberOfPages > 1 || isLoading) && (
          <div className="mt-8 max-w-xl w-full mx-auto">
            <Pagination
              tableId={tableId}
              numberOfPages={numberOfPages}
              isLoading={isLoading}
            />
          </div>
        )}
      </div>

      <CorrelationEvidenceModal
        entityType={tableLocation as "chemical" | "disease"}
        chemicalName={
          tableLocation === "chemical"
            ? (selected?.source_chemical_name ?? commonName)
            : (selected?.name ?? "")
        }
        diseaseName={
          tableLocation === "chemical" ? (selected?.name ?? "") : commonName
        }
        improvesEvidences={selected?.improves_evidences}
        worsensEvidences={selected?.worsens_evidences}
        isOpen={selectedRowIdx >= 0}
        onClose={() => setSelectedRowIdx(-1)}
      />
    </>
  );
};

CorrelationTable.displayName = "CorrelationTable";

export default CorrelationTable;
