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
import { useReportRows } from "@/context/reportModeContext";
import { usePaginations } from "@/context/paginationsContext";
import { getDiseaseData } from "@/utils/fetching";
import { ChemicalCorrelation } from "@/types";

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

  const { getTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);
  const reporter = useReportRows();

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
        search
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
  }, [tableLocation, direction, search, currentPage, commonName]);

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

  const headers = useMemo(
    () => [
      { label: "Direction" },
      ...(showSource ? [{ label: "Via Chemical" }] : []),
      { label: peer === "disease" ? "Disease" : "Chemical" },
      { label: "Publications" },
    ],
    [showSource, peer]
  );

  // Skeleton grid derived from the same headers the <th>s render, so the
  // placeholder cells line up. Last column right-aligned, rest left.
  const skeletonColumns: SkeletonColumn[] = headers.map((h, i) => ({
    key: h.label,
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
        {/* table — desktop */}
        <div className="hidden md:block overflow-x-auto">
          <table className="w-full table-fixed">
            {/* Equal widths. `table-fixed` without a colgroup sizes from
             * the header cells, which made Direction (one short word) and
             * Publications (one chip) squeeze the two name columns even
             * though the names are the longest content in the table.
             * Literal class strings — Tailwind only emits what it can see,
             * so these cannot be interpolated from headers.length. */}
            <colgroup>
              {headers.map((header) => (
                <col
                  key={header.label}
                  className={headers.length === 4 ? "w-1/4" : "w-1/3"}
                />
              ))}
            </colgroup>
            <thead className="text-light-400 text-left">
              <tr>
                {headers.map((header, index) => (
                  <th
                    key={header.label}
                    className={`h-9 border-b border-light-700 leading-none break-all md:break-normal py-1.5 ${
                      index === 0
                        ? "pr-4"
                        : index === headers.length - 1
                        ? "pl-4 text-right"
                        : "px-4"
                    }`}
                  >
                    <div className="flex flex-nowrap">
                      <span className="select-none uppercase text-xs font-medium w-full">
                        {header.label}
                      </span>
                    </div>
                  </th>
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
