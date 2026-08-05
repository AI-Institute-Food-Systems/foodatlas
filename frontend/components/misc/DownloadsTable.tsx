"use client";

import { useMemo } from "react";
import { MdDownload } from "react-icons/md";

import Chip from "@/components/basic/Chip";
import Pagination from "@/components/basic/Pagination";
import { usePaginations } from "@/context/paginationsContext";
import { DownloadEntry } from "@/types";

export type DownloadRow = DownloadEntry & { summary: string };

const TABLE_ID = "downloads-page";
// 10 per page — matches the cadence of ~1 release/week for ~2 months
// per page, so the first page usually holds "this quarter" without
// asking the user to click through.
const ROWS_PER_PAGE = 10;

type Alignment = "left" | "right";

const COLUMNS: { label: string; widthClass: string; align: Alignment }[] = [
  { label: "version", widthClass: "md:w-24", align: "left" },
  { label: "release date", widthClass: "md:w-32", align: "left" },
  { label: "summary", widthClass: "md:w-auto", align: "left" },
  { label: "size", widthClass: "md:w-24", align: "left" },
  { label: "", widthClass: "md:w-32", align: "right" },
];

interface DownloadsTableProps {
  data: DownloadRow[];
}

const DownloadsTable = ({ data }: DownloadsTableProps) => {
  const { getTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(TABLE_ID);
  const totalRows = data.length;
  const numberOfPages = Math.max(
    1,
    Math.ceil(totalRows / ROWS_PER_PAGE),
  );
  // Slice the fetched-once dataset to the active page. Client-side
  // paging keeps the server component simple — the /download endpoint
  // still returns the whole manifest in one shot (dataset is small).
  const pageRows = useMemo(() => {
    const start = (currentPage - 1) * ROWS_PER_PAGE;
    return data.slice(start, start + ROWS_PER_PAGE);
  }, [data, currentPage]);
  const showPaginator = numberOfPages > 1;

  return (
    <>
    <div className="hidden md:block overflow-x-auto">
      <table className="w-full md:table-fixed">
        <thead className="text-light-400">
          <tr>
            {COLUMNS.map((col, index) => (
              <th
                key={index}
                className={`h-12 border-b border-light-700 leading-none py-2 ${
                  col.widthClass
                } ${
                  col.align === "right" ? "text-right" : "text-left"
                } ${
                  index === 0
                    ? "pr-3"
                    : index === COLUMNS.length - 1
                    ? "pl-3"
                    : "px-3"
                }`}
              >
                <span className="select-none uppercase text-xs font-medium">
                  {col.label}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="font-light">
          {pageRows.map((row) => (
            <tr key={row.release_date + "_" + row.version}>
              <td className="py-2 pr-3">
                <div className="flex min-h-12 items-center">{row.version}</div>
              </td>
              <td className="py-2 px-3">
                <div className="flex min-h-12 items-center">
                  {row.release_date}
                </div>
              </td>
              <td className="py-2 px-3">
                <div className="flex min-h-12 items-center text-light-200">
                  {row.summary}
                </div>
              </td>
              <td className="py-2 px-3">
                <div className="flex min-h-12 items-center">
                  {row.file_size}
                </div>
              </td>
              <td className="py-2 pl-3">
                <div className="flex min-h-12 justify-end items-center">
                  <Chip
                    icon={<MdDownload className="size-3" />}
                    label="Download"
                    tone="outline"
                    size="md"
                    href={row.download_link}
                  />
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>

    {/* card list — mobile. Version + release date on top, summary and
     * size below, download button on its own line. */}
    <div className="md:hidden w-full flex flex-col divide-y divide-light-800">
      {pageRows.map((row) => (
        <div
          key={row.release_date + "_" + row.version}
          className="w-full py-3 flex flex-col gap-2"
        >
          <div className="w-full flex items-baseline justify-between gap-2">
            <span className="font-mono text-sm text-light-100">
              {row.version}
            </span>
            <span className="font-mono italic text-[11px] text-light-500 tabular-nums">
              {row.release_date}
            </span>
          </div>
          <p className="w-full text-light-200 text-sm leading-snug">
            {row.summary}
          </p>
          <div className="w-full flex items-center justify-between gap-2">
            <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
              {row.file_size}
            </span>
            <Chip
              icon={<MdDownload className="size-3" />}
              label="Download"
              tone="outline"
              size="md"
              href={row.download_link}
            />
          </div>
        </div>
      ))}
    </div>

    {showPaginator && (
      <div className="mt-4 max-w-xl w-full mx-auto">
        <Pagination
          tableId={TABLE_ID}
          numberOfPages={numberOfPages}
          isLoading={false}
        />
      </div>
    )}
    </>
  );
};

export default DownloadsTable;
