"use client";

import { useMemo, useState } from "react";
import { MdDownload } from "react-icons/md";

import Chip from "@/components/basic/Chip";
import Link from "@/components/basic/Link";
import Pagination from "@/components/basic/Pagination";
import { usePaginations } from "@/context/paginationsContext";
import { DownloadEntry } from "@/types";
import { DOWNLOADS_PATH } from "@/utils/site";

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

// What the download handler sends us back with when it can't hand out a
// bundle. Keyed by the `error` query param it sets.
const ERROR_MESSAGES: Record<string, string> = {
  missing_key: "Enter your API key to download a bundle.",
  invalid_key: "That API key was not accepted. Check it, or request a new one.",
  rate_limited: "Too many requests for this key — wait a minute and retry.",
  unavailable: "Downloads are temporarily unavailable. Please try again shortly.",
};

interface DownloadsTableProps {
  data: DownloadRow[];
  // `error` query param from a bounced download attempt, if any.
  error?: string;
}

// Downloads are gated like the API: the same key, POSTed with each
// Download click to /food-composition-downloads/<version>, which forwards
// it to the API and follows the redirect to a short-lived signed URL. The
// key lives in component state only — never in a URL, never persisted.
const DownloadButton = ({
  version,
  apiKey,
}: {
  version: string;
  apiKey: string;
}) => (
  <form method="post" action={`${DOWNLOADS_PATH}/${version}`}>
    <input type="hidden" name="key" value={apiKey} />
    <Chip
      icon={<MdDownload className="size-3" />}
      label="Download"
      tone="outline"
      size="md"
      disabled={!apiKey}
      aria-label={
        apiKey ? `Download ${version}` : "Enter your API key to download"
      }
      onClick={(e) => e.currentTarget.form?.requestSubmit()}
    />
  </form>
);

const DownloadsTable = ({ data, error }: DownloadsTableProps) => {
  const [apiKey, setApiKey] = useState("");
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
    <div className="mb-6 flex flex-col gap-2">
      <label
        htmlFor="downloads-api-key"
        className="text-sm/6 font-medium text-white"
      >
        API key
      </label>
      <input
        id="downloads-api-key"
        type="password"
        autoComplete="off"
        spellCheck={false}
        value={apiKey}
        onChange={(e) => setApiKey(e.target.value.trim())}
        placeholder="Paste the key from your access email"
        className="block w-full max-w-md rounded-lg bg-light-800 border-light-700/50 border py-2 px-3 text-sm/6 text-light-50 placeholder-light-500 focus:outline-none focus:outline-2 focus:-outline-offset-2 focus:outline-white/25"
      />
      <p className="text-sm text-light-400">
        Downloads use the same key as the API. Don&apos;t have one?{" "}
        <Link href="/contact?api-access" isExternal={false}>
          Request access
        </Link>{" "}
        — it&apos;s free.
      </p>
      {error && ERROR_MESSAGES[error] && (
        <p role="alert" className="text-sm text-amber-400">
          {ERROR_MESSAGES[error]}
        </p>
      )}
    </div>
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
                  <DownloadButton version={row.version} apiKey={apiKey} />
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
            <DownloadButton version={row.version} apiKey={apiKey} />
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
