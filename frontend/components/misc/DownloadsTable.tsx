import { MdDownload } from "react-icons/md";

import { DownloadEntry } from "@/types";

export type DownloadRow = DownloadEntry & { summary: string };

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
          {data.map((row) => (
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
                  <a
                    href={row.download_link}
                    className="flex h-fit w-fit items-center gap-1 border border-light-300 text-light-300 hover:border-light-200 hover:text-light-200 px-[0.40rem] py-[0.05rem] text-[0.7rem] rounded transition-all duration-150"
                  >
                    <MdDownload />
                    Download
                  </a>
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
      {data.map((row) => (
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
            {/* Same pill affordance as the other tables' action buttons. */}
            <a
              href={row.download_link}
              className="inline-flex items-center gap-1 font-mono italic text-xs px-2.5 py-0.5 rounded-full border border-light-700/60 text-light-300 hover:text-light-100 hover:border-light-500 transition-colors whitespace-nowrap"
            >
              <MdDownload />
              Download
            </a>
          </div>
        </div>
      ))}
    </div>
    </>
  );
};

export default DownloadsTable;
