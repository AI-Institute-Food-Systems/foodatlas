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
    <div className="overflow-x-auto">
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
  );
};

export default DownloadsTable;
