"use client";

// Presentation half of the disease Bioactivities tab. Rows arrive already
// ordered by the API (most bridging assays first), so this component never
// re-sorts — it only renders and paginates the tail.

import { MdArrowForward, MdKeyboardArrowDown } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Link from "@/components/basic/Link";
import { useReportRows } from "@/context/reportModeContext";
import { encodeSpace } from "@/utils/utils";
import type { DiseaseBioactivityChemical } from "@/types";

interface Props {
  rows: DiseaseBioactivityChemical[];
  visibleCount: number;
  onShowAll: () => void;
  // Disease whose page this table sits on — carried into issue reports.
  commonName: string;
}

const entityHref = (kind: string, name: string) =>
  `/${kind}/${encodeURIComponent(encodeSpace(name))}`;

const RelationshipChips = ({ relationships }: { relationships: string[] }) => (
  <span className="inline-flex flex-wrap gap-1">
    {relationships.map((r) => (
      <span
        key={r}
        className="text-[9px] font-mono italic uppercase tracking-[0.1em] text-light-300 border border-light-700 rounded-full px-1.5 py-[1px]"
      >
        {r}
      </span>
    ))}
  </span>
);

const DiseaseBioactivityTable = ({
  rows,
  visibleCount,
  onShowAll,
  commonName,
}: Props) => {
  const visible = rows.slice(0, visibleCount);
  const hiddenCount = rows.length - visible.length;
  const rowKey = (r: DiseaseBioactivityChemical) =>
    `${r.bioactivity_foodatlas_id}-${r.chemical_foodatlas_id}`;
  const reporter = useReportRows();
  const rowReportProps = (row: DiseaseBioactivityChemical) =>
    reporter.getRowProps({
      kind: "disease-bioactivity-row",
      entityType: "disease",
      entitySlug: commonName,
      bioactivityId: row.bioactivity_foodatlas_id,
      bioactivityName: row.bioactivity_name,
      chemicalId: row.chemical_foodatlas_id,
      chemicalName: row.chemical_name,
      nAssays: row.n_assays,
    });

  return (
    <div className="flex flex-col gap-4">
      <div className="hidden md:block overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            <col className="w-[20%]" />
            <col className="w-[34%]" />
            <col className="w-[10%]" />
            <col className="w-[10%]" />
            <col className="w-[26%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <Th>Bioactivity</Th>
              <Th>Chemical</Th>
              <Th
                align="right"
                title="Bridging assays linking this chemical and bioactivity to the disease"
              >
                Assays
              </Th>
              <Th align="right" title="Active measurements across those assays">
                Active
              </Th>
              <Th title="How the bioactivity-disease bridge classifies the link">
                Signal
              </Th>
            </tr>
          </thead>
          <tbody className="text-sm">
            {visible.map((row) => (
              <tr key={rowKey(row)} {...rowReportProps(row)}>
                <td className="py-1.5 pr-4">
                  <div className="flex min-h-9 items-center capitalize">
                    <Link
                      href={entityHref("bioactivity", row.bioactivity_name)}
                      isExternal={false}
                    >
                      {row.bioactivity_name}
                    </Link>
                  </div>
                </td>
                <td className="py-1.5 px-4">
                  <div className="flex min-h-9 items-center capitalize break-words">
                    <Link
                      href={entityHref("chemical", row.chemical_name)}
                      isExternal={false}
                    >
                      {row.chemical_name}
                    </Link>
                  </div>
                </td>
                <td className="py-1.5 px-4 text-right tabular-nums text-light-200">
                  {row.n_assays.toLocaleString()}
                </td>
                <td className="py-1.5 px-4 text-right tabular-nums text-emerald-300">
                  {row.n_active_measurements.toLocaleString()}
                </td>
                <td className="py-1.5 px-4">
                  <RelationshipChips relationships={row.relationships} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Mobile cards */}
      <div className="md:hidden divide-y divide-light-800">
        {visible.map((row) => (
          <div
            key={rowKey(row)}
            className="py-3 flex flex-col gap-2"
            {...rowReportProps(row)}
          >
            <div className="flex items-baseline justify-between gap-2 capitalize">
              <Link
                href={entityHref("chemical", row.chemical_name)}
                isExternal={false}
              >
                {row.chemical_name}
              </Link>
              <MdArrowForward className="w-3.5 h-3.5 text-light-500 shrink-0" />
            </div>
            <div className="text-[11px] font-mono italic text-light-400 capitalize">
              <Link
                href={entityHref("bioactivity", row.bioactivity_name)}
                isExternal={false}
              >
                {row.bioactivity_name}
              </Link>
            </div>
            <CardRow label="Assays">
              <span className="tabular-nums text-light-200">
                {row.n_assays.toLocaleString()}
              </span>
            </CardRow>
            <CardRow label="Active">
              <span className="tabular-nums text-emerald-300">
                {row.n_active_measurements.toLocaleString()}
              </span>
            </CardRow>
            {row.relationships.length > 0 && (
              <div>
                <RelationshipChips relationships={row.relationships} />
              </div>
            )}
          </div>
        ))}
      </div>

      {hiddenCount > 0 && (
        <button
          type="button"
          onClick={onShowAll}
          className="self-center inline-flex items-center gap-1 text-xs font-mono italic text-light-400 hover:text-light-100 transition-colors"
        >
          Show all {rows.length.toLocaleString()} rows
          <MdKeyboardArrowDown className="w-4 h-4" />
        </button>
      )}
    </div>
  );
};

const Th = ({
  children,
  align,
  title,
}: {
  children: React.ReactNode;
  align?: "right";
  title?: string;
}) => (
  <th
    title={title}
    className={twMerge(
      "h-9 border-b border-light-700 py-1.5 px-4 uppercase text-xs font-medium",
      align === "right" ? "text-right" : "text-left",
    )}
  >
    {children}
  </th>
);

const CardRow = ({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) => (
  <div className="flex items-baseline justify-between gap-3 text-[11px] font-mono">
    <span className="text-light-500 shrink-0">{label}</span>
    <span className="text-right">{children}</span>
  </div>
);

DiseaseBioactivityTable.displayName = "DiseaseBioactivityTable";
export default DiseaseBioactivityTable;
