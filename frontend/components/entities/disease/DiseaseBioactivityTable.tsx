"use client";

// Presentation half of the disease Bioactivities tab. Rows arrive already
// ordered by the API (most bridging assays first), so this component never
// re-sorts — it only renders and paginates the tail.

import { MdArrowForward, MdKeyboardArrowDown } from "react-icons/md";

import Link from "@/components/basic/Link";
import AssayEvidenceLinks from "@/components/entities/shared/AssayEvidenceLinks";
import {
  CardRow,
  CountCell,
  Th,
} from "@/components/entities/shared/EvidenceTable";
import LiteratureBadge from "@/components/entities/shared/LiteratureBadge";
import SignalChips from "@/components/entities/shared/SignalChips";
import TargetGeneChips from "@/components/entities/shared/TargetGeneChips";
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

// Direction plus, where the literature covers the pair, whether it agrees.
const SignalCell = ({ row }: { row: DiseaseBioactivityChemical }) => (
  <span className="inline-flex flex-wrap items-baseline gap-1">
    <SignalChips relationships={row.relationships} />
    <LiteratureBadge
      relationships={row.relationships}
      literatureDirections={row.literature_directions}
    />
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
            <col className="w-[15%]" />
            <col className="w-[22%]" />
            <col className="w-[8%]" />
            <col className="w-[8%]" />
            <col className="w-[21%]" />
            <col className="w-[16%]" />
            <col className="w-[10%]" />
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
              <Th title="How CTD classifies the link: therapeutic (treats) or marker/mechanism (marks or drives). Opposite directions.">
                Signal
              </Th>
              <Th title="The protein target the bridging assays measure">
                Target
              </Th>
              <Th title="The source assays behind this row">Evidence</Th>
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
                <td className="py-1.5 px-4 text-right">
                  <CountCell value={row.n_assays} />
                </td>
                <td className="py-1.5 px-4 text-right">
                  <CountCell
                    value={row.n_active_measurements}
                    tone="text-emerald-300"
                  />
                </td>
                <td className="py-1.5 px-4">
                  <SignalCell row={row} />
                </td>
                <td className="py-1.5 px-4">
                  <TargetGeneChips targets={row.targets} visible={2} />
                </td>
                <td className="py-1.5 px-4">
                  <AssayEvidenceLinks
                    assays={row.assays}
                    totalCount={row.n_assays}
                    visible={1}
                  />
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
              <CountCell value={row.n_assays} />
            </CardRow>
            <CardRow label="Active">
              <CountCell
                value={row.n_active_measurements}
                tone="text-emerald-300"
              />
            </CardRow>
            <div>
              <SignalCell row={row} />
            </div>
            {!!row.targets?.length && (
              <CardRow label="Target">
                <TargetGeneChips targets={row.targets} visible={2} />
              </CardRow>
            )}
            {!!row.assays?.length && (
              <CardRow label="Evidence">
                <AssayEvidenceLinks
                  assays={row.assays}
                  totalCount={row.n_assays}
                  visible={1}
                />
              </CardRow>
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

DiseaseBioactivityTable.displayName = "DiseaseBioactivityTable";
export default DiseaseBioactivityTable;
