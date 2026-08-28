"use client";

// Presentation half of the disease Bioactivities tab. Rows arrive already
// ordered by the API (most bridging assays first), so this component never
// re-sorts — it only renders and paginates the tail.

import { useState } from "react";
import { MdArrowForward, MdKeyboardArrowDown } from "react-icons/md";

import Link from "@/components/basic/Link";
import {
  TableSkeletonCards,
  TableSkeletonRows,
} from "@/components/basic/TableSkeleton";
import type { SkeletonColumn } from "@/components/basic/skeletonTokens";
import {
  CardRow,
  CountCell,
  Th,
} from "@/components/entities/shared/EvidenceTable";
import {
  AssaysModal,
  AssayTargetsModal,
  DetailCountButton,
} from "@/components/entities/shared/AssayDetailModals";
import LiteratureBadge from "@/components/entities/shared/LiteratureBadge";
import SignalChips from "@/components/entities/shared/SignalChips";
import { useReportRows } from "@/context/reportModeContext";
import { encodeSpace } from "@/utils/utils";
import type { DiseaseBioactivityChemical } from "@/types";

interface Props {
  rows: DiseaseBioactivityChemical[];
  visibleCount: number;
  onShowAll: () => void;
  // Disease whose page this table sits on — carried into issue reports.
  commonName: string;
  // Renders the skeleton body in place of rows. The table keeps its
  // header and column geometry while loading, so the section's heading,
  // description and chip row stay put instead of being replaced wholesale.
  isLoading?: boolean;
}

// Mirrors the <colgroup> and cell alignment of the real table below.
// No Assays or Active count columns. n_active_measurements is identically
// equal to n_assays across all 408,118 rows of mv_disease_bioactivity, and
// n_assays is already stated by the Assays cell's own button — three
// printings of one number. See AssayInferredAssociationsTable for the
// full arithmetic.
const SKELETON_COLUMNS: SkeletonColumn[] = [
  { key: "bioactivity", width: "w-[18%]" },
  { key: "chemical", width: "w-[24%]" },
  { key: "signal", width: "w-[22%]" },
  { key: "target", width: "w-[18%]" },
  { key: "evidence", width: "w-[18%]" },
];

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
  isLoading = false,
}: Props) => {
  const visible = rows.slice(0, visibleCount);
  const hiddenCount = rows.length - visible.length;
  const rowKey = (r: DiseaseBioactivityChemical) =>
    `${r.bioactivity_foodatlas_id}-${r.chemical_foodatlas_id}`;
  // Keyed by row, not a boolean, so the modal cannot show a stale row's
  // assays after a different button is clicked. Two ids rather than one
  // plus a mode, matching AssayInferredAssociationsTable, so opening one
  // cannot leave the other rendering last frame's row.
  const [assaysFor, setAssaysFor] = useState<string | null>(null);
  const [targetsFor, setTargetsFor] = useState<string | null>(null);
  const assaysRow = rows.find((r) => rowKey(r) === assaysFor);
  const targetsRow = rows.find((r) => rowKey(r) === targetsFor);
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
            <col className="w-[18%]" />
            <col className="w-[24%]" />
            <col className="w-[22%]" />
            <col className="w-[18%]" />
            <col className="w-[18%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <Th>Bioactivity</Th>
              <Th>Chemical</Th>
              <Th title="How CTD classifies the link: therapeutic (treats) or marker/mechanism (marks or drives). Opposite directions.">
                Signal
              </Th>
              <Th title="The protein target the bridging assays measure">
                Target
              </Th>
              <Th title="The bridging assays behind this row, and how many">
                Assays
              </Th>
            </tr>
          </thead>
          <tbody className="text-sm">
            {isLoading && <TableSkeletonRows columns={SKELETON_COLUMNS} />}
            {!isLoading &&
              visible.map((row) => (
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
                <td className="py-1.5 px-4">
                  <SignalCell row={row} />
                </td>
                <td className="py-1.5 px-4">
                  <DetailCountButton
                    n={row.targets?.length ?? 0}
                    noun="target"
                    onOpen={() => setTargetsFor(rowKey(row))}
                  />
                </td>
                <td className="py-1.5 px-4">
                  <DetailCountButton
                    n={row.n_assays}
                    noun="assay"
                    onOpen={() => setAssaysFor(rowKey(row))}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Mobile cards */}
      {isLoading ? (
        <TableSkeletonCards columns={SKELETON_COLUMNS} />
      ) : (
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
            <div>
              <SignalCell row={row} />
            </div>
            {!!row.targets?.length && (
              <CardRow label="Target">
                <DetailCountButton
                  n={row.targets.length}
                  noun="target"
                  onOpen={() => setTargetsFor(rowKey(row))}
                />
              </CardRow>
            )}
            {!!row.assays?.length && (
              <CardRow label="Assays">
                <DetailCountButton
                  n={row.n_assays}
                  noun="assay"
                  onOpen={() => setAssaysFor(rowKey(row))}
                />
              </CardRow>
            )}
          </div>
        ))}
      </div>
      )}

      {targetsRow && (
        <AssayTargetsModal
          targets={targetsRow.targets ?? []}
          peerName={`${targetsRow.chemical_name} — ${targetsRow.bioactivity_name}`}
          isOpen
          onClose={() => setTargetsFor(null)}
        />
      )}
      {assaysRow && (
        <AssaysModal
          assays={assaysRow.assays ?? []}
          totalCount={assaysRow.n_assays}
          peerName={`${assaysRow.chemical_name} — ${assaysRow.bioactivity_name}`}
          isOpen
          onClose={() => setAssaysFor(null)}
        />
      )}

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
