"use client";

// Shared table for the two bioactivity-inferred chemical↔disease
// association endpoints:
//
//   GET /chemical/disease-associations?common_name=<chemical>
//   GET /disease/chemical-associations?common_name=<disease>
//
// This is a DIFFERENT signal from the CTD literature correlations
// (/chemical/correlation) — an association here means the row entity
// has ≥1 Active measurement in a shared bioactivity assay.
//
// The two now share a tab (CorrelationEvidenceTab) but deliberately not
// a table: they sit as separate labelled blocks so the distinction lands
// next to the rows rather than being lost in a merged row set.
//
// Rows are ordered by n_assays desc server-side; the endpoint isn't
// paginated, so we render the first `initialPageSize` and offer a "Show
// all" affordance for the long tail — and filter in memory, since every
// row is already here.

import { useEffect, useMemo, useState } from "react";
import { MdKeyboardArrowDown } from "react-icons/md";

import {
  TableSkeletonCards,
  TableSkeletonRows,
} from "@/components/basic/TableSkeleton";
import type { SkeletonColumn } from "@/components/basic/skeletonTokens";
import {
  PeerCard,
  PeerRow,
  peerId,
  peerName,
  type PeerDirection,
} from "@/components/entities/shared/AssayInferredRow";
import { Th } from "@/components/entities/shared/EvidenceTable";
import { useReportRows } from "@/context/reportModeContext";
import { usePublishTabCount } from "@/context/tabCountsContext";
import type { AssayInferredAssociation } from "@/types";

export type { PeerDirection };

// Mirrors the <colgroup> and cell alignment of the real table below, so
// the loading grid lines up with the loaded one. Kept next to them: if
// one changes, the other is a line away.
const SKELETON_COLUMNS: SkeletonColumn[] = [
  { key: "peer", width: "w-[26%]" },
  { key: "assays", width: "w-[8%]", align: "right" },
  { key: "active", width: "w-[8%]", align: "right" },
  { key: "signal", width: "w-[24%]" },
  { key: "target", width: "w-[20%]" },
  { key: "evidence", width: "w-[14%]" },
];

interface Props {
  // Anchor entity's common_name — used only in the empty-state copy.
  commonName: string;
  // Direction of the peer we're LISTING (i.e., the entity type each
  // row links to). For the chemical page, peer = "disease"; for the
  // disease page, peer = "chemical".
  peer: PeerDirection;
  // The fetcher for this direction, e.g. () => getChemicalDiseaseAssociations(commonName).
  fetcher: () => Promise<{
    data: AssayInferredAssociation[];
    metadata: { row_count: number };
  } | null>;
  // Tab id whose badge should reflect this table's row count. Empty when
  // a parent tab owns the badge and sums this table with another —
  // usePublishTabCount no-ops on a falsy id.
  tabId?: string;
  initialPageSize?: number;
  // Search driven by a parent FilterPanel. Filtered client-side because
  // this endpoint isn't paginated — every row is already in memory.
  externalSearch?: string;
  // Post-filter row count, for a parent summing several tables.
  onTotalRowsChange?: (total: number) => void;
}

const AssayInferredAssociationsTable = ({
  commonName,
  peer,
  fetcher,
  tabId = "",
  initialPageSize = 50,
  externalSearch = "",
  onTotalRowsChange,
}: Props) => {
  const [rows, setRows] = useState<AssayInferredAssociation[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [showAll, setShowAll] = useState(false);
  const reporter = useReportRows();

  // The anchor page is whichever side we're NOT listing: on a chemical page
  // we list diseases, so a report about one is filed against the chemical.
  const anchorType = peer === "disease" ? "chemical" : "disease";
  const rowReportProps = (row: AssayInferredAssociation) =>
    reporter.getRowProps({
      kind: "assay-inferred-row",
      entityType: anchorType,
      entitySlug: commonName,
      peerId: peerId(row, peer),
      peerName: peerName(row, peer),
      nAssays: row.n_assays,
      nActiveMeasurements: row.n_active_measurements,
    });

  // Search applies to the peer's name — the only free-text column here.
  const filtered = useMemo(() => {
    const term = externalSearch.trim().toLowerCase();
    if (!term) return rows;
    return rows.filter((row) =>
      peerName(row, peer).toLowerCase().includes(term)
    );
  }, [rows, externalSearch, peer]);

  usePublishTabCount(tabId, isLoading ? null : filtered.length);
  useEffect(() => {
    if (onTotalRowsChange && !isLoading) onTotalRowsChange(filtered.length);
  }, [onTotalRowsChange, filtered.length, isLoading]);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const payload = await fetcher();
      if (cancelled) return;
      setRows(payload?.data ?? []);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [fetcher]);

  const visible = useMemo(
    () => (showAll ? filtered : filtered.slice(0, initialPageSize)),
    [filtered, showAll, initialPageSize]
  );
  const hiddenCount = filtered.length - visible.length;

  const peerLabel = peer === "disease" ? "Disease" : "Chemical";

  if (!isLoading && filtered.length === 0) {
    return (
      <p className="text-sm text-light-500 italic">
        {rows.length === 0 ? (
          <>
            No assay-inferred {peer} associations for{" "}
            <span className="capitalize">{commonName}</span> in the current
            data.
          </>
        ) : (
          <>No assay-inferred {peer} associations match this search.</>
        )}
      </p>
    );
  }

  return (
    <div className="flex flex-col gap-4">
      <div className="hidden md:block overflow-x-auto">
        <table className="w-full table-fixed">
          <colgroup>
            <col className="w-[26%]" />
            <col className="w-[8%]" />
            <col className="w-[8%]" />
            <col className="w-[24%]" />
            <col className="w-[20%]" />
            <col className="w-[14%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <Th>{peerLabel}</Th>
              <Th
                align="right"
                title="Number of distinct shared bioactivity assays backing this association"
              >
                Assays
              </Th>
              <Th
                align="right"
                title="Number of Active measurements across those assays"
              >
                Active
              </Th>
              <Th title="How CTD classifies the link: therapeutic (treats) or marker/mechanism (marks or drives). Opposite directions.">
                Signal
              </Th>
              <Th title="The protein target the bridging assays measure — what the association runs through">
                Target
              </Th>
              <Th title="The source assays behind this association">Evidence</Th>
            </tr>
          </thead>
          <tbody className="text-sm">
            {isLoading ? (
              <TableSkeletonRows columns={SKELETON_COLUMNS} />
            ) : (
              visible.map((row) => (
                <PeerRow
                  key={peerId(row, peer)}
                  row={row}
                  peer={peer}
                  reportProps={rowReportProps(row)}
                />
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Mobile card view */}
      {isLoading ? (
        <TableSkeletonCards columns={SKELETON_COLUMNS} />
      ) : (
        <div className="md:hidden divide-y divide-light-800">
          {visible.map((row) => (
            <PeerCard
              key={peerId(row, peer)}
              row={row}
              peer={peer}
              reportProps={rowReportProps(row)}
            />
          ))}
        </div>
      )}

      {hiddenCount > 0 && (
        <button
          type="button"
          onClick={() => setShowAll(true)}
          className="self-center inline-flex items-center gap-1 text-xs font-mono italic text-light-400 hover:text-light-100 transition-colors"
        >
          Show all {filtered.length.toLocaleString()} associations
          <MdKeyboardArrowDown className="w-4 h-4" />
        </button>
      )}
    </div>
  );
};

AssayInferredAssociationsTable.displayName = "AssayInferredAssociationsTable";
export default AssayInferredAssociationsTable;
