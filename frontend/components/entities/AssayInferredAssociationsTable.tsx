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
  AssaysModal,
  AssayTargetsModal,
} from "@/components/entities/shared/AssayDetailModals";
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

// There is no "Active" column, and adding one back would be a mistake.
// `n_active_measurements` is identically equal to `n_assays` in every row
// of the data: 347,632/347,632 in mv_chemical_disease_bioactivity and
// 408,118/408,118 in mv_disease_bioactivity, zero exceptions. The
// materializer computes them as nunique(source_assay_id) and nunique(bm)
// over evidence that carries exactly one active measurement per assay, so
// they cannot diverge as long as that holds. Two columns of the same
// number read as corroboration and are not.
//
// The field is still returned by the API and still travels in the row's
// report metadata — only the column is gone.
//
// Mirrors the <colgroup> and cell alignment of the real table below, so
// the loading grid lines up with the loaded one. Kept next to them: if
// one changes, the other is a line away.
const SKELETON_COLUMNS: SkeletonColumn[] = [
  { key: "peer", width: "w-[28%]" },
  { key: "assays", width: "w-[10%]", align: "right" },
  { key: "signal", width: "w-[24%]" },
  { key: "target", width: "w-[20%]" },
  { key: "evidence", width: "w-[18%]" },
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
  // Which row's Target / Assays modal is open, by peer id. Two ids
  // rather than one plus a mode, so opening one cannot leave the other
  // rendering last frame's row.
  const [targetsFor, setTargetsFor] = useState<string | null>(null);
  const [assaysFor, setAssaysFor] = useState<string | null>(null);
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
  const byId = (id: string | null) =>
    id === null ? undefined : rows.find((r) => peerId(r, peer) === id);
  const targetsRow = byId(targetsFor);
  const assaysRow = byId(assaysFor);

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
            <col className="w-[28%]" />
            <col className="w-[10%]" />
            <col className="w-[24%]" />
            <col className="w-[20%]" />
            <col className="w-[18%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <Th>{peerLabel}</Th>
              <Th
                align="right"
                title="Number of distinct shared bioactivity assays backing this association"
              >
                # Assays
              </Th>
              <Th title="How CTD classifies the link: therapeutic (treats) or marker/mechanism (marks or drives). Opposite directions.">
                Signal
              </Th>
              <Th title="The protein target the bridging assays measure — what the association runs through">
                Target
              </Th>
              <Th title="The source assays behind this association">Assays</Th>
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
                  onOpenTargets={() => setTargetsFor(peerId(row, peer))}
                  onOpenAssays={() => setAssaysFor(peerId(row, peer))}
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
              onOpenTargets={() => setTargetsFor(peerId(row, peer))}
              onOpenAssays={() => setAssaysFor(peerId(row, peer))}
            />
          ))}
        </div>
      )}

      {targetsRow && (
        <AssayTargetsModal
          targets={targetsRow.targets ?? []}
          peerName={peerName(targetsRow, peer)}
          isOpen
          onClose={() => setTargetsFor(null)}
        />
      )}
      {assaysRow && (
        <AssaysModal
          assays={assaysRow.assays ?? []}
          totalCount={assaysRow.n_assays}
          peerName={peerName(assaysRow, peer)}
          isOpen
          onClose={() => setAssaysFor(null)}
        />
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
