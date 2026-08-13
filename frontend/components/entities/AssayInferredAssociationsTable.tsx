"use client";

// Shared table for the two bioactivity-inferred chemical↔disease
// association endpoints:
//
//   GET /chemical/disease-associations?common_name=<chemical>
//   GET /disease/chemical-associations?common_name=<disease>
//
// This is a DIFFERENT signal from the CTD literature correlations
// (/chemical/correlation) — an association here means the row entity
// has ≥1 Active measurement in a shared bioactivity assay. Kept
// visually distinct from the CTD Health Impacts section so users don't
// conflate the two evidence sources.
//
// Rendered on a dedicated "Diseases (assay-inferred)" tab on chemical
// pages and "Chemicals (assay-inferred)" on disease pages. Rows are
// ordered by n_assays desc server-side; the endpoint isn't paginated,
// so we render the first `initialPageSize` and offer a "Show all"
// affordance for the long tail.

import { useEffect, useMemo, useState } from "react";
import { MdKeyboardArrowDown } from "react-icons/md";

import LoadingCard from "@/components/basic/LoadingCard";
import {
  PeerCard,
  PeerRow,
  peerId,
  peerName,
  type PeerDirection,
} from "@/components/entities/shared/AssayInferredRow";
import { Th } from "@/components/entities/shared/EvidenceTable";
import { useLoadingGate } from "@/context/pageReadyContext";
import { useReportRows } from "@/context/reportModeContext";
import { usePublishTabCount } from "@/context/tabCountsContext";
import type { AssayInferredAssociation } from "@/types";

export type { PeerDirection };

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
  // Tab id whose badge should reflect this table's row count.
  tabId: string;
  initialPageSize?: number;
}

const AssayInferredAssociationsTable = ({
  commonName,
  peer,
  fetcher,
  tabId,
  initialPageSize = 50,
}: Props) => {
  const [rows, setRows] = useState<AssayInferredAssociation[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  useLoadingGate(isLoading);
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

  usePublishTabCount(tabId, isLoading ? null : rows.length);

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
    () => (showAll ? rows : rows.slice(0, initialPageSize)),
    [rows, showAll, initialPageSize]
  );
  const hiddenCount = rows.length - visible.length;

  const peerLabel = peer === "disease" ? "Disease" : "Chemical";

  if (isLoading) {
    return (
      <div className="flex flex-col gap-2">
        {Array.from({ length: 8 }).map((_, i) => (
          <LoadingCard key={i} className="h-8" />
        ))}
      </div>
    );
  }

  if (rows.length === 0) {
    return (
      <p className="text-sm text-light-500 italic">
        No assay-inferred {peer} associations for{" "}
        <span className="capitalize">{commonName}</span> in the current data.
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
            {visible.map((row) => (
              <PeerRow
                key={peerId(row, peer)}
                row={row}
                peer={peer}
                reportProps={rowReportProps(row)}
              />
            ))}
          </tbody>
        </table>
      </div>

      {/* Mobile card view */}
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

      {hiddenCount > 0 && (
        <button
          type="button"
          onClick={() => setShowAll(true)}
          className="self-center inline-flex items-center gap-1 text-xs font-mono italic text-light-400 hover:text-light-100 transition-colors"
        >
          Show all {rows.length.toLocaleString()} associations
          <MdKeyboardArrowDown className="w-4 h-4" />
        </button>
      )}
    </div>
  );
};

AssayInferredAssociationsTable.displayName = "AssayInferredAssociationsTable";
export default AssayInferredAssociationsTable;
