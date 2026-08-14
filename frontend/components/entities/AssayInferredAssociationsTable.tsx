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
import { MdArrowForward, MdKeyboardArrowDown } from "react-icons/md";

import Link from "@/components/basic/Link";
import {
  TableSkeletonCards,
  TableSkeletonRows,
} from "@/components/basic/TableSkeleton";
import type { SkeletonColumn } from "@/components/basic/skeletonTokens";
import { useReportRows } from "@/context/reportModeContext";
import { usePublishTabCount } from "@/context/tabCountsContext";
import { encodeSpace } from "@/utils/utils";
import type { AssayInferredAssociation } from "@/types";
import { useDeferredLoading } from "@/hooks/useDeferredLoading";

// Which side of the pair the *other* entity is on — determines which
// name/id to render and where its detail page lives.
export type PeerDirection = "disease" | "chemical";

// Mirrors the <colgroup> and cell alignment of the real table below, so
// the loading grid lines up with the loaded one. Kept next to them: if
// one changes, the other is a line away.
const SKELETON_COLUMNS: SkeletonColumn[] = [
  { key: "peer", width: "w-[40%]" },
  { key: "assays", width: "w-[10%]", align: "right" },
  { key: "active", width: "w-[10%]", align: "right" },
  { key: "signal", width: "w-[40%]" },
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
  // Skeleton only once the fetch has been slow enough to be worth
  // announcing; the empty state below still branches on isLoading.
  const showSkeleton = useDeferredLoading(isLoading);
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

  if (!isLoading && rows.length === 0) {
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
            <col className="w-[40%]" />
            <col className="w-[10%]" />
            <col className="w-[10%]" />
            <col className="w-[40%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <th className="h-9 border-b border-light-700 py-1.5 pr-4 uppercase text-xs font-medium">
                {peerLabel}
              </th>
              <th
                className="h-9 border-b border-light-700 py-1.5 px-4 text-right uppercase text-xs font-medium"
                title="Number of distinct shared bioactivity assays backing this association"
              >
                Assays
              </th>
              <th
                className="h-9 border-b border-light-700 py-1.5 px-4 text-right uppercase text-xs font-medium"
                title="Number of Active measurements across those assays"
              >
                Active
              </th>
              <th className="h-9 border-b border-light-700 py-1.5 px-4 uppercase text-xs font-medium">
                Signal
              </th>
            </tr>
          </thead>
          <tbody className="text-sm">
            {showSkeleton ? (
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
      {showSkeleton ? (
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
          Show all {rows.length.toLocaleString()} associations
          <MdKeyboardArrowDown className="w-4 h-4" />
        </button>
      )}
    </div>
  );
};

const peerId = (row: AssayInferredAssociation, peer: PeerDirection) =>
  peer === "disease" ? row.disease_foodatlas_id : row.chemical_foodatlas_id;

const peerName = (row: AssayInferredAssociation, peer: PeerDirection) =>
  peer === "disease" ? row.disease_name : row.chemical_name;

const peerHref = (row: AssayInferredAssociation, peer: PeerDirection) =>
  `/${peer}/${encodeURIComponent(encodeSpace(peerName(row, peer)))}`;

const RelationshipChips = ({
  relationships,
}: {
  relationships: string[];
}) => (
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

const PeerRow = ({
  row,
  peer,
  reportProps,
}: {
  row: AssayInferredAssociation;
  peer: PeerDirection;
  reportProps: ReturnType<ReturnType<typeof useReportRows>["getRowProps"]>;
}) => (
  <tr {...reportProps}>
    <td className="py-1.5 pr-4">
      <div className="flex min-h-9 items-center capitalize">
        <Link href={peerHref(row, peer)} isExternal={false}>
          {peerName(row, peer)}
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
);

const PeerCard = ({
  row,
  peer,
  reportProps,
}: {
  row: AssayInferredAssociation;
  peer: PeerDirection;
  reportProps: ReturnType<ReturnType<typeof useReportRows>["getRowProps"]>;
}) => (
  <div className="py-3 flex flex-col gap-2" {...reportProps}>
    <div className="flex items-baseline justify-between gap-2 capitalize">
      <Link href={peerHref(row, peer)} isExternal={false}>
        {peerName(row, peer)}
      </Link>
      <MdArrowForward className="w-3.5 h-3.5 text-light-500" />
    </div>
    <div className="flex items-baseline justify-between text-[11px] font-mono">
      <span className="text-light-500">Assays</span>
      <span className="tabular-nums text-light-200">
        {row.n_assays.toLocaleString()}
      </span>
    </div>
    <div className="flex items-baseline justify-between text-[11px] font-mono">
      <span className="text-light-500">Active</span>
      <span className="tabular-nums text-emerald-300">
        {row.n_active_measurements.toLocaleString()}
      </span>
    </div>
    {row.relationships.length > 0 && (
      <div>
        <RelationshipChips relationships={row.relationships} />
      </div>
    )}
  </div>
);

AssayInferredAssociationsTable.displayName = "AssayInferredAssociationsTable";
export default AssayInferredAssociationsTable;
