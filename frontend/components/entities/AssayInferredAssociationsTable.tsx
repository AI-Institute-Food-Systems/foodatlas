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
// Rows are ordered by n_assays desc server-side. The endpoint isn't
// paginated, so filtering and paging both happen in memory — every row is
// already here. Paging rather than a "show all" button because that is
// what every other table on these pages does, including the literature
// table directly above this one, and because the tail is long: caffeine
// alone has 89 associations, obesity 1,693.

import { useEffect, useMemo, useRef, useState } from "react";

import Pagination from "@/components/basic/Pagination";
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
import { usePaginations } from "@/context/paginationsContext";
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
  { key: "peer", width: "w-[30%]" },
  { key: "signal", width: "w-[26%]" },
  { key: "target", width: "w-[22%]" },
  { key: "evidence", width: "w-[22%]" },
];

const ROWS_PER_PAGE = 20;

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
  externalSearch = "",
  onTotalRowsChange,
}: Props) => {
  const [rows, setRows] = useState<AssayInferredAssociation[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  // Per-direction id: the chemical and disease pages each render one of
  // these, and a shared id would make them share a page number.
  const tableId = `assay-inferred-${peer}-${commonName}`;
  const { getTablePaginations, setTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations(tableId);
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

  const totalPages = Math.max(
    1,
    Math.ceil(filtered.length / ROWS_PER_PAGE)
  );
  const visible = useMemo(
    () =>
      filtered.slice(
        (currentPage - 1) * ROWS_PER_PAGE,
        currentPage * ROWS_PER_PAGE
      ),
    [filtered, currentPage]
  );

  // Filtering to fewer pages while on a later one slices past the end and
  // renders the empty state over real rows, with the paginator unmounted
  // and no way back. Guarded on !isLoading because rows are empty on the
  // first render, when an unguarded clamp would discard a persisted page.
  useEffect(() => {
    if (!isLoading && currentPage > totalPages) {
      setTablePaginations(tableId, 1, ROWS_PER_PAGE);
    }
  }, [isLoading, currentPage, totalPages, tableId, setTablePaginations]);

  // A new search should land on its best matches, not on page 4 of them.
  // Ref-compared rather than a bare effect so mounting doesn't reset a
  // page the user navigated to.
  const lastSearch = useRef(externalSearch);
  useEffect(() => {
    if (lastSearch.current === externalSearch) return;
    lastSearch.current = externalSearch;
    setTablePaginations(tableId, 1, ROWS_PER_PAGE);
  }, [externalSearch, tableId, setTablePaginations]);

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
            <col className="w-[30%]" />
            <col className="w-[26%]" />
            <col className="w-[22%]" />
            <col className="w-[22%]" />
          </colgroup>
          <thead className="text-light-400 text-left">
            <tr>
              <Th>{peerLabel}</Th>
              <Th title="How CTD classifies the link: therapeutic (treats) or marker/mechanism (marks or drives). Opposite directions.">
                Signal
              </Th>
              <Th title="The protein target the bridging assays measure — what the association runs through">
                Target
              </Th>
              <Th title="The source assays behind this association, and how many">
                Assays
              </Th>
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

      {(totalPages > 1 || isLoading) && (
        <div className="mt-2 max-w-xl w-full mx-auto">
          <Pagination
            tableId={tableId}
            numberOfPages={totalPages}
            isLoading={isLoading}
          />
        </div>
      )}
    </div>
  );
};

AssayInferredAssociationsTable.displayName = "AssayInferredAssociationsTable";
export default AssayInferredAssociationsTable;
