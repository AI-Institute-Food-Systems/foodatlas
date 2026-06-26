// Modal that shows the per-(head, bioactivity) measurement list.
//
// Lazy-fetches the FULL measurement set from /bioactivity/measurements
// when both the anchor entity id and the selected row id are known —
// that endpoint returns Hill-fit fields (zero/infinite/logAC50/slope)
// the MV-nested sample doesn't carry, so we can render a curve sparkline.
// Falls back to `initialMeasurements` (the row's MV-capped sample) when
// the fetch fails or anchor/row ids aren't both provided.

"use client";

import { useEffect, useMemo, useState } from "react";
import { MdInfoOutline } from "react-icons/md";

import LoadingCard from "@/components/basic/LoadingCard";
import Modal from "@/components/basic/Modal";
import HillCurveSparkline from "@/components/entities/bioactivity/HillCurveSparkline";
import { getBioactivityMeasurements } from "@/utils/fetching";
import type {
  BioactivityMeasurement,
  BioactivityMeasurementFull,
} from "@/types";

// Union row used by the modal — we render whichever subset of fields the
// row actually carries. Full rows pick up the Hill-curve sparkline.
type ModalRow = Partial<BioactivityMeasurementFull> & BioactivityMeasurement;

interface Props {
  isOpen: boolean;
  onClose: () => void;
  headLabel: string;
  tailLabel: string;
  initialMeasurements?: BioactivityMeasurement[] | null;
  expectedCount?: number;
  // When both are provided we lazy-fetch the full measurement set on open.
  anchorId?: string | null;
  selectedId?: string | null;
  relationship?: "r5" | "r6";
  // For r5/r6 the head depends on direction; the section tells us which
  // side the table-row corresponds to (anchor vs row).
  headIsRow?: boolean;
}

const formatNumberShort = (n: number): string =>
  n.toLocaleString(undefined, { maximumSignificantDigits: 3 });

const BioactivityMeasurementsModal = ({
  isOpen,
  onClose,
  headLabel,
  tailLabel,
  initialMeasurements,
  expectedCount,
  anchorId,
  selectedId,
  relationship,
  headIsRow,
}: Props) => {
  const [fullRows, setFullRows] = useState<ModalRow[] | null>(null);
  const [isFetching, setIsFetching] = useState(false);

  // Lazy-fetch full measurements when opened. Resets on close so a
  // subsequent open re-fetches if the selection changed.
  useEffect(() => {
    if (!isOpen) {
      setFullRows(null);
      setIsFetching(false);
      return;
    }
    if (!anchorId || !selectedId || !relationship) return;
    let cancelled = false;
    setIsFetching(true);
    const headId = headIsRow ? selectedId : anchorId;
    const tailId = headIsRow ? anchorId : selectedId;
    (async () => {
      const payload = await getBioactivityMeasurements(
        headId,
        tailId,
        relationship
      );
      if (cancelled) return;
      const data = (payload?.data as BioactivityMeasurementFull[] | undefined) ?? null;
      setFullRows(data && data.length ? data : null);
      setIsFetching(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [isOpen, anchorId, selectedId, relationship, headIsRow]);

  const rows = useMemo<ModalRow[]>(
    () => fullRows ?? initialMeasurements ?? [],
    [fullRows, initialMeasurements]
  );
  const totalKnown = fullRows?.length ?? expectedCount ?? rows.length;
  const showingFewerThanTotal =
    fullRows == null && expectedCount != null && rows.length < expectedCount;

  // Defer rendering the (potentially long) measurements table by one paint
  // so the modal animation opens snappily and the user sees a skeleton in
  // place of a frozen UI while React commits the rows. Resets when closed
  // so the next open also gets the spinner.
  const [isContentReady, setIsContentReady] = useState(false);
  useEffect(() => {
    if (!isOpen) {
      setIsContentReady(false);
      return;
    }
    const raf = requestAnimationFrame(() => setIsContentReady(true));
    return () => cancelAnimationFrame(raf);
  }, [isOpen]);

  const showSkeleton = !isContentReady || (isFetching && rows.length === 0);

  return (
    <Modal
      title={`${headLabel} × ${tailLabel}`}
      isOpen={isOpen}
      onClose={onClose}
      description={
        <span className="font-mono italic text-xs text-light-400 capitalize">
          {totalKnown.toLocaleString()} measurement
          {totalKnown === 1 ? "" : "s"}
          {showingFewerThanTotal && (
            <span className="ml-2 not-italic normal-case text-light-600">
              (showing first {rows.length})
            </span>
          )}
        </span>
      }
    >
      <div className="mt-4 max-h-[70vh] overflow-y-auto">
        {showSkeleton ? (
          <MeasurementsSkeleton rowCount={Math.min(rows.length || 6, 8)} />
        ) : rows.length === 0 ? (
          <div className="h-32 flex items-center justify-center text-light-300 gap-2 text-sm">
            <MdInfoOutline /> No measurements recorded for this pair
          </div>
        ) : (
          <MeasurementsTable rows={rows} />
        )}
      </div>
    </Modal>
  );
};

const MeasurementsSkeleton = ({ rowCount }: { rowCount: number }) => (
  <table className="w-full table-fixed">
    <colgroup>
      <col className="w-[28%]" />
      <col className="w-[16%]" />
      <col className="w-[12%]" />
      <col className="w-[22%]" />
      <col className="w-[22%]" />
    </colgroup>
    <thead className="text-light-400 text-left">
      <tr>
        {["Assay", "Endpoint", "Outcome", "Value", "Curve"].map((h) => (
          <th
            key={h}
            className="h-9 border-b border-light-700 leading-none py-1.5 px-2 first:pl-0 last:pr-0"
          >
            <span className="select-none uppercase text-xs font-medium">{h}</span>
          </th>
        ))}
      </tr>
    </thead>
    <tbody>
      {Array.from({ length: rowCount }).map((_, i) => (
        <tr key={i}>
          <td className="py-1.5 pr-2" colSpan={5}>
            <LoadingCard className="h-5" />
          </td>
        </tr>
      ))}
    </tbody>
  </table>
);

const MeasurementsTable = ({ rows }: { rows: ModalRow[] }) => (
  <table className="w-full table-fixed">
    <colgroup>
      <col className="w-[28%]" />
      <col className="w-[16%]" />
      <col className="w-[12%]" />
      <col className="w-[22%]" />
      <col className="w-[22%]" />
    </colgroup>
    <thead className="text-light-400 text-left">
      <tr>
        {["Assay", "Endpoint", "Outcome", "Value", "Curve"].map((h) => (
          <th
            key={h}
            className="h-9 border-b border-light-700 leading-none py-1.5 px-2 first:pl-0 last:pr-0"
          >
            <span className="select-none uppercase text-xs font-medium">{h}</span>
          </th>
        ))}
      </tr>
    </thead>
    <tbody className="text-sm font-light">
      {rows.map((m, i) => (
        <tr key={`${m.assay ?? "row"}-${i}`}>
          <td className="py-1.5 pr-2 align-top">
            <div className="font-mono text-xs text-light-200 truncate" title={m.assay ?? undefined}>
              {m.assay ?? "—"}
            </div>
          </td>
          <td className="py-1.5 px-2 align-top text-light-200">
            {m.endpoint || "—"}
          </td>
          <td className="py-1.5 px-2 align-top">
            <OutcomeBadge outcome={m.outcome} />
          </td>
          <td className="py-1.5 px-2 align-top font-mono text-xs text-light-200 tabular-nums text-right">
            {m.value === null || m.value === undefined ? (
              <span className="text-light-600">—</span>
            ) : (
              <>
                {formatNumberShort(m.value)}{" "}
                <span className="text-light-500">{m.unit || ""}</span>
              </>
            )}
          </td>
          <td className="py-1.5 pl-2 align-top text-light-300">
            <HillCurveSparkline
              zero={m.efficacy_zeroactivity}
              infinite={m.efficacy_infiniteactivity}
              logAC50={m.efficacy_logac50_value}
              slope={m.efficacy_hillslope}
            />
          </td>
        </tr>
      ))}
    </tbody>
  </table>
);

const OutcomeBadge = ({ outcome }: { outcome: string | null | undefined }) => {
  if (!outcome) return <span className="text-light-600">—</span>;
  const lc = outcome.toLowerCase();
  const tone =
    lc === "active"
      ? "border-emerald-600/40 bg-emerald-600/10 text-emerald-300"
      : lc === "inactive"
        ? "border-light-700/60 bg-light-900/40 text-light-400"
        : "border-amber-500/40 bg-amber-500/10 text-amber-200";
  return (
    <span
      className={`inline-block capitalize text-[10px] leading-tight px-2 py-0.5 rounded-full border ${tone}`}
    >
      {outcome}
    </span>
  );
};

BioactivityMeasurementsModal.displayName = "BioactivityMeasurementsModal";

export default BioactivityMeasurementsModal;
