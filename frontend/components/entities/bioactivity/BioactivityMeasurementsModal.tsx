// Modal that shows the per-(head, bioactivity) measurement list.
//
// Today the modal reads from `initialMeasurements` — the row's nested
// measurements array as returned by the list endpoints (currently capped
// at 25 by the materialized view). When /bioactivity/measurements is
// deployed, we can switch to lazy-fetching the full unbounded set via
// getBioactivityMeasurements.

"use client";

import { useEffect, useMemo, useState } from "react";
import { MdInfoOutline } from "react-icons/md";

import LoadingCard from "@/components/basic/LoadingCard";
import Modal from "@/components/basic/Modal";
import type { BioactivityMeasurement } from "@/types";

interface Props {
  isOpen: boolean;
  onClose: () => void;
  headLabel: string;
  tailLabel: string;
  initialMeasurements?: BioactivityMeasurement[] | null;
  expectedCount?: number;
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
}: Props) => {
  const rows = useMemo<BioactivityMeasurement[]>(
    () => initialMeasurements ?? [],
    [initialMeasurements]
  );
  const totalKnown = expectedCount ?? rows.length;
  const showingFewerThanTotal = expectedCount != null && rows.length < expectedCount;

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
        {!isContentReady ? (
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
      <col className="w-[34%]" />
      <col className="w-[20%]" />
      <col className="w-[14%]" />
      <col className="w-[32%]" />
    </colgroup>
    <thead className="text-light-400 text-left">
      <tr>
        {["Assay", "Endpoint", "Outcome", "Value"].map((h) => (
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
          <td className="py-1.5 pr-2" colSpan={4}>
            <LoadingCard className="h-5" />
          </td>
        </tr>
      ))}
    </tbody>
  </table>
);

const MeasurementsTable = ({ rows }: { rows: BioactivityMeasurement[] }) => (
  <table className="w-full table-fixed">
    <colgroup>
      <col className="w-[34%]" />
      <col className="w-[20%]" />
      <col className="w-[14%]" />
      <col className="w-[32%]" />
    </colgroup>
    <thead className="text-light-400 text-left">
      <tr>
        {["Assay", "Endpoint", "Outcome", "Value"].map((h) => (
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
          <td className="py-1.5 pl-2 align-top font-mono text-xs text-light-200 tabular-nums text-right">
            {m.value === null ? (
              <span className="text-light-600">—</span>
            ) : (
              <>
                {formatNumberShort(m.value)}{" "}
                <span className="text-light-500">{m.unit || ""}</span>
              </>
            )}
          </td>
        </tr>
      ))}
    </tbody>
  </table>
);

const OutcomeBadge = ({ outcome }: { outcome: string | null }) => {
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
