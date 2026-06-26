// Modal that shows the per-(head, bioactivity) measurement list.
//
// Lazy-fetches the FULL measurement set from /bioactivity/measurements
// when both the anchor entity id and the selected row id are known —
// that endpoint returns Hill-fit fields (zero/infinite/logAC50/slope)
// the MV-nested sample doesn't carry, so we can render a curve sparkline.
// Falls back to `initialMeasurements` (the row's MV-capped sample) when
// the fetch fails or anchor/row ids aren't both provided.
//
// Layout is fixed-height (Modal fullHeight) with three stable zones:
// header (title + toolbar), scrollable rows area, pinned footer
// (pagination). Toolbar / pagination / row slots are ALWAYS rendered —
// when loading, the same shell holds skeleton rows; when on the last
// page or after filtering, placeholder rows pad up to PAGE_SIZE so the
// table doesn't shrink. Net result: the modal opens at its final size
// and stays there — buttons don't migrate, dialog doesn't recenter.

"use client";

import { useEffect, useMemo, useState } from "react";
import {
  MdClose,
  MdInfoOutline,
  MdKeyboardArrowLeft,
  MdKeyboardArrowRight,
  MdKeyboardDoubleArrowLeft,
  MdKeyboardDoubleArrowRight,
  MdSearch,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Button from "@/components/basic/Button";
import LoadingCard from "@/components/basic/LoadingCard";
import Modal from "@/components/basic/Modal";
import HillCurveSparkline from "@/components/entities/bioactivity/HillCurveSparkline";
import { getBioactivityMeasurements } from "@/utils/fetching";
import type {
  BioactivityMeasurement,
  BioactivityMeasurementFull,
} from "@/types";

type ModalRow = Partial<BioactivityMeasurementFull> & BioactivityMeasurement;

const PAGE_SIZE = 20;
const OUTCOME_OPTIONS = ["all", "active", "inactive", "unspecified", "inconclusive"] as const;
type OutcomeFilter = (typeof OUTCOME_OPTIONS)[number];

interface Props {
  isOpen: boolean;
  onClose: () => void;
  headLabel: string;
  tailLabel: string;
  initialMeasurements?: BioactivityMeasurement[] | null;
  expectedCount?: number;
  anchorId?: string | null;
  selectedId?: string | null;
  relationship?: "r5" | "r6";
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

  // Lazy-fetch full measurements on open. Resets on close so a subsequent
  // open re-fetches if the selection changed.
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
      const payload = await getBioactivityMeasurements(headId, tailId, relationship);
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

  const [searchTerm, setSearchTerm] = useState("");
  const [outcomeFilter, setOutcomeFilter] = useState<OutcomeFilter>("all");
  const [currentPage, setCurrentPage] = useState(1);

  // Reset filters/page when the modal closes or the underlying selection
  // changes (different chemical/food clicked).
  useEffect(() => {
    setSearchTerm("");
    setOutcomeFilter("all");
    setCurrentPage(1);
  }, [isOpen, selectedId]);

  // Outcomes computed from the FULL row set (not filtered) so the chip
  // row doesn't reflow when filters apply.
  const availableOutcomes = useMemo<OutcomeFilter[]>(() => {
    const present = new Set<string>();
    rows.forEach((r) => {
      const o = r.outcome?.toLowerCase().trim();
      if (o) present.add(o);
    });
    return OUTCOME_OPTIONS.filter((o) => o === "all" || present.has(o));
  }, [rows]);

  const filtered = useMemo(() => {
    const term = searchTerm.trim().toLowerCase();
    return rows.filter((r) => {
      if (outcomeFilter !== "all") {
        const o = r.outcome?.toLowerCase().trim() ?? "";
        if (o !== outcomeFilter) return false;
      }
      if (term) {
        const haystack = `${r.assay ?? ""} ${r.endpoint ?? ""}`.toLowerCase();
        if (!haystack.includes(term)) return false;
      }
      return true;
    });
  }, [rows, searchTerm, outcomeFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  // Snap to page 1 when filters shrink the result set below the current page.
  useEffect(() => {
    if (currentPage > totalPages) setCurrentPage(1);
  }, [currentPage, totalPages]);

  const visible = useMemo(() => {
    const start = (currentPage - 1) * PAGE_SIZE;
    return filtered.slice(start, start + PAGE_SIZE);
  }, [filtered, currentPage]);

  // Defer the table commit by one paint so the modal animation opens
  // snappily even when the row count is large.
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

  // Total uses the full row count if available, falls back to the
  // upstream-supplied expectedCount (which itself is the per-pair
  // measurement_count from the list endpoint).
  const totalKnown = fullRows?.length ?? expectedCount ?? rows.length;
  const showingFewerThanTotal =
    fullRows == null && expectedCount != null && rows.length < expectedCount;

  const placeholderCount = Math.max(0, PAGE_SIZE - visible.length);
  const showEmptyState = !showSkeleton && filtered.length === 0;

  return (
    <Modal
      title={`Assay measurements · ${headLabel} × ${tailLabel}`}
      isOpen={isOpen}
      onClose={onClose}
      fullHeight
      description={
        <span className="font-mono italic text-xs text-light-400 capitalize">
          {totalKnown.toLocaleString()} measurement
          {totalKnown === 1 ? "" : "s"}
          {filtered.length !== rows.length && (
            <span className="ml-2 not-italic normal-case text-light-600">
              · {filtered.length.toLocaleString()} after filters
            </span>
          )}
          {showingFewerThanTotal && (
            <span className="ml-2 not-italic normal-case text-light-600">
              (showing first {rows.length})
            </span>
          )}
        </span>
      }
      footer={
        <div
          className={twMerge(
            "max-w-xl w-full mx-auto flex items-center justify-between transition-opacity",
            totalPages > 1 ? "opacity-100" : "opacity-0 pointer-events-none"
          )}
          aria-hidden={totalPages <= 1}
        >
          <Button
            isIconOnly
            isSquared
            isDisabled={showSkeleton || currentPage === 1}
            onClick={() => setCurrentPage(1)}
            aria-label="First page"
          >
            <MdKeyboardDoubleArrowLeft />
          </Button>
          <Button
            isIconOnly
            isSquared
            isDisabled={showSkeleton || currentPage === 1}
            onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
            aria-label="Previous page"
          >
            <MdKeyboardArrowLeft />
          </Button>
          <span className="w-40 text-center text-sm text-light-300">
            Page {currentPage} of {totalPages}
          </span>
          <Button
            isIconOnly
            isSquared
            isDisabled={showSkeleton || currentPage === totalPages}
            onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
            aria-label="Next page"
          >
            <MdKeyboardArrowRight />
          </Button>
          <Button
            isIconOnly
            isSquared
            isDisabled={showSkeleton || currentPage === totalPages}
            onClick={() => setCurrentPage(totalPages)}
            aria-label="Last page"
          >
            <MdKeyboardDoubleArrowRight />
          </Button>
        </div>
      }
    >
      {/* Toolbar — ALWAYS rendered (even on skeleton) so its presence
       * isn't a layout-shift trigger. Outcome chips render based on the
       * full row set so they don't pop in/out as filters change. */}
      <div className="mb-4 shrink-0 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="relative flex items-center">
          <MdSearch className="absolute left-2.5 w-5 h-5 text-light-400" />
          <input
            className="pl-9 pr-9 w-full lg:w-72 h-9 text-sm rounded-lg border border-light-50/5 bg-light-900 focus:bg-light-400/20 hover:bg-light-400/20 text-light-100 placeholder-light-400 transition duration-100 ease-in-out outline-light-50/60 disabled:opacity-60"
            type="text"
            placeholder="Search assay or endpoint"
            value={searchTerm}
            disabled={showSkeleton}
            onChange={(e) => {
              setSearchTerm(e.target.value);
              setCurrentPage(1);
            }}
          />
          {searchTerm && (
            <button
              type="button"
              aria-label="Clear search"
              onClick={() => {
                setSearchTerm("");
                setCurrentPage(1);
              }}
              className="absolute right-2 flex items-center justify-center w-5 h-5 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
            >
              <MdClose className="w-3.5 h-3.5" />
            </button>
          )}
        </div>
        {/* Outcome filter row — reserved with a min-height so the row
         * doesn't appear/disappear based on whether multiple outcomes
         * are present. */}
        <div className="min-h-[2rem] flex flex-wrap items-center gap-1.5">
          {availableOutcomes.length > 1 &&
            availableOutcomes.map((opt) => {
              const active = outcomeFilter === opt;
              return (
                <button
                  key={opt}
                  type="button"
                  disabled={showSkeleton}
                  onClick={() => {
                    setOutcomeFilter(opt);
                    setCurrentPage(1);
                  }}
                  aria-pressed={active}
                  className={twMerge(
                    "px-3 py-1 rounded-full border-[1.5px] font-mono italic text-xs capitalize transition-colors disabled:opacity-60",
                    active
                      ? "bg-light-200 text-light-900 border-light-200 font-semibold"
                      : "bg-transparent border-light-700/60 text-light-400 hover:text-light-100 hover:border-light-500"
                  )}
                >
                  {opt}
                </button>
              );
            })}
        </div>
      </div>

      {/* Scroll area — flex-1 so it absorbs the dialog's free space.
       * Inside: the table always renders PAGE_SIZE row slots (data +
       * empty padders), and an empty-state OVERLAY appears on top when
       * the filtered set is empty, so the row scaffolding doesn't
       * collapse. */}
      <div className="flex-1 min-h-0 overflow-y-auto relative">
        <MeasurementsTable
          rows={visible}
          placeholderCount={placeholderCount}
          skeleton={showSkeleton}
        />
        {showEmptyState && (
          <div className="absolute inset-0 flex items-center justify-center bg-light-950/80 text-light-300 gap-2 text-sm pointer-events-none">
            <MdInfoOutline />
            {rows.length === 0
              ? "No measurements recorded for this pair"
              : "No measurements match the current filters"}
          </div>
        )}
      </div>
    </Modal>
  );
};

const MeasurementsTable = ({
  rows,
  placeholderCount,
  skeleton,
}: {
  rows: ModalRow[];
  placeholderCount: number;
  skeleton: boolean;
}) => {
  // When in skeleton mode we draw PAGE_SIZE shimmer rows; otherwise we
  // draw the real rows and pad up to PAGE_SIZE with empty <tr>s so the
  // last-page case doesn't shrink the table height.
  const dataRows = skeleton ? [] : rows;
  const padCount = skeleton ? PAGE_SIZE : placeholderCount;
  return (
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
              <span className="select-none uppercase text-xs font-medium">
                {h}
              </span>
            </th>
          ))}
        </tr>
      </thead>
      <tbody className="text-sm font-light">
        {dataRows.map((m, i) => (
          <tr key={`${m.assay ?? "row"}-${i}`}>
            <td className="py-1.5 pr-2 align-top">
              <div
                className="font-mono text-xs text-light-200 truncate"
                title={m.assay ?? undefined}
              >
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
        {Array.from({ length: padCount }).map((_, i) => (
          <tr key={`pad-${i}`}>
            <td className="py-1.5 pr-2" colSpan={5}>
              {skeleton ? (
                <LoadingCard className="h-5" />
              ) : (
                <div className="h-5" />
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
};

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
