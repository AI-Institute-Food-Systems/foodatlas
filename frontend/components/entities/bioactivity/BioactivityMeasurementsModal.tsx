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

import { Fragment, useEffect, useMemo, useState } from "react";
import {
  MdChevronRight,
  MdClose,
  MdInfoOutline,
  MdKeyboardArrowLeft,
  MdKeyboardArrowRight,
  MdKeyboardDoubleArrowLeft,
  MdKeyboardDoubleArrowRight,
  MdSearch,
  MdTune,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Button from "@/components/basic/Button";
import Card from "@/components/basic/Card";
import Link from "@/components/basic/Link";
import LoadingCard from "@/components/basic/LoadingCard";
import Modal from "@/components/basic/Modal";
import HillCurveSparkline from "@/components/entities/bioactivity/HillCurveSparkline";
import { getBioactivityMeasurements } from "@/utils/fetching";
import { assayExternalUrl } from "@/utils/utils";
import type {
  BioactivityMeasurement,
  BioactivityMeasurementFull,
} from "@/types";

type ModalRow = Partial<BioactivityMeasurementFull> & BioactivityMeasurement;

// A row qualifies for the accordion expand only when all four 4PL Hill
// parameters are present. ChEMBL / PubChem rows usually carry just a
// logAC50 + raw value; only ToxCast-style measurements have zero /
// infinite / slope too.
const hasHillFit = (m: ModalRow): boolean => {
  const { efficacy_zeroactivity: z, efficacy_infiniteactivity: inf } = m;
  const { efficacy_logac50_value: lac, efficacy_hillslope: s } = m;
  return [z, inf, lac, s].every(
    (v) => v != null && Number.isFinite(v),
  ) && z !== inf && s !== 0;
};

const rowKey = (m: ModalRow, i: number): string =>
  m.bioactivity_metadata_id ?? `row-${i}`;

const PAGE_SIZE = 20;
const OUTCOME_OPTIONS = ["all", "active", "inactive", "unspecified", "inconclusive"] as const;
type OutcomeFilter = (typeof OUTCOME_OPTIONS)[number];

// Mirrors the big-table sidebar so users see the same three source
// buckets everywhere. Client-side match against measurement.evidence_source
// prefix (backend classifies rows the same way).
const SOURCE_KINDS: { key: string; label: string }[] = [
  { key: "", label: "both" },
  { key: "experimental", label: "experimental" },
  { key: "predicted", label: "predicted" },
];

const matchesSourceKind = (
  source: string | null | undefined,
  kind: string,
): boolean => {
  if (!kind) return true;
  const s = (source ?? "").toLowerCase();
  if (kind === "experimental") return s.startsWith("exp");
  if (kind === "predicted") return s.startsWith("pred") || s.startsWith("comp");
  return true;
};

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
  const [sourceFilter, setSourceFilter] = useState<string>("");
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  // Accordion expansion — at most one row open at a time. Stored by
  // stable row key so re-renders + page changes don't desync it.
  const [expandedKey, setExpandedKey] = useState<string | null>(null);

  // Reset filters/page/expand when the modal closes or the underlying
  // selection changes (different chemical/food clicked).
  useEffect(() => {
    setSearchTerm("");
    setOutcomeFilter("all");
    setSourceFilter("");
    setMobileFiltersOpen(false);
    setCurrentPage(1);
    setExpandedKey(null);
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
      if (sourceFilter && !matchesSourceKind(r.evidence_source, sourceFilter)) {
        return false;
      }
      if (term) {
        const haystack = `${r.assay ?? ""} ${r.endpoint ?? ""}`.toLowerCase();
        if (!haystack.includes(term)) return false;
      }
      return true;
    });
  }, [rows, searchTerm, outcomeFilter, sourceFilter]);

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
      {/* Modal body: same sidebar+table split as the big bioactivity
       * tables — sidebar left at lg+, drawer at sub-lg. Toolbar +
       * skeleton scaffolding still ALWAYS renders so layout doesn't
       * shift on filter changes. */}
      <ModalBody
        searchTerm={searchTerm}
        onSearchChange={(v) => {
          setSearchTerm(v);
          setCurrentPage(1);
        }}
        onSearchClear={() => {
          setSearchTerm("");
          setCurrentPage(1);
        }}
        outcomeFilter={outcomeFilter}
        onOutcomeChange={(o) => {
          setOutcomeFilter(o);
          setCurrentPage(1);
        }}
        availableOutcomes={availableOutcomes}
        sourceFilter={sourceFilter}
        onSourceChange={(s) => {
          setSourceFilter(s);
          setCurrentPage(1);
        }}
        mobileFiltersOpen={mobileFiltersOpen}
        onOpenMobileFilters={() => setMobileFiltersOpen(true)}
        onCloseMobileFilters={() => setMobileFiltersOpen(false)}
        showSkeleton={showSkeleton}
      >
        <MeasurementsTable
          rows={visible}
          placeholderCount={placeholderCount}
          skeleton={showSkeleton}
          expandedKey={expandedKey}
          onToggleExpand={(k) =>
            setExpandedKey((prev) => (prev === k ? null : k))
          }
        />
        {showEmptyState && (
          <div className="absolute inset-0 flex items-center justify-center bg-light-950/80 text-light-300 gap-2 text-sm pointer-events-none">
            <MdInfoOutline />
            {rows.length === 0
              ? "No measurements recorded for this pair"
              : "No measurements match the current filters"}
          </div>
        )}
      </ModalBody>
    </Modal>
  );
};

// Chrome: sidebar (lg+) with search + outcome + source filters, or a
// top bar with search + Filters button that opens a drawer (sub-lg).
// Mirrors BioactivityTable so the modal reads like a small version of
// the big table.
const ModalBody = ({
  searchTerm,
  onSearchChange,
  onSearchClear,
  outcomeFilter,
  onOutcomeChange,
  availableOutcomes,
  sourceFilter,
  onSourceChange,
  mobileFiltersOpen,
  onOpenMobileFilters,
  onCloseMobileFilters,
  showSkeleton,
  children,
}: {
  searchTerm: string;
  onSearchChange: (v: string) => void;
  onSearchClear: () => void;
  outcomeFilter: OutcomeFilter;
  onOutcomeChange: (o: OutcomeFilter) => void;
  availableOutcomes: OutcomeFilter[];
  sourceFilter: string;
  onSourceChange: (s: string) => void;
  mobileFiltersOpen: boolean;
  onOpenMobileFilters: () => void;
  onCloseMobileFilters: () => void;
  showSkeleton: boolean;
  children: React.ReactNode;
}) => {
  const searchInput = (
    <div className="relative flex items-center">
      <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
      <input
        className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none disabled:opacity-60"
        type="text"
        placeholder="Search assay or endpoint"
        aria-label="Search assay or endpoint"
        value={searchTerm}
        disabled={showSkeleton}
        onChange={(e) => onSearchChange(e.target.value)}
      />
      {searchTerm && (
        <button
          type="button"
          aria-label="Clear search"
          onClick={onSearchClear}
          className="absolute right-2 flex items-center justify-center w-4 h-4 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
        >
          <MdClose className="w-3 h-3" />
        </button>
      )}
    </div>
  );

  const outcomeFilterPanel = availableOutcomes.length > 1 && (
    <div className="flex flex-col gap-1.5">
      <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400">
        Outcome
      </span>
      <div
        className="flex flex-col -mx-1"
        role="radiogroup"
        aria-label="Outcome"
      >
        {availableOutcomes.map((opt) => (
          <RadioRow
            key={opt}
            label={opt}
            selected={outcomeFilter === opt}
            disabled={showSkeleton}
            onClick={() => onOutcomeChange(opt)}
          />
        ))}
      </div>
    </div>
  );

  const sourceFilterPanel = (
    <div className="flex flex-col gap-1.5">
      <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400">
        Assay Source
      </span>
      <div
        className="flex flex-col -mx-1"
        role="radiogroup"
        aria-label="Assay Source"
      >
        {SOURCE_KINDS.map(({ key, label }) => (
          <RadioRow
            key={label}
            label={label}
            selected={sourceFilter === key}
            disabled={showSkeleton}
            onClick={() => onSourceChange(key)}
          />
        ))}
      </div>
    </div>
  );

  const filtersOnlyPanel = (
    <div className="flex flex-col gap-5">
      {outcomeFilterPanel}
      {sourceFilterPanel}
    </div>
  );

  return (
    <div className="flex-1 min-h-0 flex flex-col lg:flex-row lg:gap-6">
      {/* Desktop sidebar */}
      <aside className="hidden lg:block w-48 shrink-0">
        <Card className="px-4 py-4 gap-5">
          {searchInput}
          {filtersOnlyPanel}
        </Card>
      </aside>

      {/* Right column: sub-lg top bar + scroll area */}
      <div className="flex-1 min-w-0 flex flex-col">
        <div className="lg:hidden mb-4 shrink-0 flex items-center gap-3">
          <div className="flex-1 min-w-0 max-w-xs">{searchInput}</div>
          <button
            type="button"
            onClick={onOpenMobileFilters}
            className="inline-flex items-center gap-2 rounded-md border border-light-700/60 bg-light-900/60 px-3 py-1.5 text-xs font-mono italic text-light-300 hover:text-light-100 hover:border-light-500 transition-colors"
          >
            <MdTune className="w-4 h-4" />
            Filters
          </button>
        </div>

        <div className="flex-1 min-h-0 overflow-y-auto relative">
          {children}
        </div>
      </div>

      {mobileFiltersOpen && (
        <div
          className="fixed inset-0 z-50 lg:hidden"
          role="dialog"
          aria-modal="true"
          aria-label="Filters"
        >
          <button
            type="button"
            aria-label="Close filters"
            onClick={onCloseMobileFilters}
            className="absolute inset-0 bg-black/60 cursor-default"
          />
          <aside className="absolute right-0 top-0 h-full w-[85vw] max-w-sm bg-light-950 border-l border-light-700/50 overflow-y-auto flex flex-col gap-4 p-4">
            <div className="flex items-center justify-between">
              <span className="font-mono italic text-sm text-light-300">
                Filters
              </span>
              <button
                type="button"
                aria-label="Close filters"
                onClick={onCloseMobileFilters}
                className="w-8 h-8 rounded-full flex items-center justify-center text-light-400 hover:text-light-100 hover:bg-light-800 transition-colors"
              >
                <MdClose className="w-4 h-4" />
              </button>
            </div>
            {filtersOnlyPanel}
          </aside>
        </div>
      )}
    </div>
  );
};

const RadioRow = ({
  label,
  selected,
  disabled,
  onClick,
}: {
  label: string;
  selected: boolean;
  disabled?: boolean;
  onClick: () => void;
}) => (
  <button
    type="button"
    role="radio"
    aria-checked={selected}
    disabled={disabled}
    onClick={onClick}
    className={twMerge(
      "group w-full flex items-center gap-2 pl-1 pr-2 py-1 rounded transition-colors text-left disabled:opacity-60",
      selected
        ? "text-light-100 hover:bg-light-900/70"
        : "text-light-400 hover:text-light-100 hover:bg-light-900/50",
    )}
  >
    <span
      aria-hidden
      className={twMerge(
        "w-3.5 h-3.5 rounded-full border flex-shrink-0 flex items-center justify-center transition-colors",
        selected
          ? "border-accent-600 bg-accent-600/20"
          : "border-light-700 group-hover:border-light-500",
      )}
    >
      {selected && (
        <span className="w-1.5 h-1.5 rounded-full bg-accent-600" aria-hidden />
      )}
    </span>
    <span className="font-mono italic text-xs capitalize flex-1">
      {label}
    </span>
  </button>
);

const MeasurementsTable = ({
  rows,
  placeholderCount,
  skeleton,
  expandedKey,
  onToggleExpand,
}: {
  rows: ModalRow[];
  placeholderCount: number;
  skeleton: boolean;
  expandedKey: string | null;
  onToggleExpand: (key: string) => void;
}) => {
  // When in skeleton mode we draw PAGE_SIZE shimmer rows; otherwise we
  // draw the real rows and pad up to PAGE_SIZE with empty <tr>s so the
  // last-page case doesn't shrink the table height.
  const dataRows = skeleton ? [] : rows;
  // Each expanded row takes one extra slot — reduce pads to keep the
  // total slot count visually stable.
  const expandedInView = skeleton
    ? 0
    : dataRows.some((m, i) => rowKey(m, i) === expandedKey)
      ? 1
      : 0;
  const padCount = skeleton
    ? PAGE_SIZE
    : Math.max(0, placeholderCount - expandedInView);
  return (
    <table className="w-full table-fixed">
      <colgroup>
        <col className="w-[28%]" />
        <col className="w-[16%]" />
        <col className="w-[10%]" />
        <col className="w-[12%]" />
        <col className="w-[34%]" />
      </colgroup>
      <thead className="text-light-400 text-left sticky top-0 z-10 bg-light-950">
        <tr>
          {["Assay", "Endpoint", "Outcome", "Source", "Value"].map(
            (h, idx) => (
              <th
                key={h}
                className={twMerge(
                  "h-9 border-b border-light-700 leading-none py-1.5 px-2 first:pl-0 last:pr-0 bg-light-950",
                  idx === 4 && "text-right",
                )}
              >
                <span className="select-none uppercase text-xs font-medium">
                  {h}
                </span>
              </th>
            )
          )}
        </tr>
      </thead>
      <tbody className="text-sm font-light">
        {dataRows.map((m, i) => {
          const key = rowKey(m, i);
          const canExpand = hasHillFit(m);
          const isExpanded = expandedKey === key;
          return (
            <Fragment key={key}>
              <tr
                onClick={canExpand ? () => onToggleExpand(key) : undefined}
                aria-expanded={canExpand ? isExpanded : undefined}
                className={twMerge(
                  "transition-colors",
                  canExpand && "cursor-pointer hover:bg-light-900/50",
                  isExpanded && "bg-light-900/50",
                )}
              >
                <td className="py-1.5 pr-2 align-top">
                  <AssayCell assay={m.assay} />
                </td>
                <td className="py-1.5 px-2 align-top text-light-200">
                  {m.endpoint || "—"}
                </td>
                <td className="py-1.5 px-2 align-top">
                  <OutcomeBadge outcome={m.outcome} />
                </td>
                <td className="py-1.5 px-2 align-top">
                  <SourceBadge source={m.evidence_source} />
                </td>
                <td className="py-1.5 pl-2 align-top">
                  <div className="flex items-center justify-between gap-3">
                    {canExpand ? (
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          onToggleExpand(key);
                        }}
                        aria-label={
                          isExpanded
                            ? "Hide Hill curve"
                            : "Show Hill curve"
                        }
                        className={twMerge(
                          "inline-flex items-center gap-1 px-2 py-0.5 rounded-full border font-mono italic text-xs whitespace-nowrap transition-colors",
                          isExpanded
                            ? "border-accent-600/60 bg-accent-600/10 text-accent-600"
                            : "border-light-700/60 text-light-400 hover:text-light-100 hover:border-light-500",
                        )}
                      >
                        <span>
                          {isExpanded ? "Hide" : "Show"} Hill Curve
                        </span>
                        <MdChevronRight
                          className={twMerge(
                            "transition-transform duration-150",
                            isExpanded && "rotate-90",
                          )}
                        />
                      </button>
                    ) : (
                      <span aria-hidden />
                    )}
                    <span className="font-mono text-xs text-light-200 tabular-nums text-right">
                      {m.value === null || m.value === undefined ? (
                        <span className="text-light-600">—</span>
                      ) : (
                        <>
                          {formatNumberShort(m.value)}{" "}
                          <span className="text-light-500">
                            {m.unit && m.unit !== "None" ? m.unit : ""}
                          </span>
                        </>
                      )}
                    </span>
                  </div>
                </td>
              </tr>
              {isExpanded && (
                <tr>
                  <td
                    colSpan={5}
                    className="py-3 px-3 bg-light-900/30 border-l-2 border-l-accent-600 border-b border-light-700/40"
                  >
                    <ExpandedHillFit m={m} />
                  </td>
                </tr>
              )}
            </Fragment>
          );
        })}
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

// Inline expanded view for one (assay × bioactivity) measurement —
// shown below the row when its accordion is open. Large Hill curve on
// the left, four-parameter fit metadata on the right. Only rendered
// for rows that pass `hasHillFit`, so all four numbers are guaranteed
// finite here.
const ExpandedHillFit = ({ m }: { m: ModalRow }) => {
  const lac = m.efficacy_logac50_value;
  const ac50 = lac != null ? 10 ** lac : null;
  const fmtNum = (v: number | null | undefined, digits = 2): string =>
    v == null || !Number.isFinite(v) ? "—" : v.toFixed(digits);
  const fmtSig = (v: number | null | undefined): string =>
    v == null || !Number.isFinite(v)
      ? "—"
      : v.toLocaleString(undefined, { maximumSignificantDigits: 3 });
  const unitLabel = m.unit && m.unit !== "None" ? m.unit : "";
  return (
    <div className="flex flex-col md:flex-row gap-6 items-stretch w-full">
      {/* Important values — left, fixed-width column for fast scan. */}
      <dl className="md:w-48 flex-shrink-0 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs font-mono content-start">
        <dt className="italic uppercase text-light-500">AC50</dt>
        <dd className="text-light-100 tabular-nums">
          {fmtSig(ac50)}
          {unitLabel && <span className="text-light-500"> {unitLabel}</span>}
        </dd>
        <dt className="italic uppercase text-light-500">log AC50</dt>
        <dd className="text-light-100 tabular-nums">{fmtNum(lac, 3)}</dd>
        <dt className="italic uppercase text-light-500">Hill slope</dt>
        <dd className="text-light-100 tabular-nums">
          {fmtNum(m.efficacy_hillslope, 2)}
        </dd>
        <dt className="italic uppercase text-light-500">top</dt>
        <dd className="text-light-100 tabular-nums">
          {fmtNum(m.efficacy_infiniteactivity, 1)}
        </dd>
        <dt className="italic uppercase text-light-500">bottom</dt>
        <dd className="text-light-100 tabular-nums">
          {fmtNum(m.efficacy_zeroactivity, 1)}
        </dd>
        {m.evidence_fit_r2 != null && (
          <>
            <dt className="italic uppercase text-light-500">R²</dt>
            <dd className="text-light-100 tabular-nums">
              {fmtNum(m.evidence_fit_r2, 3)}
            </dd>
          </>
        )}
        {m.evidence_fit_curveclass && (
          <>
            <dt className="italic uppercase text-light-500">curve class</dt>
            <dd className="text-light-100">{m.evidence_fit_curveclass}</dd>
          </>
        )}
      </dl>
      {/* Curve — right, fills remaining horizontal space. Wrapper has
       * an explicit aspect ratio matching the viewBox so the
       * preserveAspectRatio default (meet) doesn't letterbox. */}
      <div
        className="flex-1 min-w-0 text-light-300"
        style={{ aspectRatio: "720 / 320" }}
      >
        <HillCurveSparkline
          zero={m.efficacy_zeroactivity}
          infinite={m.efficacy_infiniteactivity}
          logAC50={m.efficacy_logac50_value}
          slope={m.efficacy_hillslope}
          unit={m.unit}
          width={720}
          height={320}
          fluid
        />
      </div>
    </div>
  );
};

// Assay id cell — a raw identifier ("AID: 364", "CHEMBL329341") that the
// helper turns into a landing-page URL when we recognise the scheme.
// External-link rendering matches the OverviewCardCatalog convention
// (font-mono label wrapped in <Link isExternal>) so entities feel
// consistent across the site. Unknown schemes render as plain text.
const AssayCell = ({ assay }: { assay: string | null | undefined }) => {
  if (!assay) return <span className="text-light-600">—</span>;
  const ext = assayExternalUrl(assay);
  if (!ext) {
    return (
      <div
        className="font-mono text-xs text-light-200 truncate"
        title={assay}
      >
        {assay}
      </div>
    );
  }
  return (
    <div className="truncate" title={`${assay} — open on ${ext.source}`}>
      <Link href={ext.url} isExternal>
        <span className="font-mono text-xs">{assay}</span>
      </Link>
    </div>
  );
};

// Distinguishes Experimental measurements from Predicted / computational
// ones — the staging snapshot is all Experimental today but the field is
// in base_attestations_bioactivity and will populate once Predicted rows
// land. Stays mute (em-dash) for blank/unknown values.
const SourceBadge = ({
  source,
}: {
  source: string | null | undefined;
}) => {
  if (!source) return <span className="text-light-600">—</span>;
  const lc = source.toLowerCase();
  const tone = lc.startsWith("exp")
    ? "border-light-700/60 bg-light-900/40 text-light-300"
    : lc.startsWith("pred") || lc.startsWith("comp")
      ? "border-amber-500/40 bg-amber-500/10 text-amber-200"
      : "border-light-700/60 bg-light-900/40 text-light-400";
  return (
    <span
      className={`inline-block capitalize text-[10px] leading-tight px-2 py-0.5 rounded-full border ${tone}`}
      title={source}
    >
      {source}
    </span>
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
