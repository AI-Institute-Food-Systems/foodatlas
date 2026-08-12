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
  MdCheck,
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
import Chip from "@/components/basic/Chip";
import Link from "@/components/basic/Link";
import LoadingCard from "@/components/basic/LoadingCard";
import Modal from "@/components/basic/Modal";
import { useReportRows } from "@/context/reportModeContext";
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
  { key: "", label: "All" },
  { key: "experimental", label: "Experimental" },
  { key: "predicted", label: "Predicted" },
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
  // When rendered from the food-inferred-bioactivities table, this is
  // /food/efficacy's n_curves (records with a fittable AC50 — the ones
  // that contributed to the row's efficacy metric). Not every assay in
  // `rows` contributed (MIC-only rows have no logac50), so we surface
  // the delta so users aren't misled. Undefined for other call sites.
  contributedCount?: number;
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
  contributedCount,
  anchorId,
  selectedId,
  relationship,
  headIsRow,
}: Props) => {
  const [fullRows, setFullRows] = useState<ModalRow[] | null>(null);
  const [isFetching, setIsFetching] = useState(false);
  const reporter = useReportRows();

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
  // Multi-select evidence-type filter. Values are whatever the backend
  // labels rows with (NPASS-style: molecular-level / in vitro / in vivo
  // / adme-tox). Empty array = no filter (show all).
  const [evidenceTypeFilter, setEvidenceTypeFilter] = useState<string[]>([]);
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
    setEvidenceTypeFilter([]);
    setMobileFiltersOpen(false);
    setCurrentPage(1);
    setExpandedKey(null);
  }, [isOpen, selectedId]);

  // Faceted counts — each dimension applies every OTHER active filter
  // (excluding its own) so the numbers stay in sync with what the modal
  // would render under each selection. Kills the "count says 5 but
  // filter changes row count by 3" mismatch the composition sidebar had
  // before we made its counts faceted server-side.
  const outcomeCounts = useMemo<Record<OutcomeFilter, number>>(() => {
    const q = searchTerm.trim().toLowerCase();
    const counts: Record<OutcomeFilter, number> = {
      all: 0,
      active: 0,
      inactive: 0,
      unspecified: 0,
      inconclusive: 0,
    };
    // Apply source + search; exclude outcome. Then group by outcome.
    rows.forEach((r) => {
      if (sourceFilter && !matchesSourceKind(r.evidence_source, sourceFilter)) {
        return;
      }
      if (
        q &&
        !`${r.assay ?? ""} ${r.endpoint ?? ""}`.toLowerCase().includes(q)
      ) {
        return;
      }
      counts.all += 1;
      const o = r.outcome?.toLowerCase().trim() as OutcomeFilter | undefined;
      if (o && o in counts && o !== "all") counts[o] += 1;
    });
    return counts;
  }, [rows, sourceFilter, searchTerm]);

  const sourceKindCounts = useMemo<Record<string, number>>(() => {
    const q = searchTerm.trim().toLowerCase();
    const counts: Record<string, number> = {
      "": 0,
      experimental: 0,
      predicted: 0,
    };
    // Apply outcome + search; exclude source_kind. Then group by kind.
    rows.forEach((r) => {
      if (outcomeFilter !== "all") {
        const o = r.outcome?.toLowerCase().trim() ?? "";
        if (o !== outcomeFilter) return;
      }
      if (
        q &&
        !`${r.assay ?? ""} ${r.endpoint ?? ""}`.toLowerCase().includes(q)
      ) {
        return;
      }
      counts[""] += 1;
      if (matchesSourceKind(r.evidence_source, "experimental")) {
        counts.experimental += 1;
      }
      if (matchesSourceKind(r.evidence_source, "predicted")) {
        counts.predicted += 1;
      }
    });
    return counts;
  }, [rows, outcomeFilter, searchTerm]);

  // Per-evidence_type row counts derived from the full row set. Sorted
  // by count desc so the biggest bucket surfaces first — same ordering
  // as the sidebar chip counts on the big BioactivityTable.
  const evidenceTypeOptions = useMemo<
    { evidence_type: string; count: number }[]
  >(() => {
    const counts = new Map<string, number>();
    rows.forEach((r) => {
      const et = (r.evidence_type ?? "").trim();
      if (!et) return;
      counts.set(et, (counts.get(et) ?? 0) + 1);
    });
    return Array.from(counts.entries())
      .map(([evidence_type, count]) => ({ evidence_type, count }))
      .sort((a, b) => b.count - a.count);
  }, [rows]);

  const filtered = useMemo(() => {
    const term = searchTerm.trim().toLowerCase();
    const wantedEvidence = new Set(evidenceTypeFilter);
    return rows.filter((r) => {
      if (outcomeFilter !== "all") {
        const o = r.outcome?.toLowerCase().trim() ?? "";
        if (o !== outcomeFilter) return false;
      }
      if (sourceFilter && !matchesSourceKind(r.evidence_source, sourceFilter)) {
        return false;
      }
      if (wantedEvidence.size > 0) {
        const et = (r.evidence_type ?? "").trim();
        if (!wantedEvidence.has(et)) return false;
      }
      if (term) {
        const haystack = `${r.assay ?? ""} ${r.endpoint ?? ""}`.toLowerCase();
        if (!haystack.includes(term)) return false;
      }
      return true;
    });
  }, [rows, searchTerm, outcomeFilter, sourceFilter, evidenceTypeFilter]);

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

  // Sidebar chrome mirrors BioactivityTable — cream Card hanging off the
  // modal's left edge at min-[1440px]. Below that the sidebar is hidden
  // and the modal body renders a "search + Filters" top bar + drawer.
  const searchInput = (
    <SearchInput
      value={searchTerm}
      disabled={showSkeleton}
      onChange={(v) => {
        setSearchTerm(v);
        setCurrentPage(1);
      }}
      onClear={() => {
        setSearchTerm("");
        setCurrentPage(1);
      }}
    />
  );
  const toggleEvidenceType = (etype: string) => {
    setEvidenceTypeFilter((prev) =>
      prev.includes(etype) ? prev.filter((e) => e !== etype) : [...prev, etype]
    );
    setCurrentPage(1);
  };
  const clearEvidenceTypes = () => {
    setEvidenceTypeFilter([]);
    setCurrentPage(1);
  };
  const filtersOnlyPanel = (
    <FiltersOnlyPanel
      outcomeFilter={outcomeFilter}
      outcomeCounts={outcomeCounts}
      sourceKindCounts={sourceKindCounts}
      onOutcomeChange={(o) => {
        setOutcomeFilter(o);
        setCurrentPage(1);
      }}
      sourceFilter={sourceFilter}
      onSourceChange={(s) => {
        setSourceFilter(s);
        setCurrentPage(1);
      }}
      evidenceTypeOptions={evidenceTypeOptions}
      selectedEvidenceTypes={evidenceTypeFilter}
      onToggleEvidenceType={toggleEvidenceType}
      onClearEvidenceTypes={clearEvidenceTypes}
      showSkeleton={showSkeleton}
    />
  );

  return (
    <Modal
      title={`Assay measurements · ${headLabel} × ${tailLabel}`}
      isOpen={isOpen}
      onClose={onClose}
      fullHeight
      sidebar={
        <Card className="px-4 py-4 gap-5">
          {searchInput}
          {filtersOnlyPanel}
        </Card>
      }
      description={
        <span className="font-mono italic text-xs text-light-400 capitalize">
          {totalKnown.toLocaleString()} measurement
          {totalKnown === 1 ? "" : "s"}
          {contributedCount != null && contributedCount < totalKnown && (
            <span className="ml-2 not-italic normal-case text-light-600">
              · {contributedCount.toLocaleString()} contributed to efficacy
            </span>
          )}
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
      {/* Sub-1440px top bar: search + Filters button — hidden at
       * min-[1440px] where the sidebar (outside the panel) carries
       * these controls instead. */}
      <div className="min-[1440px]:hidden mb-4 shrink-0 flex items-center gap-3">
        <div className="flex-1 min-w-0 max-w-xs">{searchInput}</div>
        <button
          type="button"
          onClick={() => setMobileFiltersOpen(true)}
          className="inline-flex items-center gap-2 rounded-md border border-light-700/60 bg-light-900/60 px-3 py-1.5 text-xs font-mono italic text-light-300 hover:text-light-100 hover:border-light-500 transition-colors"
        >
          <MdTune className="w-4 h-4" />
          Filters
        </button>
      </div>

      {/* Scroll area — the row scaffolding pads out to PAGE_SIZE so
       * last-page + filtered-empty cases don't shrink the table. */}
      <div className="flex-1 min-h-0 overflow-y-auto relative">
        <MeasurementsTable
          rows={visible}
          placeholderCount={placeholderCount}
          skeleton={showSkeleton}
          expandedKey={expandedKey}
          onToggleExpand={(k) =>
            setExpandedKey((prev) => (prev === k ? null : k))
          }
          getRowReportProps={(m) =>
            reporter.getRowProps({
              kind: "bioactivity-measurement",
              entityType: "bioactivity",
              bioactivityId: m.bioactivity_metadata_id,
              bioactivityName: headIsRow ? headLabel : tailLabel,
              assay: m.assay ?? undefined,
              endpoint: m.endpoint ?? undefined,
              outcome: m.outcome ?? undefined,
              value:
                typeof m.value === "number" ? String(m.value) : undefined,
              unit: m.unit ?? undefined,
            })
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
      </div>

      {/* Sub-1440px filter drawer. */}
      {mobileFiltersOpen && (
        <div
          className="fixed inset-0 z-[60] min-[1440px]:hidden"
          role="dialog"
          aria-modal="true"
          aria-label="Filters"
        >
          <button
            type="button"
            aria-label="Close filters"
            onClick={() => setMobileFiltersOpen(false)}
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
                onClick={() => setMobileFiltersOpen(false)}
                className="w-8 h-8 rounded-full flex items-center justify-center text-light-400 hover:text-light-100 hover:bg-light-800 transition-colors"
              >
                <MdClose className="w-4 h-4" />
              </button>
            </div>
            {filtersOnlyPanel}
          </aside>
        </div>
      )}
    </Modal>
  );
};

// Reused inside the modal's outside sidebar, the sub-1440px top bar,
// and the drawer.
const SearchInput = ({
  value,
  disabled,
  onChange,
  onClear,
}: {
  value: string;
  disabled?: boolean;
  onChange: (v: string) => void;
  onClear: () => void;
}) => (
  <div className="relative flex items-center">
    <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
    <input
      className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none disabled:opacity-60"
      type="text"
      placeholder="Search assay or endpoint"
      aria-label="Search assay or endpoint"
      value={value}
      disabled={disabled}
      onChange={(e) => onChange(e.target.value)}
    />
    {value && (
      <button
        type="button"
        aria-label="Clear search"
        onClick={onClear}
        className="absolute right-2 flex items-center justify-center w-4 h-4 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
      >
        <MdClose className="w-3 h-3" />
      </button>
    )}
  </div>
);

// Non-search filters — Outcome + Evidence + Assay Source. Renders
// every option (including zero-count ones) so the filter chrome stays
// stable; each row's count reflects the FULL row set. Rendered inside
// the sidebar (min-[1440px]) and inside the drawer (sub-1440px).
const FiltersOnlyPanel = ({
  outcomeFilter,
  outcomeCounts,
  sourceKindCounts,
  onOutcomeChange,
  sourceFilter,
  onSourceChange,
  evidenceTypeOptions,
  selectedEvidenceTypes,
  onToggleEvidenceType,
  onClearEvidenceTypes,
  showSkeleton,
}: {
  outcomeFilter: OutcomeFilter;
  outcomeCounts: Record<OutcomeFilter, number>;
  sourceKindCounts: Record<string, number>;
  onOutcomeChange: (o: OutcomeFilter) => void;
  sourceFilter: string;
  onSourceChange: (s: string) => void;
  evidenceTypeOptions: { evidence_type: string; count: number }[];
  selectedEvidenceTypes: string[];
  onToggleEvidenceType: (etype: string) => void;
  onClearEvidenceTypes: () => void;
  showSkeleton: boolean;
}) => (
  <div className="flex flex-col gap-5">
    <div className="flex flex-col gap-1.5">
      <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400">
        Outcome
      </span>
      <div
        className="flex flex-col -mx-1"
        role="radiogroup"
        aria-label="Outcome"
      >
        {OUTCOME_OPTIONS.map((opt) => {
          const c = outcomeCounts[opt];
          return (
            <RadioRow
              key={opt}
              label={opt}
              count={c}
              selected={outcomeFilter === opt}
              disabled={showSkeleton || (opt !== "all" && c === 0)}
              onClick={() => onOutcomeChange(opt)}
            />
          );
        })}
      </div>
    </div>
    {evidenceTypeOptions.length > 0 && (
      <div className="flex flex-col gap-1.5">
        <div className="flex items-baseline justify-between gap-2">
          <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400">
            Evidence
          </span>
          {selectedEvidenceTypes.length > 0 && (
            <button
              type="button"
              onClick={onClearEvidenceTypes}
              className="text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors"
            >
              clear
            </button>
          )}
        </div>
        <div className="flex flex-col -mx-1">
          {evidenceTypeOptions.map(({ evidence_type, count }) => (
            <CheckRow
              key={evidence_type}
              label={evidence_type}
              count={count}
              selected={selectedEvidenceTypes.includes(evidence_type)}
              disabled={showSkeleton}
              onClick={() => onToggleEvidenceType(evidence_type)}
            />
          ))}
        </div>
      </div>
    )}
    <div className="flex flex-col gap-1.5">
      <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400">
        Assay Source
      </span>
      <div
        className="flex flex-col -mx-1"
        role="radiogroup"
        aria-label="Assay Source"
      >
        {/* Counts derived client-side from the modal's row set via
         * `matchesSourceKind` so the chip counts match the filter
         * behavior exactly. */}
        {SOURCE_KINDS.map(({ key, label }) => {
          const c = sourceKindCounts[key] ?? 0;
          return (
            <RadioRow
              key={label}
              label={label}
              count={c}
              selected={sourceFilter === key}
              disabled={showSkeleton || (key !== "" && c === 0)}
              onClick={() => onSourceChange(key)}
            />
          );
        })}
      </div>
    </div>
  </div>
);

const RadioRow = ({
  label,
  count,
  selected,
  disabled,
  onClick,
}: {
  label: string;
  count?: number;
  selected: boolean;
  disabled?: boolean;
  onClick: () => void;
}) => (
  <button
    type="button"
    role="radio"
    aria-checked={selected}
    disabled={disabled}
    aria-disabled={disabled || undefined}
    onClick={onClick}
    className={twMerge(
      "group w-full flex items-center gap-2 pl-1 pr-2 py-1 rounded transition-colors text-left disabled:opacity-40 disabled:cursor-not-allowed disabled:hover:bg-transparent",
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
    {typeof count === "number" && (
      <span
        className={twMerge(
          "tabular-nums text-[10px] flex-shrink-0",
          selected ? "text-light-400" : "text-light-500"
        )}
      >
        {count.toLocaleString()}
      </span>
    )}
  </button>
);

const CheckRow = ({
  label,
  count,
  selected,
  disabled,
  onClick,
}: {
  label: string;
  count?: number;
  selected: boolean;
  disabled?: boolean;
  onClick: () => void;
}) => (
  <button
    type="button"
    onClick={onClick}
    disabled={disabled}
    aria-pressed={selected}
    aria-disabled={disabled || undefined}
    className={twMerge(
      "group w-full flex items-center gap-2 pl-1 pr-2 py-1 rounded transition-colors text-left disabled:opacity-40 disabled:cursor-not-allowed disabled:hover:bg-transparent",
      selected
        ? "text-light-100 hover:bg-light-900/70"
        : "text-light-400 hover:text-light-100 hover:bg-light-900/50",
    )}
  >
    <span
      aria-hidden
      className={twMerge(
        "w-3.5 h-3.5 rounded-[3px] border flex-shrink-0 flex items-center justify-center transition-colors",
        selected
          ? "border-accent-600 bg-accent-600/20 text-accent-600"
          : "border-light-700 group-hover:border-light-500",
      )}
    >
      {selected && <MdCheck className="w-3 h-3" />}
    </span>
    {/* CheckRow is evidence-types only, so capitalizing here is safe —
     * unlike the shared UnitRow, which must not touch unit casing. */}
    <span className="font-mono text-xs flex-1 min-w-0 truncate capitalize">
      {label}
    </span>
    {typeof count === "number" && (
      <span
        className={twMerge(
          "tabular-nums text-[10px] flex-shrink-0",
          selected ? "text-light-400" : "text-light-500",
        )}
      >
        {count.toLocaleString()}
      </span>
    )}
  </button>
);

const MeasurementsTable = ({
  rows,
  placeholderCount,
  skeleton,
  expandedKey,
  onToggleExpand,
  getRowReportProps,
}: {
  rows: ModalRow[];
  placeholderCount: number;
  skeleton: boolean;
  expandedKey: string | null;
  onToggleExpand: (key: string) => void;
  // Callback that returns the report-select props for a given
  // measurement row. Returns {} when the reporter is not in select
  // mode so applying it is a no-op.
  getRowReportProps: (m: ModalRow) => Record<string, unknown>;
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
    <>
    {/* Desktop table */}
    <table className="hidden md:table w-full table-fixed">
      <colgroup>
        <col className="w-[24%]" />
        <col className="w-[14%]" />
        <col className="w-[10%]" />
        <col className="w-[12%]" />
        <col className="w-[14%]" />
        <col className="w-[26%]" />
      </colgroup>
      <thead className="text-light-400 text-left sticky top-0 z-10 bg-light-950">
        <tr>
          {["Assay", "Endpoint", "Outcome", "Source", "Evidence", "Value"].map(
            (h, idx, arr) => (
              <th
                key={h}
                className={twMerge(
                  "h-9 border-b border-light-700 leading-none py-1.5 px-2 first:pl-0 last:pr-0 bg-light-950",
                  idx === arr.length - 1 && "text-right",
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
          const rowReportProps = getRowReportProps(m);
          const inSelectMode = Boolean(rowReportProps.onClick);
          return (
            <Fragment key={key}>
              <tr
                {...rowReportProps}
                onClick={
                  inSelectMode
                    ? (rowReportProps.onClick as React.MouseEventHandler)
                    : canExpand
                    ? () => onToggleExpand(key)
                    : undefined
                }
                aria-expanded={canExpand ? isExpanded : undefined}
                className={twMerge(
                  "transition-colors",
                  canExpand && "cursor-pointer hover:bg-light-900/50",
                  isExpanded && "bg-light-900/50",
                  rowReportProps.className as string | undefined,
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
                <td className="py-1.5 px-2 align-top text-light-200">
                  {m.evidence_type ? (
                    <span className="font-mono text-xs capitalize">
                      {m.evidence_type}
                    </span>
                  ) : (
                    <span className="text-light-600">—</span>
                  )}
                </td>
                <td className="py-1.5 pl-2 align-top">
                  <div className="flex items-center justify-between gap-3">
                    {canExpand ? (
                      <Chip
                        icon={
                          <MdChevronRight
                            className={twMerge(
                              "size-3.5 transition-transform duration-150",
                              isExpanded && "rotate-90",
                            )}
                          />
                        }
                        label={`${isExpanded ? "Hide" : "Show"} Hill Curve`}
                        tone={isExpanded ? "cream" : "outline"}
                        size="md"
                        onClick={(e) => {
                          e.stopPropagation();
                          onToggleExpand(key);
                        }}
                        aria-label={
                          isExpanded ? "Hide Hill curve" : "Show Hill curve"
                        }
                        aria-pressed={isExpanded}
                      />
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
                    colSpan={6}
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
            <td className="py-1.5 pr-2" colSpan={6}>
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

    {/* Card list — mobile. Primary line: Assay + Value. Endpoint /
     * Outcome / Source stack below as label:value rows. Hill Curve
     * button + expanded fit view live at the bottom of the card. */}
    <div className="md:hidden w-full flex flex-col divide-y divide-light-800">
      {skeleton
        ? Array.from({ length: PAGE_SIZE }).map((_, i) => (
            <div key={`sk-${i}`} className="w-full py-3">
              <LoadingCard className="h-5" />
            </div>
          ))
        : dataRows.map((m, i) => {
            const key = rowKey(m, i);
            const canExpand = hasHillFit(m);
            const isExpanded = expandedKey === key;
            const rowReportProps = getRowReportProps(m);
            return (
              <div
                key={key}
                {...rowReportProps}
                className={twMerge(
                  "w-full py-3 flex flex-col gap-2 text-sm",
                  rowReportProps.className as string | undefined,
                )}
              >
                <div className="w-full flex items-center justify-between gap-2 flex-wrap">
                  <AssayCell assay={m.assay} />
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
                <div className="w-full flex items-baseline justify-between gap-2">
                  <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                    Endpoint
                  </span>
                  <span className="text-light-200 text-right">
                    {m.endpoint || "—"}
                  </span>
                </div>
                <div className="w-full flex items-center justify-between gap-2">
                  <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                    Outcome
                  </span>
                  <OutcomeBadge outcome={m.outcome} />
                </div>
                <div className="w-full flex items-center justify-between gap-2">
                  <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                    Source
                  </span>
                  <SourceBadge source={m.evidence_source} />
                </div>
                <div className="w-full flex items-baseline justify-between gap-2">
                  <span className="font-mono italic text-[10px] uppercase tracking-wider text-light-500">
                    Evidence
                  </span>
                  <span className="text-light-200 text-right font-mono text-xs capitalize">
                    {m.evidence_type || "—"}
                  </span>
                </div>
                {canExpand && (
                  <div className="w-full flex justify-end">
                    <Chip
                      icon={
                        <MdChevronRight
                          className={twMerge(
                            "size-3.5 transition-transform duration-150",
                            isExpanded && "rotate-90",
                          )}
                        />
                      }
                      label={`${isExpanded ? "Hide" : "Show"} Hill Curve`}
                      tone={isExpanded ? "cream" : "outline"}
                      size="md"
                      onClick={() => onToggleExpand(key)}
                      aria-label={
                        isExpanded ? "Hide Hill curve" : "Show Hill curve"
                      }
                      aria-pressed={isExpanded}
                    />
                  </div>
                )}
                {isExpanded && (
                  <div className="w-full pt-3 pb-3 pl-3 pr-2 border-t border-l-2 border-l-accent-600 border-light-700/40">
                    <ExpandedHillFit m={m} />
                  </div>
                )}
              </div>
            );
          })}
    </div>
    </>
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
