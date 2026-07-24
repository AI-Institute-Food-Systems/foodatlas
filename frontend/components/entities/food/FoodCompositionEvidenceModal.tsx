"use client";

import { useEffect, useMemo, useState } from "react";
import {
  MdCallSplit,
  MdClose,
  MdSearch,
  MdTune,
  MdWarningAmber,
} from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import EvidenceTable from "@/components/entities/food/EvidenceTable";
import Modal from "@/components/basic/Modal";
import { FoodEvidence, FoodEvidenceExtraction } from "@/types/Evidence";

// On the food page the head (food) side of ambiguity is owned by the entity
// banner; the modal only surfaces ambiguity on the counterpart (chemical) side.
const isCounterpartAmbiguous = (ex: FoodEvidenceExtraction): boolean =>
  (ex.chemical_candidates?.length ?? 0) > 1;

const isLowTrust = (ex: FoodEvidenceExtraction): boolean => Boolean(ex.trust_low);

export type EvidenceFilter =
  | "all"
  | "ambiguous"
  | "not-ambiguous"
  | "low-trust";

// Mirrors the BioactivityMeasurementsModal's Assay Source picker so users
// see the same radio-row shape on both modals. FoodEvidence.reference
// carries `source_name` = "FoodAtlas" | "FDC" — we render "both" as the
// default no-filter option.
const SOURCE_KINDS: { key: string; label: string }[] = [
  { key: "", label: "both" },
  { key: "FoodAtlas", label: "FoodAtlas" },
  { key: "FDC", label: "FDC" },
];

const matchesSource = (
  ev: FoodEvidence,
  sourceKey: string,
): boolean => !sourceKey || ev.reference.source_name === sourceKey;

interface FoodCompositionEvidenceModalProps {
  foodName: string;
  chemicalName: string;
  evidences: FoodEvidence[] | undefined;
  isOpen: boolean;
  onClose: () => void;
  initialFilter?: EvidenceFilter;
}

const AMBIGUITY_CYCLE: EvidenceFilter[] = ["all", "ambiguous", "not-ambiguous"];
const LOW_TRUST_CYCLE: EvidenceFilter[] = ["all", "low-trust"];

const FoodCompositionEvidenceModal = ({
  foodName,
  chemicalName,
  evidences,
  isOpen,
  onClose,
  initialFilter = "all",
}: FoodCompositionEvidenceModalProps) => {
  const [filter, setFilter] = useState<EvidenceFilter>(initialFilter);
  const [searchTerm, setSearchTerm] = useState("");
  const [sourceKind, setSourceKind] = useState("");
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  useEffect(() => {
    if (isOpen) {
      setFilter(initialFilter);
      setSearchTerm("");
      setSourceKind("");
      setMobileFiltersOpen(false);
    }
  }, [isOpen, initialFilter]);

  // Extraction-level counts now that the table renders one row per
  // extraction — chip labels stay in sync with what the table actually
  // shows after filtering.
  const { totalCount, ambiguousCount, notAmbiguousCount, lowTrustCount } =
    useMemo(() => {
      let total = 0;
      let ambig = 0;
      let low = 0;
      evidences?.forEach((ev) => {
        ev.extraction.forEach((ex) => {
          total += 1;
          if (isCounterpartAmbiguous(ex)) ambig += 1;
          if (isLowTrust(ex)) low += 1;
        });
      });
      return {
        totalCount: total,
        ambiguousCount: ambig,
        notAmbiguousCount: total - ambig,
        lowTrustCount: low,
      };
    }, [evidences]);

  // Source counts — computed off the FULL row set so labels stay stable
  // as the user narrows the view (they reflect "what would clicking this
  // source give me", not the current filtered subset).
  const sourceCounts = useMemo(() => {
    const counts: Record<string, number> = { "": 0, FoodAtlas: 0, FDC: 0 };
    evidences?.forEach((ev) => {
      const key = ev.reference.source_name;
      counts[key] = (counts[key] ?? 0) + ev.extraction.length;
      counts[""] += ev.extraction.length;
    });
    return counts;
  }, [evidences]);

  const cycleAmbiguityFilter = () =>
    setFilter((f) => {
      const idx = AMBIGUITY_CYCLE.indexOf(f);
      if (idx === -1) return AMBIGUITY_CYCLE[1];
      return AMBIGUITY_CYCLE[(idx + 1) % AMBIGUITY_CYCLE.length];
    });

  const cycleLowTrustFilter = () =>
    setFilter((f) => {
      const idx = LOW_TRUST_CYCLE.indexOf(f);
      if (idx === -1) return LOW_TRUST_CYCLE[1];
      return LOW_TRUST_CYCLE[(idx + 1) % LOW_TRUST_CYCLE.length];
    });

  const matchesSearch = (
    ev: FoodEvidence,
    ex: FoodEvidenceExtraction,
    q: string,
  ): boolean => {
    if (!q) return true;
    const haystack = [
      ex.extracted_chemical_name,
      ex.extracted_food_name,
      ex.extracted_concentration,
      ex.method,
      ev.premise,
      ev.reference.display_name,
      ev.reference.id,
      ...(ex.chemical_candidates ?? []),
      ...(ex.food_candidates ?? []),
    ]
      .filter((s): s is string => Boolean(s))
      .join(" ")
      .toLowerCase();
    return haystack.includes(q);
  };

  // Filter is applied at the extraction level so the table's row set
  // exactly matches the active chip's count. Evidences with no rows
  // remaining after the extraction filter are dropped so their paper
  // header doesn't dangle empty in the expanded row.
  const displayedEvidences = useMemo(() => {
    if (!evidences) return evidences;
    const q = searchTerm.trim().toLowerCase();
    const chipPredicate = (ex: FoodEvidenceExtraction) => {
      if (filter === "ambiguous") return isCounterpartAmbiguous(ex);
      if (filter === "not-ambiguous") return !isCounterpartAmbiguous(ex);
      if (filter === "low-trust") return isLowTrust(ex);
      return true;
    };
    return evidences
      .filter((ev) => matchesSource(ev, sourceKind))
      .map((ev) => ({
        ...ev,
        extraction: ev.extraction.filter(
          (ex) => chipPredicate(ex) && matchesSearch(ev, ex, q),
        ),
      }))
      .filter((ev) => ev.extraction.length > 0);
  }, [evidences, filter, searchTerm, sourceKind]);

  const filteredCount = useMemo(
    () =>
      displayedEvidences?.reduce(
        (sum, ev) => sum + ev.extraction.length,
        0,
      ) ?? 0,
    [displayedEvidences],
  );

  const ambiguityLabel =
    filter === "ambiguous"
      ? `Only ambiguous (${ambiguousCount})`
      : filter === "not-ambiguous"
      ? `Not ambiguous (${notAmbiguousCount})`
      : `All (${totalCount})`;

  const lowTrustLabel =
    filter === "low-trust"
      ? `Only low-trust (${lowTrustCount})`
      : `All (${totalCount})`;

  const searchInput = (
    <SearchInput
      value={searchTerm}
      onChange={setSearchTerm}
      onClear={() => setSearchTerm("")}
    />
  );

  const filtersPanel = (
    <FiltersPanel
      sourceKind={sourceKind}
      sourceCounts={sourceCounts}
      onSourceKindChange={setSourceKind}
      filter={filter}
      ambiguousCount={ambiguousCount}
      notAmbiguousCount={notAmbiguousCount}
      lowTrustCount={lowTrustCount}
      totalCount={totalCount}
      onCycleAmbiguity={cycleAmbiguityFilter}
      onCycleLowTrust={cycleLowTrustFilter}
      ambiguityLabel={ambiguityLabel}
      lowTrustLabel={lowTrustLabel}
    />
  );

  return (
    <Modal
      fullHeight
      title="Data Points"
      description={
        <div className="flex flex-col gap-1">
          <p>
            The following data points indicate that{" "}
            <span className="capitalize font-semibold">{foodName}</span>{" "}
            contains{" "}
            <span className="capitalize font-semibold">{chemicalName}</span>
          </p>
          <span className="font-mono italic text-xs text-light-400">
            {totalCount.toLocaleString()} data point
            {totalCount === 1 ? "" : "s"}
            {filteredCount !== totalCount && (
              <span className="ml-2 not-italic normal-case text-light-600">
                · {filteredCount.toLocaleString()} after filters
              </span>
            )}
          </span>
        </div>
      }
      isOpen={isOpen}
      onClose={onClose}
      sidebar={
        <Card className="px-4 py-4 gap-5">
          {searchInput}
          {filtersPanel}
        </Card>
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

      <EvidenceTable
        evidences={displayedEvidences}
        chemicalName={chemicalName}
      />

      {/* Sub-1440px filter drawer. Mirrors the bioactivity modal's
       * drawer so the same filter chrome is reachable on narrow
       * viewports where the sidebar is hidden. */}
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
                onClick={() => setMobileFiltersOpen(false)}
                aria-label="Close filters"
                className="w-8 h-8 rounded-full flex items-center justify-center text-light-400 hover:text-light-100 hover:bg-light-800/60 transition-colors"
              >
                <MdClose className="w-4 h-4" />
              </button>
            </div>
            {filtersPanel}
          </aside>
        </div>
      )}
    </Modal>
  );
};

FoodCompositionEvidenceModal.displayName = "FoodCompositionEvidenceModal";

export default FoodCompositionEvidenceModal;

// -- Sidebar-only widgets ---------------------------------------------------

const SearchInput = ({
  value,
  onChange,
  onClear,
}: {
  value: string;
  onChange: (v: string) => void;
  onClear: () => void;
}) => (
  <div className="relative flex items-center">
    <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
    <input
      className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none"
      type="text"
      placeholder="Search chemical, food, or paper"
      aria-label="Search chemical, food, or paper"
      value={value}
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

const FiltersPanel = ({
  sourceKind,
  sourceCounts,
  onSourceKindChange,
  filter,
  ambiguousCount,
  notAmbiguousCount,
  lowTrustCount,
  totalCount,
  onCycleAmbiguity,
  onCycleLowTrust,
  ambiguityLabel,
  lowTrustLabel,
}: {
  sourceKind: string;
  sourceCounts: Record<string, number>;
  onSourceKindChange: (k: string) => void;
  filter: EvidenceFilter;
  ambiguousCount: number;
  notAmbiguousCount: number;
  lowTrustCount: number;
  totalCount: number;
  onCycleAmbiguity: () => void;
  onCycleLowTrust: () => void;
  ambiguityLabel: string;
  lowTrustLabel: string;
}) => (
  <div className="flex flex-col gap-5">
    <div className="flex flex-col gap-1.5">
      <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400">
        Source
      </span>
      <div
        className="flex flex-col -mx-1"
        role="radiogroup"
        aria-label="Evidence source"
      >
        {SOURCE_KINDS.map(({ key, label }) => (
          <RadioRow
            key={label}
            label={label}
            count={sourceCounts[key] ?? 0}
            selected={sourceKind === key}
            disabled={key !== "" && (sourceCounts[key] ?? 0) === 0}
            onClick={() => onSourceKindChange(key)}
          />
        ))}
      </div>
    </div>

    <div className="flex flex-col gap-1.5">
      <span className="font-mono italic text-[11px] uppercase tracking-wider text-light-400">
        Quality
      </span>
      <div className="flex flex-col gap-2">
        <button
          type="button"
          onClick={onCycleAmbiguity}
          disabled={ambiguousCount === 0}
          aria-disabled={ambiguousCount === 0 || undefined}
          className={twMerge(
            "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium w-fit transition-colors",
            filter === "ambiguous"
              ? "text-amber-300 border-amber-400 bg-amber-500/20 hover:bg-amber-500/30"
              : filter === "not-ambiguous"
              ? "text-light-300 border-light-400 bg-light-400/15 hover:bg-light-400/25"
              : "text-light-300 border-light-500 bg-light-500/10 hover:bg-light-500/20",
            ambiguousCount === 0 &&
              "opacity-40 cursor-not-allowed hover:bg-transparent",
          )}
          aria-label="Cycle ambiguity filter"
        >
          <MdCallSplit className="size-3.5 rotate-90" />
          {ambiguityLabel}
        </button>
        <button
          type="button"
          onClick={onCycleLowTrust}
          disabled={lowTrustCount === 0}
          aria-disabled={lowTrustCount === 0 || undefined}
          className={twMerge(
            "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium w-fit transition-colors",
            filter === "low-trust"
              ? "text-rose-300 border-rose-400 bg-rose-500/20 hover:bg-rose-500/30"
              : "text-light-300 border-light-500 bg-light-500/10 hover:bg-light-500/20",
            lowTrustCount === 0 &&
              "opacity-40 cursor-not-allowed hover:bg-transparent",
          )}
          aria-label="Cycle low-trust filter"
        >
          <MdWarningAmber className="size-3.5" />
          {lowTrustLabel}
        </button>
      </div>
      <span className="mt-1 text-[10px] text-light-500 font-mono">
        {totalCount.toLocaleString()} data point
        {totalCount === 1 ? "" : "s"} total
      </span>
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
  count: number;
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
    <span className="font-mono text-xs flex-1 min-w-0 truncate">{label}</span>
    <span
      className={twMerge(
        "tabular-nums text-[10px] flex-shrink-0",
        selected ? "text-light-400" : "text-light-500",
      )}
    >
      {count.toLocaleString()}
    </span>
  </button>
);
