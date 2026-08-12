"use client";

import { useEffect, useMemo, useState } from "react";
import { MdClose, MdSearch, MdTune, MdWarningAmber } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import EvidenceTable from "@/components/entities/food/EvidenceTable";
import Modal from "@/components/basic/Modal";
import { FoodEvidence, FoodEvidenceExtraction } from "@/types/Evidence";

const isLowTrust = (ex: FoodEvidenceExtraction): boolean => Boolean(ex.trust_low);

export type EvidenceFilter = "all" | "low-trust";

// Mirrors the BioactivityMeasurementsModal's Assay Source picker so users
// see the same radio-row shape on both modals. "All" leads as the no-filter
// option; the rest are derived from the evidence actually present, because
// this list used to be hardcoded to FoodAtlas + FDC and so could never offer
// PTFI. Known sources keep a stable order; anything new sorts after them
// rather than being dropped.
//
// The option list comes from the UNFILTERED evidence set on purpose. Its
// counts are faceted (see countExtractions), and deriving the list from those
// counts would make a source vanish the moment another filter zeroed it —
// leaving no way to click back to it. The row disables at zero instead.
const SOURCE_ORDER = ["FDC", "FoodAtlas", "PTFI"];

const buildSourceKinds = (
  keys: string[],
): { key: string; label: string }[] => {
  const present = [...keys].sort((a, b) => {
    const ia = SOURCE_ORDER.indexOf(a);
    const ib = SOURCE_ORDER.indexOf(b);
    if (ia !== -1 && ib !== -1) return ia - ib;
    if (ia !== -1) return -1;
    if (ib !== -1) return 1;
    return a.localeCompare(b);
  });
  return [
    { key: "", label: "All" },
    ...present.map((k) => ({ key: k, label: k })),
  ];
};

const matchesSource = (
  ev: FoodEvidence,
  sourceKey: string,
): boolean => !sourceKey || ev.reference.source_name === sourceKey;

const matchesSearch = (
  ev: FoodEvidence,
  ex: FoodEvidenceExtraction,
  q: string,
): boolean => {
  if (!q) return true;
  // Deliberately excludes ev.premise — the source text often mentions
  // dozens of unrelated chemicals/foods, so searching it would surface
  // extractions that don't actually mention the search term as the
  // extracted entity. Users search to filter on WHAT was extracted,
  // not what appeared somewhere in the paragraph.
  const haystack = [
    ex.extracted_chemical_name,
    ex.extracted_food_name,
    ex.extracted_concentration,
    ex.method,
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

interface FoodCompositionEvidenceModalProps {
  foodName: string;
  chemicalName: string;
  evidences: FoodEvidence[] | undefined;
  isOpen: boolean;
  onClose: () => void;
  initialFilter?: EvidenceFilter;
}

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

  const query = searchTerm.trim().toLowerCase();

  // Counts every extraction passing the given filter combination. Callers
  // omit the dimension whose own chip they're labelling, which is what makes
  // the counts faceted: a number answers "what would I get if I clicked
  // this?", never "what am I looking at right now".
  //
  // Previously both count sets were computed off the full evidence list, so
  // narrowing by search or quality left every source number unchanged — the
  // one place in the app that ignored the joint-filtering rule the sidebars
  // follow.
  const countExtractions = useMemo(
    () =>
      ({
        source,
        lowTrustOnly = false,
      }: {
        source: string;
        lowTrustOnly?: boolean;
      }): number => {
        let n = 0;
        evidences?.forEach((ev) => {
          if (!matchesSource(ev, source)) return;
          ev.extraction.forEach((ex) => {
            if (lowTrustOnly && !isLowTrust(ex)) return;
            if (!matchesSearch(ev, ex, query)) return;
            n += 1;
          });
        });
        return n;
      },
    [evidences, query],
  );

  // Quality counts hold source + search fixed, varying only quality.
  const lowTrustOnly = filter === "low-trust";
  const totalCount = useMemo(
    () => countExtractions({ source: sourceKind }),
    [countExtractions, sourceKind],
  );
  const lowTrustCount = useMemo(
    () => countExtractions({ source: sourceKind, lowTrustOnly: true }),
    [countExtractions, sourceKind],
  );

  // Source counts hold quality + search fixed, varying only source. The key
  // list stays derived from the full set so options never disappear.
  const sourceKeys = useMemo(() => {
    const keys: string[] = [];
    evidences?.forEach((ev) => {
      const name = ev.reference.source_name;
      if (name && !keys.includes(name)) keys.push(name);
    });
    return keys;
  }, [evidences]);

  const sourceCounts = useMemo(() => {
    const counts: Record<string, number> = {
      "": countExtractions({ source: "", lowTrustOnly }),
    };
    sourceKeys.forEach((key) => {
      counts[key] = countExtractions({ source: key, lowTrustOnly });
    });
    return counts;
  }, [countExtractions, sourceKeys, lowTrustOnly]);

  const cycleLowTrustFilter = () =>
    setFilter((f) => {
      const idx = LOW_TRUST_CYCLE.indexOf(f);
      if (idx === -1) return LOW_TRUST_CYCLE[1];
      return LOW_TRUST_CYCLE[(idx + 1) % LOW_TRUST_CYCLE.length];
    });

  // Filter is applied at the extraction level so the table's row set
  // exactly matches the active chip's count. Evidences with no rows
  // remaining after the extraction filter are dropped so their paper
  // header doesn't dangle empty in the expanded row.
  const displayedEvidences = useMemo(() => {
    if (!evidences) return evidences;
    const chipPredicate = (ex: FoodEvidenceExtraction) =>
      lowTrustOnly ? isLowTrust(ex) : true;
    return evidences
      .filter((ev) => matchesSource(ev, sourceKind))
      .map((ev) => ({
        ...ev,
        extraction: ev.extraction.filter(
          (ex) => chipPredicate(ex) && matchesSearch(ev, ex, query),
        ),
      }))
      .filter((ev) => ev.extraction.length > 0);
  }, [evidences, lowTrustOnly, query, sourceKind]);

  const filteredCount = useMemo(
    () =>
      displayedEvidences?.reduce(
        (sum, ev) => sum + ev.extraction.length,
        0,
      ) ?? 0,
    [displayedEvidences],
  );

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
      sourceKeys={sourceKeys}
      sourceCounts={sourceCounts}
      onSourceKindChange={setSourceKind}
      filter={filter}
      lowTrustCount={lowTrustCount}
      totalCount={totalCount}
      onCycleLowTrust={cycleLowTrustFilter}
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
  sourceKeys,
  sourceCounts,
  onSourceKindChange,
  filter,
  lowTrustCount,
  totalCount,
  onCycleLowTrust,
  lowTrustLabel,
}: {
  sourceKind: string;
  sourceKeys: string[];
  sourceCounts: Record<string, number>;
  onSourceKindChange: (k: string) => void;
  filter: EvidenceFilter;
  lowTrustCount: number;
  totalCount: number;
  onCycleLowTrust: () => void;
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
        {buildSourceKinds(sourceKeys).map(({ key, label }) => (
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
    {/* Source names come from the data, so a future lowercase one would
     * otherwise render lowercase here while its sibling modal capitalizes.
     * Harmless on the current values — CSS capitalize leaves FDC/PTFI alone. */}
    <span className="font-mono text-xs flex-1 min-w-0 truncate capitalize">
      {label}
    </span>
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
