"use client";

import { useEffect, useMemo, useState } from "react";
import { MdClose, MdTune, MdWarningAmber } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import Card from "@/components/basic/Card";
import EvidenceTable from "@/components/entities/food/EvidenceTable";
import Modal from "@/components/basic/Modal";
import {
  FilterGroup,
  FilterOption,
  FilterOptionList,
  FilterSearchInput,
} from "@/components/entities/shared/filters/FilterControls";
import { FilterDrawer } from "@/components/entities/shared/filters/FilterPanel";
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
      <FilterDrawer
        open={mobileFiltersOpen}
        onClose={() => setMobileFiltersOpen(false)}
      >
        {filtersPanel}
      </FilterDrawer>
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
  <FilterSearchInput
      value={value}
      onChange={(v) => onChange(v)}
      onClear={onClear}
      placeholder="Search chemical, food, or paper"
      ariaLabel="Search chemical, food, or paper"
    />
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
    <FilterGroup label="Source">
      <FilterOptionList mode="radio" ariaLabel="Evidence source">
        {buildSourceKinds(sourceKeys).map(({ key, label }) => (
          <FilterOption
            key={label}
            mode="radio"
            label={label}
            count={sourceCounts[key] ?? 0}
            selected={sourceKind === key}
            disabled={key !== "" && (sourceCounts[key] ?? 0) === 0}
            onClick={() => onSourceKindChange(key)}
          />
        ))}
      </FilterOptionList>
    </FilterGroup>

    <FilterGroup label="Quality">
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
    </FilterGroup>
  </div>
);

