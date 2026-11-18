"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { MdTune } from "react-icons/md";

import Card from "@/components/basic/Card";
import EvidenceTable from "@/components/entities/food/EvidenceTable";
import Modal from "@/components/basic/Modal";
import {
  FilterGroup,
  FilterOption,
  FilterOptionList,
  FilterSearchInput,
} from "@/components/entities/shared/filters/FilterControls";
import {
  FilterDrawer,
  FilterPanelBody,
} from "@/components/entities/shared/filters/FilterPanel";
import { sortFacetOptions } from "@/components/entities/shared/filters/facetOptions";
import {
  countDataPoints,
  extractionMatches,
  sourceNamesFor,
} from "@/components/entities/food/compositionDataPoints";
import { FoodEvidence } from "@/types/Evidence";

export type EvidenceFilter = "all" | "low-trust";

// The source option list comes from the UNFILTERED evidence set on purpose.
// Its counts are faceted (other filters applied), and deriving the list from
// those counts would make a source vanish the moment another filter zeroed it,
// leaving no way to click back to it. The row disables at zero instead.
interface FoodCompositionEvidenceModalProps {
  foodName: string;
  chemicalName: string;
  evidences: FoodEvidence[] | undefined;
  isOpen: boolean;
  onClose: () => void;
  initialFilter?: EvidenceFilter;
  // Source filter keys ("fdc", "ptfi", ...) selected on the table behind
  // this modal. The modal opens on those sources only, so its count equals
  // the row's "N data points"; the reader can add the others back here.
  selectedSources?: string[];
}

const FoodCompositionEvidenceModal = ({
  foodName,
  chemicalName,
  evidences,
  isOpen,
  onClose,
  initialFilter = "all",
  selectedSources,
}: FoodCompositionEvidenceModalProps) => {
  const [filter, setFilter] = useState<EvidenceFilter>(initialFilter);
  const [searchTerm, setSearchTerm] = useState("");
  // Selected source names; empty = every source.
  const [sources, setSources] = useState<string[]>([]);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  // The page's sources, keyed as a string: callers pass a fresh array on
  // every render.
  const openingSourcesKey = sourceNamesFor(selectedSources).join("+");

  // "Clear filters" shows every source, but keeps `initialFilter`: arriving
  // via a TrustBadge opens the modal on low-trust, and that intent stays.
  const clearFilters = useCallback(() => {
    setFilter(initialFilter);
    setSearchTerm("");
    setSources([]);
  }, [initialFilter]);

  const isFiltersDirty =
    filter !== initialFilter || searchTerm !== "" || sources.length > 0;

  useEffect(() => {
    if (isOpen) {
      clearFilters();
      setSources(openingSourcesKey ? openingSourcesKey.split("+") : []);
      setMobileFiltersOpen(false);
    }
  }, [isOpen, clearFilters, openingSourcesKey]);

  const lowTrustOnly = filter === "low-trust";
  const count = useCallback(
    (f: { sources: string[]; lowTrustOnly?: boolean }) =>
      countDataPoints(evidences, { ...f, search: searchTerm }),
    [evidences, searchTerm]
  );

  // Faceted: each group holds the other filters fixed and varies its own.
  const totalCount = useMemo(() => count({ sources }), [count, sources]);
  const lowTrustCount = useMemo(
    () => count({ sources, lowTrustOnly: true }),
    [count, sources]
  );

  // Has trust been evaluated for this evidence at all? The API omits
  // `trust_low` entirely when it hasn't, and sets it on EVERY extraction when
  // it has — so field presence, not truthiness, is the signal.
  //
  // Distinguishes two cases a count alone conflates:
  //   - evaluated, none low here  → show Quality, disable the empty option
  //   - never evaluated           → no Quality group; there is nothing to say
  //
  // The second is the common one today. Only /food/composition applies the
  // trust filter (food.py), and only when its "Low-trust data points" toggle
  // is on; the chemical page reuses this modal via /chemical/composition-
  // evidence, which has no trust handling, so its extractions never carry the
  // field. Without this the group would sit there permanently greyed at 0.
  const trustEvaluated = useMemo(
    () =>
      (evidences ?? []).some((ev) =>
        (ev.extraction ?? []).some((ex) => "trust_low" in ex),
      ),
    [evidences],
  );

  // The key list stays derived from the full set so options never disappear.
  const sourceKeys = useMemo(() => {
    const keys: string[] = [];
    evidences?.forEach((ev) => {
      const name = ev.reference.source_name;
      if (name && !keys.includes(name)) keys.push(name);
    });
    return sortFacetOptions(keys, (k) => k);
  }, [evidences]);

  const sourceCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    sourceKeys.forEach((key) => {
      counts[key] = count({ sources: [key], lowTrustOnly });
    });
    return counts;
  }, [count, sourceKeys, lowTrustOnly]);

  // Filtered at the extraction level, so the table's rows match the count.
  // Evidences with no rows left are dropped so their paper header doesn't
  // dangle empty.
  const displayedEvidences = useMemo(() => {
    if (!evidences) return evidences;
    const f = { sources, lowTrustOnly, search: searchTerm };
    return evidences
      .map((ev) => ({
        ...ev,
        extraction: ev.extraction.filter((ex) => extractionMatches(ev, ex, f)),
      }))
      .filter((ev) => ev.extraction.length > 0);
  }, [evidences, lowTrustOnly, searchTerm, sources]);

  const filteredCount = useMemo(
    () =>
      displayedEvidences?.reduce(
        (sum, ev) => sum + ev.extraction.length,
        0,
      ) ?? 0,
    [displayedEvidences],
  );

  const toggleSource = (name: string) =>
    setSources((prev) =>
      prev.includes(name) ? prev.filter((s) => s !== name) : [...prev, name]
    );

  const searchInput = (
    <SearchInput
      value={searchTerm}
      onChange={setSearchTerm}
      onClear={() => setSearchTerm("")}
    />
  );

  // One node, rendered into both the sidebar and the drawer, so the reset
  // belongs here rather than at either call site.
  const filtersPanel = (
    <FilterPanelBody isDirty={isFiltersDirty} onReset={clearFilters}>
      <FiltersPanel
        sources={sources}
        sourceKeys={sourceKeys}
        sourceCounts={sourceCounts}
        onToggleSource={toggleSource}
        onClearSources={() => setSources([])}
        filter={filter}
        lowTrustCount={lowTrustCount}
        totalCount={totalCount}
        onSetFilter={setFilter}
        trustEvaluated={trustEvaluated}
      />
    </FilterPanelBody>
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
          <span
            className="font-mono italic text-xs text-light-400"
            data-filtered-count={filteredCount}
          >
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

      <EvidenceTable evidences={displayedEvidences} />

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
  sources,
  sourceKeys,
  sourceCounts,
  onToggleSource,
  onClearSources,
  filter,
  lowTrustCount,
  totalCount,
  onSetFilter,
  trustEvaluated,
}: {
  sources: string[];
  sourceKeys: string[];
  sourceCounts: Record<string, number>;
  onToggleSource: (name: string) => void;
  onClearSources: () => void;
  filter: EvidenceFilter;
  lowTrustCount: number;
  totalCount: number;
  onSetFilter: (f: EvidenceFilter) => void;
  // False when this evidence carries no trust judgement at all.
  trustEvaluated: boolean;
}) => (
  <div className="flex flex-col gap-5">
    <FilterGroup
      label="Source"
      onClear={sources.length > 0 ? onClearSources : undefined}
    >
      <FilterOptionList>
        {sourceKeys.map((key) => (
          <FilterOption
            key={key}
            label={key}
            count={sourceCounts[key] ?? 0}
            selected={sources.includes(key)}
            onClick={() => onToggleSource(key)}
          />
        ))}
      </FilterOptionList>
    </FilterGroup>

    {/* Two mutually exclusive options, so a radio facet like every other
      * single-select group. It used to be one cycle button whose label WAS
      * its state ("All (6)" / "Only low-trust (2)"), which read as a mystery
      * chip when nothing was low-trust: disabled, warning-triangled, and
      * saying "All (6)" next to a line repeating the same 6.
      *
      * Rendered whenever trust WAS evaluated, with the empty option disabled
      * — the rule Source above already follows and FilterOption documents.
      * Hiding at a zero COUNT would trap the user: these counts are faceted,
      * so searching can drive lowTrustCount to 0 while "Low-trust only" is
      * still the active filter, and the control to switch back would vanish
      * with it. Absent trust data is the separate case handled above. */}
    {trustEvaluated && (
    <FilterGroup label="Quality">
      <FilterOptionList mode="radio" ariaLabel="Evidence quality">
        <FilterOption
          mode="radio"
          label="All"
          count={totalCount}
          selected={filter === "all"}
          resetOption
          onClick={() => onSetFilter("all")}
        />
        <FilterOption
          mode="radio"
          label="Low-trust only"
          count={lowTrustCount}
          selected={filter === "low-trust"}
          onClick={() => onSetFilter("low-trust")}
        />
      </FilterOptionList>
    </FilterGroup>
    )}
  </div>
);

