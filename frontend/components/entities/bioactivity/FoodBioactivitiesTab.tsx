"use client";

// Shared search + source-kind filter chrome for BOTH the direct
// FoodBioactivitiesSection and the FoodInferredBioactivitiesSection.
// Instead of each section owning its own sidebar (as they do
// standalone), this component hosts one sidebar aside + one mobile
// drawer and drives both tables via `externalSearch` /
// `externalSourceKind` / `hideChrome` props.

import { useEffect, useState } from "react";
import { MdCheck, MdClose, MdSearch, MdTune } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import {
  FilterGroup,
  FilterOption,
  FilterOptionList,
} from "@/components/entities/shared/filters/FilterControls";
import FilterPanel from "@/components/entities/shared/filters/FilterPanel";
import FoodBioactivitiesSection from "@/components/entities/bioactivity/FoodBioactivitiesSection";
import FoodInferredBioactivitiesSection from "@/components/entities/bioactivity/FoodInferredBioactivitiesSection";
import {
  getBioactivityEndpointOptions,
  getBioactivityEvidenceTypeCounts,
  getBioactivitySourceKindCounts,
} from "@/utils/fetching";
import { usePublishTabCount } from "@/context/tabCountsContext";

interface Props {
  commonName: string;
  anchorId?: string | null;
}

const SOURCE_KINDS: { key: string; label: string }[] = [
  { key: "", label: "All" },
  { key: "experimental", label: "Experimental" },
  { key: "predicted", label: "Predicted" },
];

const TOP_UNITS = 5;

const FoodBioactivitiesTab = ({ commonName, anchorId }: Props) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedSourceKind, setSelectedSourceKind] = useState<string>("");
  const [selectedUnits, setSelectedUnits] = useState<string[]>([]);
  const [showAllUnits, setShowAllUnits] = useState(false);
  const [unitOptions, setUnitOptions] = useState<
    { unit: string; count: number }[]
  >([]);
  const [selectedEvidenceTypes, setSelectedEvidenceTypes] = useState<string[]>(
    []
  );
  const [evidenceTypeOptions, setEvidenceTypeOptions] = useState<
    { evidence_type: string; count: number }[]
  >([]);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  // Aggregated filtered totals from direct + inferred tables → the
  // "Bioactivities" tab badge. Each sub-table reports null while its
  // fetch is in flight, and the two resolve independently — so the sum
  // is only meaningful once BOTH have reported. Publishing as soon as
  // either landed made the badge show a partial count and then visibly
  // jump (e.g. 12 -> 348) when the second table finished. Staying null
  // until both are in keeps the placeholder up for that whole window.
  const [directTotal, setDirectTotal] = useState<number | null>(null);
  const [inferredTotal, setInferredTotal] = useState<number | null>(null);
  const combinedTotal =
    directTotal === null || inferredTotal === null
      ? null
      : directTotal + inferredTotal;
  usePublishTabCount("bioactivities", combinedTotal);

  // Source-kind counts for the sidebar Assay Source picker. Aggregated
  // across BOTH the direct (food-bioactivities) and inferred
  // (food-inferred-bioactivities) directions since the sidebar drives
  // both tables — same treatment as the Unit filter above.
  const [sourceKindCounts, setSourceKindCounts] = useState<{
    both: number;
    experimental: number;
    predicted: number;
  } | null>(null);
  useEffect(() => {
    if (!commonName) return;
    let cancelled = false;
    // Apply the tab's current search + unit filter (no category filter
    // on the food-bioactivities/inferred directions today) so the source
    // kind counts stay in sync with the visible tables.
    const filters = {
      filterUnit: selectedUnits.join("+"),
      filterEvidenceType: selectedEvidenceTypes.join("+"),
      search: searchTerm,
    };
    (async () => {
      const [direct, inferred] = await Promise.all([
        getBioactivitySourceKindCounts(
          commonName,
          "food-bioactivities",
          filters,
        ),
        getBioactivitySourceKindCounts(
          commonName,
          "food-inferred-bioactivities",
          filters,
        ),
      ]);
      if (cancelled) return;
      if (!direct && !inferred) {
        setSourceKindCounts(null);
        return;
      }
      setSourceKindCounts({
        both: (direct?.both ?? 0) + (inferred?.both ?? 0),
        experimental:
          (direct?.experimental ?? 0) + (inferred?.experimental ?? 0),
        predicted: (direct?.predicted ?? 0) + (inferred?.predicted ?? 0),
      });
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName, selectedUnits, selectedEvidenceTypes, searchTerm]);

  const sourceKindParam = selectedSourceKind;
  const unitParam = selectedUnits.join("+");
  const evidenceTypeParam = selectedEvidenceTypes.join("+");

  // Aggregated evidence-type counts across BOTH tables (direct +
  // inferred). Each direction returns a list of {evidence_type, count};
  // we merge on evidence_type and re-sort by summed count.
  useEffect(() => {
    if (!commonName) return;
    let cancelled = false;
    (async () => {
      const evidenceFacets = {
        filterUnit: selectedUnits.join("+"),
        filterSourceKind: selectedSourceKind,
        search: searchTerm,
      };
      const [direct, inferred] = await Promise.all([
        getBioactivityEvidenceTypeCounts(
          commonName,
          "food-bioactivities",
          evidenceFacets
        ),
        getBioactivityEvidenceTypeCounts(
          commonName,
          "food-inferred-bioactivities",
          evidenceFacets
        ),
      ]);
      if (cancelled) return;
      const totals = new Map<string, number>();
      for (const o of [...direct, ...inferred]) {
        const t = (o.evidence_type ?? "").trim();
        if (!t) continue;
        totals.set(t, (totals.get(t) ?? 0) + (o.count ?? 0));
      }
      setEvidenceTypeOptions(
        Array.from(totals.entries())
          .map(([evidence_type, count]) => ({ evidence_type, count }))
          .sort((a, b) => b.count - a.count)
      );
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName, selectedUnits, selectedSourceKind, searchTerm]);

  // Aggregated unit list across BOTH tables — direct (food-level
  // measurements, usually just "mmol/100g") + inferred (all measurements
  // for every chemical present in this food, so IC50 uM/nM, MIC ug/mL,
  // etc). Fetches both directions and merges counts so the sidebar
  // surfaces the full spectrum of units the user might filter by.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const unitFacets = {
        filterEvidenceType: selectedEvidenceTypes.join("+"),
        filterSourceKind: selectedSourceKind,
        search: searchTerm,
      };
      const [direct, inferred] = await Promise.all([
        getBioactivityEndpointOptions(
          commonName,
          "food-bioactivities",
          unitFacets
        ),
        getBioactivityEndpointOptions(
          commonName,
          "food-inferred-bioactivities",
          unitFacets
        ),
      ]);
      if (cancelled) return;
      const totals = new Map<string, number>();
      for (const o of [...direct, ...inferred]) {
        const u = (o.unit ?? "").trim();
        if (!u) continue;
        totals.set(u, (totals.get(u) ?? 0) + (o.count ?? 0));
      }
      setUnitOptions(
        Array.from(totals.entries())
          .map(([unit, count]) => ({ unit, count }))
          .sort((a, b) => b.count - a.count)
      );
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName, selectedEvidenceTypes, selectedSourceKind, searchTerm]);

  const chooseSourceKind = (kind: string) => setSelectedSourceKind(kind);
  const toggleUnit = (unit: string) => {
    setSelectedUnits((prev) =>
      prev.includes(unit) ? prev.filter((u) => u !== unit) : [...prev, unit]
    );
  };
  const clearUnits = () => setSelectedUnits([]);
  const toggleEvidenceType = (etype: string) => {
    setSelectedEvidenceTypes((prev) =>
      prev.includes(etype) ? prev.filter((e) => e !== etype) : [...prev, etype]
    );
  };
  const clearEvidenceTypes = () => setSelectedEvidenceTypes([]);
  const visibleUnits = showAllUnits
    ? unitOptions
    : unitOptions.slice(0, TOP_UNITS);
  const hiddenUnitsCount = Math.max(0, unitOptions.length - TOP_UNITS);

  const searchInput = (
    <div className="relative flex items-center">
      <MdSearch className="absolute left-2 w-4 h-4 text-light-400" />
      <input
        className="pl-8 pr-8 w-full h-8 text-xs rounded-md border border-light-700/60 bg-light-900/60 focus:bg-light-900 focus:border-light-500 hover:border-light-500 text-light-100 placeholder-light-500 transition-colors duration-100 ease-in-out outline-none"
        type="text"
        placeholder="Search…"
        aria-label="Search bioactivity or chemical"
        value={searchTerm}
        onChange={(e) => setSearchTerm(e.target.value.toLowerCase())}
      />
      {searchTerm && (
        <button
          type="button"
          aria-label="Clear search"
          onClick={() => setSearchTerm("")}
          className="absolute right-2 flex items-center justify-center w-4 h-4 rounded-full text-light-400 hover:text-light-100 hover:bg-light-700 transition-colors"
        >
          <MdClose className="w-3 h-3" />
        </button>
      )}
    </div>
  );

  const sourceFilter = (
    <FilterGroup label="Assay Source">
      {/* Single-select, hence the radio affordance. */}
      <FilterOptionList mode="radio" ariaLabel="Assay Source">
        {SOURCE_KINDS.map(({ key, label }) => {
          const c =
            sourceKindCounts === null
              ? undefined
              : key === ""
              ? sourceKindCounts.both
              : key === "experimental"
              ? sourceKindCounts.experimental
              : sourceKindCounts.predicted;
          return (
            <FilterOption
              key={label}
              mode="radio"
              label={label}
              count={c}
              countsLoaded={sourceKindCounts !== null}
              selected={selectedSourceKind === key}
              disabled={typeof c === "number" && key !== "" && c === 0}
              onClick={() => chooseSourceKind(key)}
            />
          );
        })}
      </FilterOptionList>
    </FilterGroup>
  );


  const unitFilter = unitOptions.length > 0 && (
    <FilterGroup
      label="Unit"
      onClear={selectedUnits.length > 0 ? clearUnits : undefined}
    >
      <FilterOptionList>
        {visibleUnits.map(({ unit, count }) => (
          <FilterOption
            key={unit}
            label={unit}
            count={count}
            selected={selectedUnits.includes(unit)}
            onClick={() => toggleUnit(unit)}
            capitalize={false}
          />
        ))}
        {!showAllUnits && hiddenUnitsCount > 0 && (
          <button
            type="button"
            onClick={() => setShowAllUnits(true)}
            className="mt-1 px-1 py-1 text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors text-left"
          >
            {hiddenUnitsCount} more…
          </button>
        )}
        {showAllUnits && unitOptions.length > TOP_UNITS && (
          <button
            type="button"
            onClick={() => setShowAllUnits(false)}
            className="mt-1 px-1 py-1 text-[11px] font-mono italic text-light-400 hover:text-light-100 underline-offset-4 hover:underline transition-colors text-left"
          >
            collapse
          </button>
        )}
      </FilterOptionList>
    </FilterGroup>
  );

  const isFiltersDirty =
    searchTerm !== "" ||
    selectedUnits.length > 0 ||
    selectedEvidenceTypes.length > 0 ||
    selectedSourceKind !== "";
  const resetAllFilters = () => {
    setSearchTerm("");
    setSelectedUnits([]);
    setSelectedEvidenceTypes([]);
    setSelectedSourceKind("");
  };

  const evidenceFilter = evidenceTypeOptions.length > 0 && (
    <FilterGroup
      label="Evidence"
      onClear={
        selectedEvidenceTypes.length > 0 ? clearEvidenceTypes : undefined
      }
    >
      <FilterOptionList>
        {evidenceTypeOptions.map(({ evidence_type, count }) => (
          <FilterOption
            key={evidence_type}
            label={evidence_type}
            count={count}
            selected={selectedEvidenceTypes.includes(evidence_type)}
            onClick={() => toggleEvidenceType(evidence_type)}
          />
        ))}
      </FilterOptionList>
    </FilterGroup>
  );

  const filters = (
    <>
      {unitFilter}
      {evidenceFilter}
      {sourceFilter}
    </>
  );

  return (
    <FilterPanel
      search={searchInput}
      filters={filters}
      isDirty={isFiltersDirty}
      onReset={resetAllFilters}
      open={mobileFiltersOpen}
      onOpenChange={setMobileFiltersOpen}
    >
      <div className="flex flex-col gap-12">

      <FoodBioactivitiesSection
        commonName={commonName}
        anchorId={anchorId}
        externalSearch={searchTerm}
        externalSourceKind={sourceKindParam}
        externalUnit={unitParam}
        externalEvidenceType={evidenceTypeParam}
        hideChrome
        onTotalRowsChange={setDirectTotal}
        onResetFilters={resetAllFilters}
      />
      <div className="border-t-2 border-double border-light-700/60" />
      <FoodInferredBioactivitiesSection
        commonName={commonName}
        externalSearch={searchTerm}
        externalSourceKind={sourceKindParam}
        externalUnit={unitParam}
        externalEvidenceType={evidenceTypeParam}
        hideChrome
        onTotalRowsChange={setInferredTotal}
        onResetFilters={resetAllFilters}
      />
      </div>
    </FilterPanel>
  );
};

FoodBioactivitiesTab.displayName = "FoodBioactivitiesTab";
export default FoodBioactivitiesTab;
