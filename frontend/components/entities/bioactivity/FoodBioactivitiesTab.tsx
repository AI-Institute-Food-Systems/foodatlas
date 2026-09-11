"use client";

// Shared search + source-kind filter chrome for BOTH the direct
// FoodBioactivitiesSection and the FoodInferredBioactivitiesSection.
// Instead of each section owning its own sidebar (as they do
// standalone), this component hosts one sidebar aside + one mobile
// drawer and drives both tables via `externalSearch` /
// `externalSourceKind` / `hideChrome` props.

import { useCallback, useEffect, useMemo, useState } from "react";
import { MdCheck, MdClose, MdTune } from "react-icons/md";
import { twMerge } from "tailwind-merge";

import {
  FACET_MAX_HEIGHT,
  FilterGroup,
  FilterOption,
  FilterOptionList,
  FilterSearchInput,
} from "@/components/entities/shared/filters/FilterControls";
import FilterPanel from "@/components/entities/shared/filters/FilterPanel";
import { sumFacetCounts } from "@/components/entities/shared/filters/facetOptions";
import FoodBioactivitiesSection from "@/components/entities/bioactivity/FoodBioactivitiesSection";
import FoodInferredBioactivitiesSection from "@/components/entities/bioactivity/FoodInferredBioactivitiesSection";
import { useServerFacetOptions } from "@/hooks/useServerFacetOptions";
import {
  getBioactivityEndpointOptions,
  getBioactivityEvidenceTypeCounts,
  getBioactivitySourceKindCounts,
  NO_SIDEBAR_FILTERS,
  type BioactivitySidebarFilters,
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

const FoodBioactivitiesTab = ({ commonName, anchorId }: Props) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedSourceKind, setSelectedSourceKind] = useState<string>("");
  const [selectedUnits, setSelectedUnits] = useState<string[]>([]);
  const [selectedEvidenceTypes, setSelectedEvidenceTypes] = useState<string[]>(
    []
  );
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

  // Evidence-type and Unit facets, each aggregated across BOTH tables
  // (direct + inferred) since the sidebar drives both: each direction
  // returns its own counts and they are summed per value. The full option
  // set is fetched once per food and the faceted counts laid over it —
  // see useServerFacetOptions. Unit is the wide one: direct rows are
  // food-level (usually just "mmol/100g"), inferred rows carry every
  // measurement for every chemical in the food (IC50 uM/nM, MIC ug/mL…).
  const fetchEvidenceTypeCounts = useCallback(
    async (f: BioactivitySidebarFilters) => {
      if (!commonName) return [];
      const [direct, inferred] = await Promise.all([
        getBioactivityEvidenceTypeCounts(commonName, "food-bioactivities", f),
        getBioactivityEvidenceTypeCounts(
          commonName,
          "food-inferred-bioactivities",
          f
        ),
      ]);
      const asFacet = (o: { evidence_type: string; count: number }) => ({
        value: o.evidence_type,
        count: o.count,
      });
      return sumFacetCounts(direct.map(asFacet), inferred.map(asFacet));
    },
    [commonName]
  );
  const evidenceFacetFilters = useMemo<BioactivitySidebarFilters>(
    () => ({
      filterUnit: unitParam,
      filterSourceKind: selectedSourceKind,
      search: searchTerm,
    }),
    [unitParam, selectedSourceKind, searchTerm]
  );
  const { options: evidenceTypeOptions, loaded: evidenceTypesLoaded } =
    useServerFacetOptions(
      fetchEvidenceTypeCounts,
      evidenceFacetFilters,
      NO_SIDEBAR_FILTERS
    );

  const fetchUnitCounts = useCallback(
    async (f: BioactivitySidebarFilters) => {
      if (!commonName) return [];
      const [direct, inferred] = await Promise.all([
        getBioactivityEndpointOptions(commonName, "food-bioactivities", f),
        getBioactivityEndpointOptions(
          commonName,
          "food-inferred-bioactivities",
          f
        ),
      ]);
      const asFacet = (o: { unit: string; count: number }) => ({
        value: o.unit,
        count: o.count,
      });
      return sumFacetCounts(direct.map(asFacet), inferred.map(asFacet));
    },
    [commonName]
  );
  const unitFacetFilters = useMemo<BioactivitySidebarFilters>(
    () => ({
      filterEvidenceType: evidenceTypeParam,
      filterSourceKind: selectedSourceKind,
      search: searchTerm,
    }),
    [evidenceTypeParam, selectedSourceKind, searchTerm]
  );
  const { options: unitOptions, loaded: unitsLoaded } = useServerFacetOptions(
    fetchUnitCounts,
    unitFacetFilters,
    NO_SIDEBAR_FILTERS
  );

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

  const searchInput = (
    <FilterSearchInput
      value={searchTerm}
      onChange={(v) => setSearchTerm(v.toLowerCase())}
      onClear={() => setSearchTerm("")}
      placeholder="Search…"
      ariaLabel="Search bioactivity or chemical"
    />
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
              resetOption={key === ""}
              onClick={() => chooseSourceKind(key)}
            />
          );
        })}
      </FilterOptionList>
    </FilterGroup>
  );

  // A group is omitted only when the food has NO values for the dimension
  // at all; options the current filters zero out stay, disabled.
  const unitFilter = unitOptions.length > 0 && (
    <FilterGroup
      label="Unit"
      onClear={selectedUnits.length > 0 ? clearUnits : undefined}
    >
      <FilterOptionList maxHeightClass={FACET_MAX_HEIGHT}>
        {unitOptions.map(({ value, count }) => (
          <FilterOption
            key={value}
            label={value}
            count={count}
            countsLoaded={unitsLoaded}
            selected={selectedUnits.includes(value)}
            onClick={() => toggleUnit(value)}
            capitalize={false}
          />
        ))}
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
        {evidenceTypeOptions.map(({ value, count }) => (
          <FilterOption
            key={value}
            label={value}
            count={count}
            countsLoaded={evidenceTypesLoaded}
            selected={selectedEvidenceTypes.includes(value)}
            onClick={() => toggleEvidenceType(value)}
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
