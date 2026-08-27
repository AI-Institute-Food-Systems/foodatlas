"use client";

// The merged chemical↔disease evidence tab, used from both ends.
//
// Chemical and disease pages each used to carry two tabs over the same
// relationship — "Diseases"/"Chemicals" (CTD literature) and "Lab
// Activity" (shared bioactivity assays) — which read as two unrelated
// features rather than two sources for one question. This is the same
// shape FoodBioactivitiesTab already uses for direct vs inferred
// bioactivities: one FilterPanel, two stacked tables, one summed badge.
//
// The two tables are NOT merged row-wise. Their columns barely overlap
// (PMIDs and a direction on one side, assay/active counts, signal and
// target genes on the other), so one table would be mostly empty cells.

import { useEffect, useState } from "react";

import {
  FilterGroup,
  FilterOption,
  FilterOptionList,
  FilterSearchInput,
} from "@/components/entities/shared/filters/FilterControls";
import FilterPanel from "@/components/entities/shared/filters/FilterPanel";
import ChemicalAssayInferredSection from "@/components/entities/chemical/ChemicalAssayInferredSection";
import ChemicalCorrelationSection from "@/components/entities/chemical/ChemicalCorrelationSection";
import DiseaseAssayInferredSection from "@/components/entities/disease/DiseaseAssayInferredSection";
import DiseaseCorrelationsSection from "@/components/entities/disease/DiseaseCorrelationsSection";
import type { CorrelationDirection } from "@/components/entities/shared/CorrelationRow";
import { usePublishTabCount } from "@/context/tabCountsContext";
import { getCorrelationDirectionCounts } from "@/utils/fetching";
import { useDebouncedValue } from "@/hooks/useDebouncedValue";

interface Props {
  commonName: string;
  // Which page this is: "chemical" lists diseases, "disease" lists
  // chemicals.
  anchor: "chemical" | "disease";
}

const DIRECTIONS: { key: CorrelationDirection; label: string }[] = [
  { key: "all", label: "All" },
  { key: "positive", label: "Improves" },
  { key: "negative", label: "Worsens" },
];

const CorrelationEvidenceTab = ({ commonName, anchor }: Props) => {
  const [searchTerm, setSearchTerm] = useState("");
  const [direction, setDirection] = useState<CorrelationDirection>("all");
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  // The CTD half filters server-side, so every keystroke would be a
  // round-trip. The assay half filters in memory and doesn't care.
  const debouncedSearch = useDebouncedValue(searchTerm, 300);

  // Aggregated totals → the one tab badge. Each table reports null while
  // its fetch is in flight and the two resolve independently, so the sum
  // is only meaningful once BOTH have landed. Publishing early made the
  // badge show one table's count and then visibly jump — the same
  // problem FoodBioactivitiesTab documents.
  const [literatureTotal, setLiteratureTotal] = useState<number | null>(null);
  const [inferredTotal, setInferredTotal] = useState<number | null>(null);
  usePublishTabCount(
    "health",
    literatureTotal === null || inferredTotal === null
      ? null
      : literatureTotal + inferredTotal
  );

  // Direction facet counts. Under the active search but NOT the active
  // direction, so the option you're about to pick doesn't read zero.
  const [directionCounts, setDirectionCounts] = useState<{
    improves: number;
    worsens: number;
    both: number;
  } | null>(null);
  useEffect(() => {
    if (!commonName) return;
    let cancelled = false;
    (async () => {
      const counts = await getCorrelationDirectionCounts(
        commonName,
        anchor,
        debouncedSearch
      );
      if (!cancelled) setDirectionCounts(counts);
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName, anchor, debouncedSearch]);

  const peerLabel = anchor === "chemical" ? "disease" : "chemical";

  const searchInput = (
    <FilterSearchInput
      value={searchTerm}
      onChange={setSearchTerm}
      onClear={() => setSearchTerm("")}
      placeholder="Search…"
      ariaLabel={`Search ${peerLabel}`}
    />
  );

  const countFor = (key: CorrelationDirection) => {
    if (directionCounts === null) return undefined;
    if (key === "positive") return directionCounts.improves;
    if (key === "negative") return directionCounts.worsens;
    return directionCounts.both;
  };

  const directionFilter = (
    <FilterGroup label="Direction">
      {/* Single-select, hence the radio affordance. Applies to the
       * literature table only — an assay association has no direction. */}
      <FilterOptionList mode="radio" ariaLabel="Direction">
        {DIRECTIONS.map(({ key, label }) => {
          const count = countFor(key);
          return (
            <FilterOption
              key={key}
              mode="radio"
              label={label}
              count={count}
              countsLoaded={directionCounts !== null}
              selected={direction === key}
              disabled={
                typeof count === "number" && key !== "all" && count === 0
              }
              onClick={() => setDirection(key)}
            />
          );
        })}
      </FilterOptionList>
    </FilterGroup>
  );

  const isDirty = searchTerm !== "" || direction !== "all";
  const resetAllFilters = () => {
    setSearchTerm("");
    setDirection("all");
  };

  const literature =
    anchor === "chemical" ? (
      <ChemicalCorrelationSection
        commonName={commonName}
        direction={direction}
        search={debouncedSearch}
        onTotalRowsChange={setLiteratureTotal}
      />
    ) : (
      <DiseaseCorrelationsSection
        commonName={commonName}
        direction={direction}
        search={debouncedSearch}
        onTotalRowsChange={setLiteratureTotal}
      />
    );

  const inferred =
    anchor === "chemical" ? (
      <ChemicalAssayInferredSection
        commonName={commonName}
        search={debouncedSearch}
        onTotalRowsChange={setInferredTotal}
      />
    ) : (
      <DiseaseAssayInferredSection
        commonName={commonName}
        search={debouncedSearch}
        onTotalRowsChange={setInferredTotal}
      />
    );

  return (
    <FilterPanel
      search={searchInput}
      filters={directionFilter}
      isDirty={isDirty}
      onReset={resetAllFilters}
      open={mobileFiltersOpen}
      onOpenChange={setMobileFiltersOpen}
    >
      <div className="flex flex-col gap-12">
        {literature}
        <div className="border-t-2 border-double border-light-700/60" />
        {inferred}
      </div>
    </FilterPanel>
  );
};

CorrelationEvidenceTab.displayName = "CorrelationEvidenceTab";
export default CorrelationEvidenceTab;
