"use client";

// "Bioactivities" tab on disease pages.
//
// Answers what the other disease tabs don't: which biological activities this
// disease's assay evidence actually measures, and which chemicals carry them.
//
// The attribution is assay-level on purpose. The Chemicals tab lists chemicals
// — from CTD literature and from shared assays — but either way drops what the
// assays were measuring. Going disease → chemical → all of that chemical's
// bioactivities would credit melanoma with 1,571 "antiviral" chemicals;
// attributing through the assay that bridges to the disease gives 3.
//
// Scope is deliberately narrow: assay counts only. Attaching each chemical's
// best food dose was tried and pulled — see the repository docstring for why.
//
// Chrome is the shared FilterPanel, like every other filtered table in the
// app. It used to be a hand-rolled row of Chip pills above the table: no
// search, no reset, and a facet list that pushed the table down as it grew.
// The activity picker is single-select, so it is a radio group — the same
// affordance the Direction facet uses on the Chemicals tab.

import { useEffect, useMemo, useState } from "react";
import { MdInfoOutline } from "react-icons/md";

import {
  ClearFiltersLink,
  FilterGroup,
  FilterOption,
  FilterOptionList,
  FilterSearchInput,
} from "@/components/entities/shared/filters/FilterControls";
import FilterPanel from "@/components/entities/shared/filters/FilterPanel";
import DiseaseBioactivityTable from "@/components/entities/disease/DiseaseBioactivityTable";
import { usePublishTabCount } from "@/context/tabCountsContext";
import {
  getDiseaseBioactivities,
  getDiseaseBioactivityChemicals,
} from "@/utils/fetching";
import type {
  DiseaseBioactivityChemical,
  DiseaseBioactivitySummary,
} from "@/types";

interface Props {
  commonName: string;
}

const PAGE_SIZE = 50;
const ALL = "__all__";

// The largest disease carries ~20 activities. Scrolling the group keeps the
// sidebar shorter than the table it filters instead of running past it.
const FACET_MAX_HEIGHT = "max-h-64";

const DiseaseBioactivitiesSection = ({ commonName }: Props) => {
  const [summary, setSummary] = useState<DiseaseBioactivitySummary[]>([]);
  const [rows, setRows] = useState<DiseaseBioactivityChemical[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  const [bioactivity, setBioactivity] = useState<string>(ALL);
  const [searchTerm, setSearchTerm] = useState("");
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);
  const [visibleCount, setVisibleCount] = useState(PAGE_SIZE);

  // Distinct activities, not chemical rows — see tabCounts.diseaseBioactivitiesCount,
  // which prefetches the same number. Unfiltered on purpose: the badge names
  // what this tab covers, and it is read from the tab strip, where the
  // filters that would explain a smaller number are not visible.
  usePublishTabCount("bioactivities", isLoading ? null : summary.length);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    (async () => {
      const [summaryPayload, rowsPayload] = await Promise.all([
        getDiseaseBioactivities(commonName),
        getDiseaseBioactivityChemicals(commonName),
      ]);
      if (cancelled) return;
      setSummary(summaryPayload?.data ?? []);
      setRows(rowsPayload?.data ?? []);
      setIsLoading(false);
    })();
    return () => {
      cancelled = true;
    };
  }, [commonName]);

  // Filtering client-side: the payload is already in memory, and re-fetching
  // per facet would trade a one-frame filter for a round trip.
  const searched = useMemo(() => {
    const needle = searchTerm.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter(
      (row) =>
        row.chemical_name.toLowerCase().includes(needle) ||
        row.bioactivity_name.toLowerCase().includes(needle)
    );
  }, [rows, searchTerm]);

  const filtered = useMemo(
    () =>
      bioactivity === ALL
        ? searched
        : searched.filter((row) => row.bioactivity_name === bioactivity),
    [searched, bioactivity]
  );

  // Counted under the active search but NOT the active activity, so the
  // option you are about to pick never reads zero. One pass rather than one
  // per option — the largest disease has 6k rows behind 20 activities.
  const counts = useMemo(() => {
    const map = new Map<string, number>();
    for (const row of searched) {
      map.set(row.bioactivity_name, (map.get(row.bioactivity_name) ?? 0) + 1);
    }
    map.set(ALL, searched.length);
    return map;
  }, [searched]);

  useEffect(() => setVisibleCount(PAGE_SIZE), [bioactivity, searchTerm]);

  const isDirty = searchTerm !== "" || bioactivity !== ALL;
  const resetAllFilters = () => {
    setSearchTerm("");
    setBioactivity(ALL);
  };

  // Nothing for this disease at all — no filters to offer, so no panel.
  if (!isLoading && rows.length === 0) {
    return (
      <p className="text-sm text-light-500 italic">
        No assay-attributed bioactivities for{" "}
        <span className="capitalize">{commonName}</span> in the current data.
      </p>
    );
  }

  const searchInput = (
    <FilterSearchInput
      value={searchTerm}
      onChange={setSearchTerm}
      onClear={() => setSearchTerm("")}
      placeholder="Search…"
      ariaLabel="Search chemicals and activities"
    />
  );

  const activityFilter = summary.length > 0 && (
    <FilterGroup label="Activity">
      {/* Single-select, hence the radio affordance: a row belongs to exactly
       * one activity, so two selected at once would mean the union, which
       * "All" already is. */}
      <FilterOptionList
        mode="radio"
        ariaLabel="Activity"
        maxHeightClass={FACET_MAX_HEIGHT}
      >
        <FilterOption
          mode="radio"
          label="All"
          count={counts.get(ALL) ?? 0}
          selected={bioactivity === ALL}
          onClick={() => setBioactivity(ALL)}
        />
        {summary.map((s) => {
          const count = counts.get(s.bioactivity_name) ?? 0;
          return (
            <FilterOption
              key={s.bioactivity_foodatlas_id}
              mode="radio"
              // Names arrive lowercase from the KG ("anticancer") and the
              // table cells capitalize, so the facet has to as well.
              label={s.bioactivity_name}
              count={count}
              selected={bioactivity === s.bioactivity_name}
              disabled={count === 0}
              onClick={() => setBioactivity(s.bioactivity_name)}
            />
          );
        })}
      </FilterOptionList>
    </FilterGroup>
  );

  const showFilteredEmpty = !isLoading && filtered.length === 0;

  return (
    <FilterPanel
      search={searchInput}
      filters={activityFilter}
      isDirty={isDirty}
      onReset={resetAllFilters}
      open={mobileFiltersOpen}
      onOpenChange={setMobileFiltersOpen}
    >
      <div className="flex flex-col gap-5">
        <p className="text-sm text-light-400 leading-relaxed max-w-2xl">
          Biological activities measured by the assays that link{" "}
          <span className="capitalize">{commonName}</span> to chemicals. A row
          means the chemical was <em>Active</em> in an assay that both bridges
          to this disease and is classified under that activity — so the
          activity is one this disease&apos;s own evidence measured, not merely
          something the chemical does elsewhere. Many of these chemicals are
          pharmaceuticals rather than food constituents.
        </p>

        {showFilteredEmpty ? (
          <div className="flex flex-col items-center gap-2 py-8 text-light-300">
            <div className="flex items-center gap-2 text-sm">
              <MdInfoOutline />
              No bioactivities match your filters
            </div>
            <ClearFiltersLink onClick={resetAllFilters} />
          </div>
        ) : (
          <DiseaseBioactivityTable
            rows={filtered}
            visibleCount={visibleCount}
            onShowAll={() => setVisibleCount(filtered.length)}
            commonName={commonName}
            isLoading={isLoading}
          />
        )}
      </div>
    </FilterPanel>
  );
};

DiseaseBioactivitiesSection.displayName = "DiseaseBioactivitiesSection";
export default DiseaseBioactivitiesSection;
