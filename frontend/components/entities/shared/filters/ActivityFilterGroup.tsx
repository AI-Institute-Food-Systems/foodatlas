"use client";

// The bioactivity facet for the assay-inferred table.
//
// The disease page used to carry these as a separate Bioactivities tab,
// listing the same chemicals at a finer grain: mv_disease_bioactivity is
// mv_chemical_disease_bioactivity split by what the assays measure, and
// the two hold the identical set of (chemical, disease) pairs — 347,632
// either way, set difference 0. The tab was therefore the same rows with
// one column added and chemicals repeated per activity.
//
// So the dimension became a facet and a per-row cell instead: one row per
// chemical, filterable by activity, which is the grain the other blocks
// on the tab already use.
//
// Multi-select for the same reason as Signal: 27% of pairs carry more
// than one activity, so the options overlap. Matching runs per assay, via
// assayGroups.ts: one assay must carry a selected activity and signal.

import {
  FACET_MAX_HEIGHT,
  FilterGroup,
  FilterOption,
  FilterOptionList,
} from "@/components/entities/shared/filters/FilterControls";
import { sortFacetOptions } from "@/components/entities/shared/filters/facetOptions";

/** The distinct activities across a row set. */
export const activitiesOf = (
  rows: { bioactivities?: string[] }[]
): string[] =>
  Array.from(new Set(rows.flatMap((row) => row.bioactivities ?? [])));

interface Props {
  selected: string[];
  counts: Record<string, number>;
  onToggle: (key: string) => void;
  onClear: () => void;
  countsLoaded?: boolean;
}

const ActivityFilterGroup = ({
  selected,
  counts,
  onToggle,
  onClear,
  countsLoaded = true,
}: Props) => {
  // Alphabetical. Busiest-first was the rule here once, on the argument
  // that the tail is long — but the counts move with every other filter,
  // so the options reshuffled on every click. The tail scrolls instead.
  const options = sortFacetOptions(Object.entries(counts), ([name]) => name);
  if (options.length === 0) return null;

  return (
    <FilterGroup
      label="Activity"
      onClear={selected.length > 0 ? onClear : undefined}
    >
      <FilterOptionList maxHeightClass={FACET_MAX_HEIGHT}>
        {options.map(([activity, count]) => (
          <FilterOption
            key={activity}
            label={activity}
            count={count}
            countsLoaded={countsLoaded}
            selected={selected.includes(activity)}
            onClick={() => onToggle(activity)}
          />
        ))}
      </FilterOptionList>
    </FilterGroup>
  );
};

ActivityFilterGroup.displayName = "ActivityFilterGroup";
export default ActivityFilterGroup;
