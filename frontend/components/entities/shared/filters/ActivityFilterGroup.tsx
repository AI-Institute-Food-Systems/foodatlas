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
// than one activity, so the options overlap and a row matches if it
// carries ANY selected value.

import {
  FilterGroup,
  FilterOption,
  FilterOptionList,
} from "@/components/entities/shared/filters/FilterControls";

// Long tail: the largest anchors carry ~20 activities. Scrolling keeps
// the sidebar shorter than the table it filters.
const FACET_MAX_HEIGHT = "max-h-56";

export const matchesActivities = (
  activities: string[] | undefined,
  selected: string[]
): boolean =>
  selected.length === 0 ||
  (activities ?? []).some((activity) => selected.includes(activity));

// Rows per activity, deduped within a row. Callers pass the set filtered
// by everything EXCEPT the activity selection, so an option never reads
// zero merely because another is picked.
export const countActivities = (
  rows: { bioactivities?: string[] }[]
): Record<string, number> => {
  const counts: Record<string, number> = {};
  for (const row of rows) {
    for (const activity of Array.from(new Set(row.bioactivities ?? []))) {
      counts[activity] = (counts[activity] ?? 0) + 1;
    }
  }
  return counts;
};

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
  // Busiest first, like every other facet here — the tail is long and
  // alphabetical order would bury the activities that carry the rows.
  const options = Object.entries(counts).sort(
    ([aName, a], [bName, b]) => b - a || aName.localeCompare(bName)
  );
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
            disabled={countsLoaded && count === 0}
            onClick={() => onToggle(activity)}
          />
        ))}
      </FilterOptionList>
    </FilterGroup>
  );
};

ActivityFilterGroup.displayName = "ActivityFilterGroup";
export default ActivityFilterGroup;
