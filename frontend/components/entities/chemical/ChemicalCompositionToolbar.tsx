"use client";

import { MdSearch } from "react-icons/md";

import Chip from "@/components/basic/Chip";
import SortListbox from "@/components/basic/SortListbox";
import {
  FilterGroup,
  ToggleSwitch,
} from "@/components/entities/shared/CompositionFilterControls";
import {
  SortColumn,
  SortDirection,
  SourceKey,
} from "@/utils/chemicalComposition";

// The three pieces of table chrome, exported separately because they live
// in different places at different widths — the filters move into a sticky
// left sidebar at min-[1440px] and into a drawer below it, while search
// stays inline throughout so typing never requires opening a panel. That
// arrangement mirrors the food composition table exactly; see
// FoodCompositionSection for the original.

// Mobile has no clickable column headers, so the sort lives here instead.
const SORT_OPTIONS = [
  { value: "median_concentration:desc", label: "Concentration (high → low)" },
  { value: "median_concentration:asc", label: "Concentration (low → high)" },
  { value: "name:asc", label: "Food (A → Z)" },
  { value: "name:desc", label: "Food (Z → A)" },
  { value: "evidence_count:desc", label: "Evidence (most first)" },
  { value: "evidence_count:asc", label: "Evidence (fewest first)" },
];

export const CompositionSearchInput = ({
  search,
  onSearchChange,
}: {
  search: string;
  onSearchChange: (value: string) => void;
}) => (
  <div className="flex items-center gap-2 rounded-full bg-light-800 py-1.5 px-3">
    <MdSearch className="text-light-400 flex-shrink-0" />
    <input
      type="search"
      value={search}
      onChange={(e) => onSearchChange(e.target.value)}
      placeholder="Search foods"
      aria-label="Search foods"
      className="w-full bg-transparent text-sm text-light-100 placeholder:text-light-500 focus:outline-none"
    />
  </div>
);

export const CompositionFilterPanel = ({
  sourceCounts,
  selectedSources,
  onToggleSource,
  unmeasuredCount,
  includeUnmeasured,
  onToggleUnmeasured,
}: {
  sourceCounts: { key: SourceKey; label: string; count: number }[];
  selectedSources: SourceKey[];
  onToggleSource: (key: SourceKey) => void;
  unmeasuredCount: number;
  includeUnmeasured: boolean;
  onToggleUnmeasured: () => void;
}) => (
  <div className="flex flex-col gap-4">
    {/* FilterGroup/ToggleSwitch are the food table's own controls, shared
      * rather than reimplemented, so both composition sidebars behave and
      * read identically. The toggle also wraps, which a pill chip does not
      * — "Include without concentration" overflowed the w-48 sidebar. */}
    {unmeasuredCount > 0 && (
      <FilterGroup label="Include">
        <ToggleSwitch
          label="Without concentration"
          count={unmeasuredCount}
          checked={includeUnmeasured}
          onChange={onToggleUnmeasured}
        />
      </FilterGroup>
    )}

    <FilterGroup label="Source">
      <div className="flex flex-wrap gap-1.5">
        {/* A source with no rows stays visible but inert: hiding it would
          * make the facet list change shape between chemicals, and leaving
          * it clickable offers a filter whose only outcome is an empty
          * table. */}
        {sourceCounts.map(({ key, label, count }) => (
          <Chip
            key={key}
            label={label}
            count={count}
            tone={selectedSources.includes(key) ? "cream" : "outline"}
            size="md"
            disabled={count === 0}
            onClick={count === 0 ? undefined : () => onToggleSource(key)}
            aria-pressed={selectedSources.includes(key)}
          />
        ))}
      </div>
    </FilterGroup>
  </div>
);

export const CompositionMobileSort = ({
  sort,
  onSortChange,
}: {
  sort: { column: SortColumn; direction: SortDirection };
  onSortChange: (sort: {
    column: SortColumn;
    direction: SortDirection;
  }) => void;
}) => (
  <div className="md:hidden">
    <SortListbox
      ariaLabel="Sort foods"
      value={`${sort.column}:${sort.direction}`}
      options={SORT_OPTIONS}
      onChange={(value) => {
        const [column, direction] = value.split(":");
        onSortChange({
          column: column as SortColumn,
          direction: direction as SortDirection,
        });
      }}
    />
  </div>
);
