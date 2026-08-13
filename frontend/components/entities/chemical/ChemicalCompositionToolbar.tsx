"use client";

import { MdSearch } from "react-icons/md";

import Chip from "@/components/basic/Chip";
import SortListbox from "@/components/basic/SortListbox";
import {
  SortColumn,
  SortDirection,
  SourceKey,
} from "@/utils/chemicalComposition";

// Mobile has no clickable column headers, so the sort lives here instead.
const SORT_OPTIONS = [
  { value: "median_concentration:desc", label: "Concentration (high → low)" },
  { value: "median_concentration:asc", label: "Concentration (low → high)" },
  { value: "name:asc", label: "Food (A → Z)" },
  { value: "name:desc", label: "Food (Z → A)" },
  { value: "evidence_count:desc", label: "Evidence (most first)" },
  { value: "evidence_count:asc", label: "Evidence (fewest first)" },
];

interface ChemicalCompositionToolbarProps {
  search: string;
  onSearchChange: (value: string) => void;
  sourceCounts: { key: SourceKey; label: string; count: number }[];
  selectedSources: SourceKey[];
  onToggleSource: (key: SourceKey) => void;
  unmeasuredCount: number;
  includeUnmeasured: boolean;
  onToggleUnmeasured: () => void;
  sort: { column: SortColumn; direction: SortDirection };
  onSortChange: (sort: {
    column: SortColumn;
    direction: SortDirection;
  }) => void;
}

const ChemicalCompositionToolbar = ({
  search,
  onSearchChange,
  sourceCounts,
  selectedSources,
  onToggleSource,
  unmeasuredCount,
  includeUnmeasured,
  onToggleUnmeasured,
  sort,
  onSortChange,
}: ChemicalCompositionToolbarProps) => (
  <>
    <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
      <div className="flex items-center gap-2 rounded-full bg-light-800 py-1.5 px-3 md:w-64">
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

      <div className="flex items-center gap-2 flex-wrap">
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
        {unmeasuredCount > 0 && (
          <Chip
            label="Include without concentration"
            count={unmeasuredCount}
            tone={includeUnmeasured ? "cream" : "outline"}
            size="md"
            aria-pressed={includeUnmeasured}
            onClick={onToggleUnmeasured}
          />
        )}
      </div>
    </div>

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
  </>
);

ChemicalCompositionToolbar.displayName = "ChemicalCompositionToolbar";

export default ChemicalCompositionToolbar;
