"use client";

// The CTD DirectEvidence facet, shared by every surface that renders the
// Signal column: the lab-assay tables on chemical and disease pages, and
// the disease Bioactivities tab.
//
// Multi-select rather than a radio, unlike the literature table's
// Direction facet next to it. Same r3/r4 vocabulary underneath, but a
// single row can carry BOTH values — a chemical that is therapeutic for a
// disease and also marks it — so the two options are not a partition and
// selecting both is not the same as selecting neither.
//
// Counts come from the caller because each surface holds its own rows;
// what is shared is the option list, the wording and the ANY-match rule.

import {
  FilterGroup,
  FilterOption,
  FilterOptionList,
} from "@/components/entities/shared/filters/FilterControls";
import { MARKER, THERAPEUTIC } from "@/components/entities/shared/SignalChips";

export const SIGNALS: { key: string; label: string }[] = [
  { key: THERAPEUTIC, label: "Therapeutic" },
  { key: MARKER, label: "Marker/mechanism" },
];

// Keep a row when it carries any selected signal. Empty selection keeps
// everything, which is what "no filter" has to mean for a facet whose
// options overlap.
export const matchesSignals = (
  relationships: string[] | undefined,
  selected: string[]
): boolean =>
  selected.length === 0 ||
  (relationships ?? []).some((relationship) => selected.includes(relationship));

// Rows per signal, deduped within a row. Callers pass the set filtered by
// everything EXCEPT the signal selection, so an option never reads zero
// merely because the other one is picked.
export const countSignals = (
  rows: { relationships?: string[] }[]
): Record<string, number> => {
  const counts: Record<string, number> = {};
  for (const row of rows) {
    // Array.from because the tsconfig target predates iterating a Set.
    for (const relationship of Array.from(new Set(row.relationships ?? []))) {
      counts[relationship] = (counts[relationship] ?? 0) + 1;
    }
  }
  return counts;
};

interface Props {
  selected: string[];
  counts: Record<string, number>;
  onToggle: (key: string) => void;
  onClear: () => void;
  // False while the counts are still in flight, so a row shows no count
  // rather than a misleading zero.
  countsLoaded?: boolean;
}

const SignalFilterGroup = ({
  selected,
  counts,
  onToggle,
  onClear,
  countsLoaded = true,
}: Props) => (
  <FilterGroup
    label="Signal"
    onClear={selected.length > 0 ? onClear : undefined}
  >
    <FilterOptionList>
      {SIGNALS.map(({ key, label }) => {
        const count = counts[key];
        return (
          <FilterOption
            key={key}
            label={label}
            count={count}
            countsLoaded={countsLoaded}
            selected={selected.includes(key)}
            disabled={countsLoaded && (count ?? 0) === 0}
            onClick={() => onToggle(key)}
          />
        );
      })}
    </FilterOptionList>
  </FilterGroup>
);

SignalFilterGroup.displayName = "SignalFilterGroup";
export default SignalFilterGroup;
