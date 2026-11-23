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

// Alphabetical by label, like every facet.
export const SIGNALS: { key: string; label: string }[] = [
  { key: MARKER, label: "Marker/mechanism" },
  { key: THERAPEUTIC, label: "Therapeutic" },
];

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
            // `?? 0` so a signal absent from the counts reads as zero
            // and greys out, rather than as "not loaded".
            count={countsLoaded ? (count ?? 0) : undefined}
            countsLoaded={countsLoaded}
            selected={selected.includes(key)}
            onClick={() => onToggle(key)}
          />
        );
      })}
    </FilterOptionList>
  </FilterGroup>
);

SignalFilterGroup.displayName = "SignalFilterGroup";
export default SignalFilterGroup;
