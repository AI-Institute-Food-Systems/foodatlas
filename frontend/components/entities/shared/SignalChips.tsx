"use client";

// The direction of a bioactivity↔disease link, as CTD classified it.
//
// There are exactly two values and they point opposite ways:
//
//   therapeutic       the chemical treats or mitigates the disease
//   marker/mechanism  the chemical marks or drives it — often aggravating
//
// Rendering both in the same neutral grey (which is what the two duplicate
// copies of this component used to do) reads as a single vague "associated
// with" and loses the one bit a reader actually needs. Tone carries the
// direction; the tooltip states CTD's definition of the value and stops
// there, rather than editorialising about what it does or does not imply.

import { Tooltip } from "@/components/basic/Tooltip";

export const THERAPEUTIC = "therapeutic";
export const MARKER = "marker/mechanism";

const TONE: Record<string, string> = {
  [THERAPEUTIC]: "text-emerald-300 border-emerald-800/70",
  [MARKER]: "text-amber-300 border-amber-800/70",
};

// CTD's own gloss on its two DirectEvidence values, and nothing beyond it.
const EXPLANATION: Record<string, string> = {
  [THERAPEUTIC]:
    "CTD direct evidence: the chemical treats or mitigates this disease.",
  [MARKER]:
    "CTD direct evidence: the chemical marks or is mechanistically involved in this disease.",
};

const chipClass = (relationship: string) =>
  `text-[9px] font-mono italic uppercase tracking-[0.1em] rounded-full px-1.5 py-[1px] border ${
    TONE[relationship] ?? "text-light-300 border-light-700"
  }`;

interface Props {
  relationships: string[];
}

const SignalChips = ({ relationships }: Props) => {
  if (!relationships?.length) return null;
  return (
    <span className="inline-flex flex-wrap gap-1">
      {relationships.map((relationship) =>
        EXPLANATION[relationship] ? (
          <Tooltip key={relationship} content={EXPLANATION[relationship]}>
            <span className={chipClass(relationship)}>{relationship}</span>
          </Tooltip>
        ) : (
          // An unrecognised value still renders — better a visible unknown
          // class than silently dropping evidence we were handed.
          <span key={relationship} className={chipClass(relationship)}>
            {relationship}
          </span>
        ),
      )}
    </span>
  );
};

SignalChips.displayName = "SignalChips";
export default SignalChips;
