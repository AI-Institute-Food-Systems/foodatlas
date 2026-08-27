"use client";

// Whether CTD *literature* independently backs an assay-inferred link.
//
// The two sources are genuinely independent: this row exists because a
// chemical was Active in an assay bridged to the disease, whereas the
// literature direction comes from curated publications. Only ~2.5% of
// assay-inferred pairs appear in both, so a match is a real differentiator
// rather than a decoration — which is exactly why it renders as nothing at
// all for the other 97.5% instead of an "unknown" chip nobody can act on.
//
// Both sides use the same two-value vocabulary, so they can be compared and
// not merely counted. "Differs" is only claimed when both sides actually
// state a direction and those directions don't overlap; anything short of
// that is reported as plain corroboration.

import { Tooltip } from "@/components/basic/Tooltip";
import { MARKER, THERAPEUTIC } from "./SignalChips";

const LABEL: Record<string, string> = {
  [THERAPEUTIC]: "therapeutic",
  [MARKER]: "marker/mechanism",
};

type Verdict = {
  text: string;
  tone: string;
  explanation: string;
};

export const verdictFor = (
  relationships: string[] | undefined,
  literature: string[] | undefined,
): Verdict | null => {
  if (!literature?.length) return null;

  const lit = literature.map((d) => LABEL[d] ?? d);
  const assay = (relationships ?? []).filter((r) => r in LABEL);
  const litText = lit.join(" + ");

  if (!assay.length) {
    return {
      text: "in literature",
      tone: "text-sky-300 border-sky-800/70",
      explanation: `CTD literature records this pair as ${litText}. The assay evidence carries no direction to compare it against.`,
    };
  }

  const overlap = assay.some((r) => literature.includes(r));
  return overlap
    ? {
        text: "literature agrees",
        tone: "text-sky-300 border-sky-800/70",
        explanation: `CTD literature independently records this pair as ${litText}, matching the assay evidence. Only about 2.5% of assay-inferred pairs appear in the literature at all.`,
      }
    : {
        text: "literature differs",
        tone: "text-rose-300 border-rose-800/70",
        explanation: `CTD literature records this pair as ${litText}, while the assay evidence points the other way (${assay.join(" + ")}). Worth reading both sources before drawing a conclusion.`,
      };
};

interface Props {
  relationships?: string[];
  literatureDirections?: string[];
}

const LiteratureBadge = ({ relationships, literatureDirections }: Props) => {
  const verdict = verdictFor(relationships, literatureDirections);
  if (!verdict) return null;
  return (
    <Tooltip content={verdict.explanation}>
      <span
        className={`text-[9px] font-mono italic uppercase tracking-[0.1em] rounded-full border px-1.5 py-[1px] ${verdict.tone}`}
      >
        {verdict.text}
      </span>
    </Tooltip>
  );
};

LiteratureBadge.displayName = "LiteratureBadge";
export default LiteratureBadge;
