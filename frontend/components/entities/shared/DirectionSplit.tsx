"use client";

// The therapeutic / marker split for a row that rolls up many chemicals.
//
// At this grain a chip is the wrong instrument. "Does anticancer evidence for
// hepatocellular carcinoma include anything therapeutic?" is almost always
// yes, so a chip that says so distinguishes nothing. What separates the rows
// is proportion: breast neoplasms is 270 therapeutic of 1,612 chemicals,
// while precursor cell lymphoblastic leukemia-lymphoma is 0 of 1,142. A
// reader who only saw the totals would take those to mean the same thing.
//
// A chemical classified both ways counts in both, so the two numbers need not
// sum to the row's chemical total — hence "of N" rather than a stacked bar,
// which would imply parts of a whole.

import { Tooltip } from "@/components/basic/Tooltip";

interface Props {
  nTherapeutic: number;
  nMarker: number;
  // Chemicals whose link CTD literature also records, in any direction.
  nLiterature?: number;
  // Denominator for the "of N" reading.
  nChemicals: number;
}

const DirectionSplit = ({
  nTherapeutic,
  nMarker,
  nLiterature = 0,
  nChemicals,
}: Props) => (
  <span className="inline-flex flex-wrap items-baseline gap-x-2 gap-y-1 text-[11px] font-mono">
    <Tooltip
      content={`${nTherapeutic.toLocaleString()} of ${nChemicals.toLocaleString()} chemicals carry therapeutic direct evidence — the chemical treats or mitigates this disease.`}
    >
      <span className="text-emerald-300 tabular-nums">
        {nTherapeutic.toLocaleString()} ther.
      </span>
    </Tooltip>
    <Tooltip
      content={`${nMarker.toLocaleString()} of ${nChemicals.toLocaleString()} chemicals carry marker/mechanism direct evidence — the chemical marks or drives this disease.`}
    >
      <span className="text-amber-300 tabular-nums">
        {nMarker.toLocaleString()} mark.
      </span>
    </Tooltip>
    {nLiterature > 0 && (
      <Tooltip
        content={`${nLiterature.toLocaleString()} of these chemical links are also recorded in CTD literature, independently of the assay evidence.`}
      >
        <span className="text-sky-300 tabular-nums">
          {nLiterature.toLocaleString()} lit.
        </span>
      </Tooltip>
    )}
  </span>
);

DirectionSplit.displayName = "DirectionSplit";
export default DirectionSplit;
