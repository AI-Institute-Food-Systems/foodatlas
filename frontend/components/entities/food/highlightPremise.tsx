// Colour-codes the extraction's terms — chemical, food, concentration —
// where they appear in the paper's sentence, Greek/Latin spellings
// included. Was inlined in EvidenceTable; hoisted so it can be tested
// as the pure function it is.

import type { ReactNode } from "react";

import type { FoodEvidence } from "@/types/Evidence";
import { greekVariants, matchesWithGreek } from "@/utils/greekLetters";

// Whole words only. A one-letter chemical ("S", sulfur) used to light
// up every s in the sentence, because terms were matched wherever they
// occurred. \b is not the boundary we want — Greek letters aren't \w,
// and terms start and end in punctuation ("(+)-catechin", "5.4-5.9
// g/kg") — so it is "no letter or digit on either side". A plural still
// counts: "Onions" is onion, "tomatoes" is tomato.
const NOT_WORD_BEFORE = "(?<![\\p{L}\\p{N}])";
const NOT_WORD_AFTER = "(?![\\p{L}\\p{N}])";
const PLURAL = "(?:es|s)?";

// The forms a matched part is looked up under: as written, and minus
// either plural ending — "glucoses" is glucose, "tomatoes" is tomato.
const singulars = (part: string): string[] => [
  part,
  part.replace(/s$/i, ""),
  part.replace(/es$/i, ""),
];

const matches = (part: string, name: string | null | undefined): boolean =>
  singulars(part).some((p) => matchesWithGreek(p, name));

export const highlightPremise = (evidence: FoodEvidence): ReactNode => {
  const terms = evidence.extraction
    .flatMap((e) =>
      [
        e.extracted_chemical_name,
        e.extracted_food_name,
        e.extracted_concentration,
      ].flatMap((name) => greekVariants(name))
    )
    // Longest first, so "onion juice" wins over "onion" where both fit.
    .sort((a, b) => b.length - a.length);
  if (terms.length === 0) return evidence.premise;
  // Safe to interpolate without escaping here: greekVariants() returns
  // already-escaped strings. Escaping again would double the backslashes
  // and stop matching anything.
  const regex = new RegExp(
    `${NOT_WORD_BEFORE}((?:${terms.join("|")})${PLURAL})${NOT_WORD_AFTER}`,
    "giu"
  );
  return evidence.premise.split(regex).map((part, index) => {
    const match = evidence.extraction.find(
      (e) =>
        matches(part, e.extracted_food_name) ||
        matches(part, e.extracted_chemical_name) ||
        matches(part, e.extracted_concentration)
    );
    if (matches(part, match?.extracted_food_name)) {
      return (
        <span key={index} className="text-amber-500 bg-amber-500/10">
          {part}
        </span>
      );
    }
    if (matches(part, match?.extracted_chemical_name)) {
      return (
        <span key={index} className="text-cyan-400 bg-cyan-500/10">
          {part}
        </span>
      );
    }
    if (matches(part, match?.extracted_concentration)) {
      return (
        <span key={index} className="text-teal-400 bg-teal-500/10">
          {part}
        </span>
      );
    }
    return part;
  });
};
