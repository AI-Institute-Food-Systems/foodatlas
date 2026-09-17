// The premise highlights whole words.
//
// Onion's L-Serine row is extracted as "S" (sulfur), and the highlighter
// matched terms wherever they occurred — so every s in "Onions grown in
// nutrient solutions supplemented with…" lit up cyan. Terms now match
// only between non-letters, Greek spellings and punctuation-bounded
// terms included, and a plural still counts as its singular.

import { render } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { highlightPremise } from "@/components/entities/food/highlightPremise";
import type { FoodEvidence } from "@/types/Evidence";

const evidence = (
  premise: string,
  terms: { chemical?: string; food?: string; concentration?: string }
): FoodEvidence => ({
  premise,
  extraction: [
    {
      attestation_id: "a1",
      extracted_chemical_name: terms.chemical ?? null,
      extracted_food_name: terms.food ?? null,
      extracted_concentration: terms.concentration ?? null,
      converted_concentration: { unit: "mg/100g", value: 1 },
      method: "lit2kg",
    },
  ],
  reference: { id: "r1", source_name: "FoodAtlas", display_name: "", url: "" },
});

// What got highlighted, by colour: the text of every amber (food) /
// cyan (chemical) / teal (concentration) span, in order.
const highlights = (ev: FoodEvidence) => {
  const { container } = render(<p>{highlightPremise(ev)}</p>);
  const of = (colour: string) =>
    Array.from(container.querySelectorAll(`span.text-${colour}`)).map(
      (el) => el.textContent
    );
  return { food: of("amber-500"), chemical: of("cyan-400"), concentration: of("teal-400") };
};

describe("highlightPremise", () => {
  it("lights a one-letter chemical only where it stands alone", () => {
    const h = highlights(
      evidence(
        "Onions grown in nutrient solutions showed an increase in S content between selenate and S.",
        { chemical: "S", food: "onion" }
      )
    );
    expect(h.chemical).toEqual(["S", "S"]);
    expect(h.food).toEqual(["Onions"]);
  });

  it("counts a plural as its singular, either ending", () => {
    const h = highlights(
      evidence("Tomatoes and onions both carry glucoses.", {
        food: "tomato",
        chemical: "glucose",
      })
    );
    expect(h.food).toEqual(["Tomatoes"]);
    expect(h.chemical).toEqual(["glucoses"]);
  });

  it("still finds the Greek spelling, and a term bounded by punctuation", () => {
    const h = highlights(
      evidence("(+)-catechin and α-tocopherol were quantified (5.4-5.9 g/kg).", {
        chemical: "alpha-tocopherol",
        food: "(+)-catechin",
        concentration: "5.4-5.9 g/kg",
      })
    );
    expect(h.chemical).toEqual(["α-tocopherol"]);
    expect(h.food).toEqual(["(+)-catechin"]);
    expect(h.concentration).toEqual(["5.4-5.9 g/kg"]);
  });

  it("finds a term at a hyphen but not inside another word", () => {
    const h = highlights(
      evidence("quercetin-3-glucoside, not isoquercetin, was measured.", {
        chemical: "quercetin",
      })
    );
    expect(h.chemical).toEqual(["quercetin"]);
  });

  it("prefers the longer of two nested terms", () => {
    const h = highlights(
      evidence("Fresh onion juice was pressed from onion bulbs.", {
        food: "onion juice",
        chemical: "onion",
      })
    );
    expect(h.food).toEqual(["onion juice"]);
    expect(h.chemical).toEqual(["onion"]);
  });

  it("returns the sentence untouched when there is nothing to find", () => {
    const { container } = render(
      <p>{highlightPremise(evidence("Nothing here.", {}))}</p>
    );
    expect(container.querySelector("span")).toBeNull();
    expect(container.textContent).toBe("Nothing here.");
  });
});
