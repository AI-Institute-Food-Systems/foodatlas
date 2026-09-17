// The Chemical column in the data-points table is one colour for every
// row, whatever the source and whatever the extracted name.
//
// It used to mute a row whose extracted name equalled the row's chemical.
// That reads as a per-source style, because FDC is normalised data — its
// extracted name ALWAYS equals the chemical (22/22 on staging) — while
// literature extractions mostly do not. Every FDC row came out greyer.

import { render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}), isSelectMode: false }),
}));

import EvidenceTable from "@/components/entities/food/EvidenceTable";
import type { FoodEvidence } from "@/types/Evidence";

const evidence = (
  source: "FDC" | "FoodAtlas" | "PTFI",
  extracted: string,
  id: string
): FoodEvidence => ({
  premise: "",
  extraction: [
    {
      attestation_id: id,
      extracted_chemical_name: extracted,
      extracted_food_name: "salmon",
      extracted_concentration: "1 mg",
      converted_concentration: { unit: "mg/100g", value: 1 },
      method: source.toLowerCase(),
    },
  ],
  reference: {
    id,
    source_name: source,
    display_name: `${source} ${id}`,
    url: "https://example.test",
  },
});

/** The colour class on each rendered chemical name, desktop table only. */
const nameClasses = (container: HTMLElement): string[] =>
  Array.from(container.querySelectorAll("tbody td:nth-child(2) > span")).map(
    (el) =>
      Array.from(el.classList).find((c) => /^text-light-\d+$/.test(c)) ?? ""
  );

describe("evidence table chemical names", () => {
  it("are the same colour whether or not the extracted name is the chemical", () => {
    const { container } = render(
      <EvidenceTable
        evidences={[
          // FDC: extracted name is the canonical chemical, as always.
          evidence("FDC", "zinc(2+)", "e1"),
          // FoodAtlas: a literature surface form that differs…
          evidence("FoodAtlas", "Zinc", "e2"),
          // …and one that happens to match.
          evidence("FoodAtlas", "zinc(2+)", "e3"),
          evidence("PTFI", "zinc(2+)", "e4"),
        ]}
      />
    );
    const classes = nameClasses(container);
    expect(classes).toHaveLength(4);
    expect(new Set(classes).size, `names rendered in ${classes.join(", ")}`).toBe(1);
    expect(classes[0]).toBe("text-light-100");
  });
});
