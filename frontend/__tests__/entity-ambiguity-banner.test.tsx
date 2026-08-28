// What the ambiguity banner is allowed to claim.
//
// `ambiguity_siblings` records extraction ambiguity, not nomenclature:
// materializer._build_sibling_map treats two entities as siblings when one
// raw name in one source resolved to both, then _components_from_candidates
// expands that transitively into connected components. So the banner must
// say the evidence may be misattributed — not that the name is a synonym —
// and must not present a chained 400-member cluster as if every member
// shares a name with this page.

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import EntityAmbiguityBanner from "@/components/entities/EntityAmbiguityBanner";

const siblings = (n: number) =>
  Array.from({ length: n }, (_, i) => ({
    foodatlas_id: `e${i}`,
    common_name: `sibling-${i}`,
  }));

describe("empty states", () => {
  it("renders nothing without siblings", () => {
    const { container } = render(
      <EntityAmbiguityBanner entityType="chemical" siblings={[]} />
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when the field is missing", () => {
    // Diseases never carry siblings — 0 of 3,177 rows.
    const { container } = render(
      <EntityAmbiguityBanner entityType="disease" siblings={undefined} />
    );
    expect(container).toBeEmptyDOMElement();
  });
});

describe("what it claims", () => {
  it("warns about attribution, not about the name", () => {
    render(
      <EntityAmbiguityBanner entityType="chemical" siblings={siblings(1)} />
    );
    expect(
      screen.getByText(/Some evidence here may belong to another chemical/)
    ).toBeInTheDocument();
    // The old copy's framing: a nomenclature claim plus navigation advice.
    expect(screen.queryByText(/This name is also used for/)).toBeNull();
    expect(screen.queryByText(/You may be looking for one of those/)).toBeNull();
  });

  it("says the evidence may also appear on the sibling's page", () => {
    // The pair is symmetric — both pages carry the same uncertain rows.
    render(
      <EntityAmbiguityBanner entityType="chemical" siblings={siblings(1)} />
    );
    expect(
      screen.getByText(/may appear on their pages too/)
    ).toBeInTheDocument();
  });
});

describe("large clusters", () => {
  it("caps the inline list instead of rendering every sibling", () => {
    // 709 chemicals have >100 siblings and the largest cluster is 401.
    // The old banner rendered all of them as inline links.
    render(
      <EntityAmbiguityBanner entityType="chemical" siblings={siblings(400)} />
    );
    expect(screen.getAllByRole("link")).toHaveLength(4);
    expect(screen.getByText("396 others")).toBeInTheDocument();
  });

  it("expands to the full list on request", () => {
    render(
      <EntityAmbiguityBanner entityType="chemical" siblings={siblings(400)} />
    );
    fireEvent.click(screen.getByText("396 others"));
    expect(screen.getAllByRole("link")).toHaveLength(400);
  });

  it("admits a big cluster is chained rather than a shared name", () => {
    // A 400-member component is built from A~B, B~C, … — most pairs in it
    // never co-occurred. Presenting it as "this name means any of these"
    // would overstate what the data says.
    render(
      <EntityAmbiguityBanner entityType="chemical" siblings={siblings(400)} />
    );
    expect(screen.getByText(/grouped by chained overlaps/)).toBeInTheDocument();
  });

  it("does not add the chaining caveat to a genuine small collision", () => {
    // quercetin ↔ quercetin-7-olate is a cluster of 2: a real name
    // collision, where the caveat would just be noise.
    render(
      <EntityAmbiguityBanner entityType="chemical" siblings={siblings(1)} />
    );
    expect(screen.queryByText(/grouped by chained overlaps/)).toBeNull();
    expect(screen.queryByText(/others?$/)).toBeNull();
  });
});

describe("entity wording", () => {
  it("names the entity type it is talking about", () => {
    // Foods reach clusters of 8; the copy has to read for them too.
    render(<EntityAmbiguityBanner entityType="food" siblings={siblings(2)} />);
    expect(
      screen.getByText(/Some evidence here may belong to another food/)
    ).toBeInTheDocument();
  });
});
