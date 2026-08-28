// The ambiguity affordance: where it sits, and what it is allowed to claim.
//
// Placement is the reason this is inline text and not the banner it
// replaced. HeaderSectionSuspense renders the header band alone, so a banner
// hanging below the band appeared only once the real header streamed in —
// inserting a block and pushing the tab strip and the rest of the page down.
// This rides the entity badge's line, whose height comes from the Badge.
//
// The claim is about attribution, not naming. materializer._build_sibling_map
// treats two entities as siblings when one raw name in one source resolved to
// both, and _components_from_candidates then expands that transitively into
// connected components. So the copy must not say "this name also means X",
// and must not present a chained 400-member cluster as a shared name.

import { fireEvent, render, screen, within } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import EntityAmbiguityBadge from "@/components/entities/EntityAmbiguityBadge";

const siblings = (n: number) =>
  Array.from({ length: n }, (_, i) => ({
    foodatlas_id: `e${i}`,
    common_name: `sibling-${i}`,
  }));

const open = () => fireEvent.click(screen.getByText("Ambiguous entities"));

describe("empty states", () => {
  it("renders nothing without siblings", () => {
    const { container } = render(
      <EntityAmbiguityBadge entityType="chemical" siblings={[]} />
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when the field is missing", () => {
    // Diseases never carry siblings — 0 of 3,177 rows.
    const { container } = render(
      <EntityAmbiguityBadge entityType="disease" siblings={undefined} />
    );
    expect(container).toBeEmptyDOMElement();
  });
});

describe("the collapsed affordance", () => {
  it("reads as a link, with the warning colour on the glyph alone", () => {
    // It sits beside the entity badge, where a filled amber chip competed
    // with the badge instead of deferring to it. Underline-on-hover is the
    // app's inline-action convention (FilterControls, EvidenceTable).
    render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(2)} />
    );
    const button = screen.getByText("Ambiguous entities").closest("button")!;
    expect(button.className).toContain("hover:underline");
    expect(button.className).toContain("underline-offset-4");
    expect(button.className).not.toContain("bg-amber");
    expect(button.querySelector("svg")?.getAttribute("class")).toContain(
      "text-amber-400"
    );
  });

  it("cannot wrap onto a second line", () => {
    // Load-bearing: the badge line is sized by the Badge, and the skeleton
    // reserves nothing for this. A label that wrapped would add a line the
    // skeleton has no room for, and the page would step down at handoff.
    render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(2)} />
    );
    expect(
      screen.getByText("Ambiguous entities").closest("button")!.className
    ).toContain("whitespace-nowrap");
  });

  it("is short, with the explanation behind it", () => {
    // Everything the old banner said inline now lives in the modal, so
    // the header band gains a few words' width and no height.
    render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(400)} />
    );
    expect(screen.getByText("Ambiguous entities")).toBeInTheDocument();
    // No sibling names, no prose, until it is opened.
    expect(screen.queryByText("sibling-0")).toBeNull();
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("does not grow with the size of the cluster", () => {
    // The old banner rendered every sibling as an inline link; 709
    // chemical pages have >100 siblings and the largest cluster is 401.
    const { container: small } = render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(1)} />
    );
    const { container: huge } = render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(400)} />
    );
    expect(small.textContent).toBe(huge.textContent);
  });
});

describe("what the popup claims", () => {
  it("warns about attribution, not about the name", () => {
    render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(1)} />
    );
    open();
    const dialog = within(screen.getByRole("dialog"));
    expect(
      dialog.getByText(/could not resolve to a single chemical/)
    ).toBeInTheDocument();
  });

  it("says the evidence may also appear on the sibling's page", () => {
    // The relation is symmetric — both pages carry the same uncertain rows.
    render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(1)} />
    );
    open();
    expect(
      within(screen.getByRole("dialog")).getByText(
        /may appear on their pages too/
      )
    ).toBeInTheDocument();
  });

  it("lists every sibling, however many", () => {
    render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(400)} />
    );
    open();
    expect(within(screen.getByRole("dialog")).getAllByRole("link")).toHaveLength(
      400
    );
  });

  it("admits a big cluster is chained rather than a shared name", () => {
    render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(400)} />
    );
    open();
    expect(
      within(screen.getByRole("dialog")).getByText(/grouped by chained overlaps/)
    ).toBeInTheDocument();
  });

  it("does not add the chaining caveat to a genuine small collision", () => {
    // quercetin ↔ quercetin-7-olate is a cluster of 2: a real name
    // collision, where the caveat would be noise.
    render(
      <EntityAmbiguityBadge entityType="chemical" siblings={siblings(1)} />
    );
    open();
    const dialog = within(screen.getByRole("dialog"));
    expect(dialog.queryByText(/chained overlaps/)).toBeNull();
    // And no empty second paragraph left behind by the removed sentence.
    expect(
      screen.getByRole("dialog").querySelectorAll("p")
    ).toHaveLength(1);
  });

  it("names the entity type it is talking about", () => {
    // Foods reach clusters of 8; the copy has to read for them too.
    render(<EntityAmbiguityBadge entityType="food" siblings={siblings(2)} />);
    open();
    expect(
      within(screen.getByRole("dialog")).getByText(
        /could not resolve to a single food/
      )
    ).toBeInTheDocument();
  });
});
