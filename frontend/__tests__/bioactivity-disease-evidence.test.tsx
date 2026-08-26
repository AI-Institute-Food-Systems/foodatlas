// What a bioactivity↔disease link actually claims, as rendered.
//
// The link used to read as an undifferentiated "associated with". These tests
// pin the three things that replaced it — direction, protein target, source
// assay — and the two ways they must fail quietly: a row the literature
// doesn't cover shows no literature badge at all, and a row served by an API
// that predates these fields still renders instead of taking the tab down.

import { render, screen, waitFor } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/utils/fetching", () => ({
  getBioactivityDiseases: vi.fn(),
}));
vi.mock("@/context/tabCountsContext", () => ({
  usePublishTabCount: () => undefined,
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import BioactivityDiseasesSection from "@/components/entities/bioactivity/BioactivityDiseasesSection";
import LiteratureBadge, {
  verdictFor,
} from "@/components/entities/shared/LiteratureBadge";
import TargetGeneChips, {
  targetUrl,
} from "@/components/entities/shared/TargetGeneChips";
import { getBioactivityDiseases } from "@/utils/fetching";

const diseaseRow = (over: Record<string, unknown> = {}) => ({
  disease_name: "melanoma",
  disease_foodatlas_id: "d1",
  n_chemicals: 100,
  n_assays: 400,
  n_active_measurements: 400,
  n_therapeutic: 3,
  n_marker: 98,
  n_literature: 5,
  target_genes: ["NCBIGene: 7157"],
  targets: [{ id: "NCBIGene: 7157", label: "Cellular tumor antigen p53" }],
  ...over,
});

const mount = async (rows: Record<string, unknown>[]) => {
  vi.mocked(getBioactivityDiseases).mockResolvedValue({
    data: rows,
    metadata: { row_count: rows.length },
  });
  render(<BioactivityDiseasesSection commonName="anticancer" />);
  await waitFor(() => expect(vi.mocked(getBioactivityDiseases)).toHaveBeenCalled());
};

// Desktop table and mobile cards both render in jsdom; assert on presence.
const shown = (text: string | RegExp) => screen.queryAllByText(text).length > 0;

describe("Bioactivity Diseases tab", () => {
  it("reports the direction split, not just a total", async () => {
    await mount([diseaseRow()]);
    await waitFor(() => expect(shown("melanoma")).toBe(true));
    expect(shown(/3 ther\./)).toBe(true);
    expect(shown(/98 mark\./)).toBe(true);
  });

  it("distinguishes rows a bare chemical count would flatten", async () => {
    // Same 1,000 chemicals, opposite meaning: one is almost entirely
    // therapeutic, the other has no therapeutic evidence at all.
    await mount([
      diseaseRow({
        disease_name: "melanoma",
        n_chemicals: 1000,
        n_therapeutic: 900,
        n_marker: 100,
      }),
      diseaseRow({
        disease_name: "leukemia",
        disease_foodatlas_id: "d2",
        n_chemicals: 1000,
        n_therapeutic: 0,
        n_marker: 1000,
      }),
    ]);
    await waitFor(() => expect(shown("leukemia")).toBe(true));
    expect(shown(/900 ther\./)).toBe(true);
    expect(shown(/0 ther\./)).toBe(true);
  });

  it("names the protein target rather than showing a bare gene id", async () => {
    await mount([diseaseRow()]);
    await waitFor(() => expect(shown("melanoma")).toBe(true));
    // Long labels are truncated to keep the chip on one line, so match the
    // readable stem rather than the whole string.
    expect(shown(/^Cellular tumor an/)).toBe(true);
    expect(shown("NCBIGene: 7157")).toBe(false);
  });

  it("still renders when the API predates the evidence fields", async () => {
    // Staging deploys code without reloading data, so a newer frontend does
    // meet older rows. Losing a column is acceptable; losing the tab is not.
    const legacy = diseaseRow();
    delete (legacy as Record<string, unknown>).targets;
    delete (legacy as Record<string, unknown>).target_genes;
    await mount([legacy]);
    await waitFor(() => expect(shown("melanoma")).toBe(true));
  });
});

describe("LiteratureBadge", () => {
  it("renders nothing when the literature does not cover the pair", () => {
    const { container } = render(
      <LiteratureBadge relationships={["therapeutic"]} literatureDirections={[]} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("reports agreement when both sources point the same way", () => {
    expect(verdictFor(["therapeutic"], ["therapeutic"])?.text).toBe(
      "literature agrees",
    );
  });

  it("only claims a difference when both sources state a direction", () => {
    expect(verdictFor(["therapeutic"], ["marker/mechanism"])?.text).toBe(
      "literature differs",
    );
    // No assay-side direction to compare against — corroboration, not conflict.
    expect(verdictFor([], ["marker/mechanism"])?.text).toBe("in literature");
  });
});

describe("TargetGeneChips", () => {
  it("links Entrez and UniProt ids to their own databases", () => {
    expect(targetUrl("NCBIGene: 7157")).toContain("ncbi.nlm.nih.gov/gene/7157");
    expect(targetUrl("UniProt: P04637")).toContain("uniprotkb/P04637");
    expect(targetUrl("something else")).toBeNull();
  });

  it("falls back to the id when no label was resolved", () => {
    render(<TargetGeneChips targets={[{ id: "NCBIGene: 999", label: null }]} />);
    expect(screen.getAllByText("NCBIGene: 999").length).toBeGreaterThan(0);
  });

  it("truncates long labels so a chip cannot wrap the row open", () => {
    // Untruncated, this label wrapped inside its ~180px column and took the
    // row from 30px to 115px.
    render(
      <TargetGeneChips
        targets={[
          {
            id: "NCBIGene: 3417",
            label: "Isocitrate dehydrogenase [NADP] cytoplasmic",
          },
        ]}
      />,
    );
    // textContent also carries Link's trailing external-link arrow, so measure
    // the label up to the ellipsis.
    const chip = screen.getAllByText(/^Isocitrate/)[0];
    const label = chip.textContent!.split("…")[0] + "…";
    expect(label.length).toBeLessThanOrEqual(18);
    expect(chip.textContent).toContain("…");
  });

  it("collapses the overflow rather than listing every target", () => {
    render(
      <TargetGeneChips
        targets={[
          { id: "NCBIGene: 1", label: "one" },
          { id: "NCBIGene: 2", label: "two" },
          { id: "NCBIGene: 3", label: "three" },
          { id: "NCBIGene: 4", label: "four" },
        ]}
        visible={2}
      />,
    );
    expect(screen.getAllByText("+2").length).toBeGreaterThan(0);
  });
});
