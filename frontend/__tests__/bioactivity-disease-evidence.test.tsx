// What a bioactivity↔disease link actually claims, as rendered.
//
// The link used to read as an undifferentiated "associated with". These tests
// pin the three things that replaced it — direction, protein target, source
// assay — and the two ways they must fail quietly: a row the literature
// doesn't cover shows no literature badge at all, and a row served by an API
// that predates these fields still renders instead of taking the tab down.

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
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
import { targetUrl } from "@/components/entities/shared/AssayDetailModals";
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

  it("states the target count on a button and lists them in a modal", async () => {
    // The same button and modal the assay-inferred tables use. This cell
    // was the last one still rendering inline chips plus a "+N" tooltip —
    // unreachable on touch, uncopyable, and an arbitrary slice.
    await mount([diseaseRow()]);
    await waitFor(() => expect(shown("melanoma")).toBe(true));
    expect(shown("Cellular tumor antigen p53")).toBe(false);
    fireEvent.click(screen.getAllByText("See 1 target")[0]);
    await waitFor(() =>
      expect(screen.getByText("Protein targets")).toBeInTheDocument()
    );
    // The label is whole here, not truncated as a chip had to be, and the
    // id links out.
    expect(screen.getByText("Cellular tumor antigen p53")).toBeInTheDocument();
    expect(screen.getByText("NCBIGene: 7157").closest("a")).toHaveAttribute(
      "href",
      expect.stringContaining("ncbi.nlm.nih.gov/gene/7157")
    );
  });

  it("names the disease the targets belong to", async () => {
    await mount([diseaseRow()]);
    await waitFor(() => expect(shown("melanoma")).toBe(true));
    fireEvent.click(screen.getAllByText("See 1 target")[0]);
    await waitFor(() =>
      expect(screen.getByText("Protein targets")).toBeInTheDocument()
    );
    expect(screen.getAllByText("melanoma").length).toBeGreaterThan(1);
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

describe("targetUrl", () => {
  it("links Entrez and UniProt ids to their own databases", () => {
    expect(targetUrl("NCBIGene: 7157")).toContain("ncbi.nlm.nih.gov/gene/7157");
    expect(targetUrl("UniProt: P04637")).toContain("uniprotkb/P04637");
    expect(targetUrl("something else")).toBeNull();
  });
});
