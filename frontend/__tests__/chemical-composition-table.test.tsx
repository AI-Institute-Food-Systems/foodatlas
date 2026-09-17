import { fireEvent, render, screen, within } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

// jsdom has no ResizeObserver; the mobile SortListbox (HeadlessUI) measures
// its trigger on mount. Without this the whole tree throws and renders
// empty — which would make the negative assertions here pass for the wrong
// reason.
beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

import ChemicalCompositionTable from "@/components/entities/chemical/ChemicalCompositionTable";
import { PaginationsProvider } from "@/context/paginationsContext";
import { ChemicalCompositionRow } from "@/utils/chemicalComposition";

vi.mock("@/utils/fetching", () => ({
  getChemicalCompositionEvidence: vi.fn(async () => [
    {
      premise: "Onions contain quercetin.",
      reference: { pmid: "12345678", title: "A paper" },
      extraction: [],
    },
  ]),
}));

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn() }),
  usePathname: () => "/chemical/quercetin",
  useSearchParams: () => new URLSearchParams(),
}));

const measured: ChemicalCompositionRow[] = [
  {
    id: "f1",
    name: "onion",
    median_concentration: { value: 1000, unit: "mg/100g" },
    evidence_count: 12,
    fdc_count: 12,
  },
  {
    id: "f2",
    name: "apple",
    median_concentration: { value: 250, unit: "mg/100g" },
    evidence_count: 1,
    foodatlas_count: 1,
  },
  {
    id: "f3",
    name: "parsley",
    median_concentration: { value: 1, unit: "mg/100g" },
    evidence_count: 4,
    ptfi_count: 4,
  },
];

const unmeasured: ChemicalCompositionRow[] = [
  {
    id: "f4",
    name: "kale",
    median_concentration: null,
    evidence_count: 7,
    fdc_count: 7,
  },
];

const renderTable = (props: Partial<
  React.ComponentProps<typeof ChemicalCompositionTable>
> = {}) =>
  render(
    <PaginationsProvider>
      <ChemicalCompositionTable
        withConcentrations={measured}
        withoutConcentrations={unmeasured}
        commonName="quercetin"
        chemicalId="c123"
        {...props}
      />
    </PaginationsProvider>
  );

// The desktop table and the mobile card list both render in jsdom, so every
// row appears twice. Single-match helpers would throw on all of these.
const barPercents = () =>
  screen
    .getAllByTestId("concentration-bar")
    .map((el) => el.getAttribute("data-percent"));

describe("ChemicalCompositionTable", () => {
  it("scales bars linearly against the highest concentration", () => {
    renderTable();
    // 1000 -> 100%, 250 -> 25%, 1 -> 0.1% floored to the 1.5% minimum.
    expect(barPercents()).toEqual(
      expect.arrayContaining(["100", "25", "1.5"])
    );
  });

  it("keeps bar lengths fixed when a search hides the top row", () => {
    // The scale denominator comes from the full result set. If it were
    // derived from the filtered rows, apple would jump to a full-width bar
    // and the same concentration would render at two different lengths
    // depending on what the user had typed.
    renderTable();
    fireEvent.change(screen.getAllByLabelText("Search foods")[0]!, {
      target: { value: "apple" },
    });
    expect(barPercents().every((p) => p === "25")).toBe(true);
  });

  it("ranks by concentration descending by default", () => {
    renderTable();
    const rows = screen.getAllByRole("row").slice(1); // drop the header
    const firstCell = (i: number) =>
      within(rows[i]!).getAllByRole("cell")[0]!.textContent;
    expect(firstCell(0)).toContain("onion");
    expect(firstCell(1)).toContain("apple");
    expect(firstCell(2)).toContain("parsley");
  });

  it("renders unmeasured foods with a dash and no bar", () => {
    renderTable();
    // kale is present (toggle defaults on) but contributes no bar.
    expect(screen.getAllByText("kale").length).toBeGreaterThan(0);
    expect(screen.getAllByTestId("concentration-bar")).toHaveLength(
      measured.length * 2
    );
  });

  it("drops unmeasured foods when the toggle is switched off", () => {
    // Rendered by the shared ToggleSwitch under an "Include" group, the
    // same control the food composition sidebar uses — so the label is
    // "Without concentration", not a self-contained chip sentence.
    renderTable();
    fireEvent.click(screen.getAllByText("Without concentration")[0]!);
    expect(screen.queryAllByText("kale")).toHaveLength(0);
  });

  it("pluralises the evidence chip", () => {
    renderTable();
    expect(screen.getAllByText("1 data point").length).toBeGreaterThan(0);
    expect(screen.getAllByText("12 data points").length).toBeGreaterThan(0);
  });

  it("links each food to its composition row on the food page", () => {
    renderTable();
    const link = screen
      .getAllByRole("link")
      .find((a) => a.textContent?.includes("onion"))!;
    expect(link.getAttribute("href")).toBe(
      "/food/onion?highlight=c123#composition"
    );
  });

  it("filters by source", () => {
    renderTable();
    fireEvent.click(screen.getAllByText("PTFI")[0]!);
    expect(screen.queryAllByText("onion")).toHaveLength(0);
    expect(screen.getAllByText("parsley").length).toBeGreaterThan(0);
  });

  it("reports an empty filter result without claiming there is no data", () => {
    renderTable();
    fireEvent.change(screen.getAllByLabelText("Search foods")[0]!, {
      target: { value: "zzz" },
    });
    expect(screen.getAllByText("No foods match these filters").length)
      .toBeGreaterThan(0);
  });

  it("survives an API build that omits the new count fields", () => {
    renderTable({
      withConcentrations: [
        { id: "f1", name: "onion", median_concentration: { value: 5, unit: "mg/100g" } },
      ],
      withoutConcentrations: [],
    });
    expect(screen.getAllByText("0 data points").length).toBeGreaterThan(0);
  });
});

describe("evidence modal", () => {
  it("opens on the data-point chip rather than navigating away", async () => {
    // The chip used to be a link to the food page. A composition data point
    // is the same (food, chemical) pair from either side, so it should open
    // the food page's modal instead of making the user leave.
    renderTable();
    const chips = screen.getAllByRole("button", { name: /12 data points/i });
    expect(chips.length).toBeGreaterThan(0);
    fireEvent.click(chips[0]);
    expect(await screen.findByRole("dialog")).toBeInTheDocument();
  });

  it("names the pair the row came from", async () => {
    renderTable();
    fireEvent.click(screen.getAllByRole("button", { name: /12 data points/i })[0]);
    const dialog = await screen.findByRole("dialog");
    expect(within(dialog).getByText(/onion/i)).toBeInTheDocument();
  });
});

describe("columns", () => {
  it("does not render a Sources column", () => {
    // Source is a filter, not a per-row fact worth a column of its own —
    // it was pushing the food and concentration columns narrow enough to
    // wrap long food names.
    renderTable();
    expect(screen.queryByRole("columnheader", { name: /sources/i })).toBeNull();
  });

  it("still offers source as a filter", () => {
    renderTable();
    expect(screen.getAllByRole("button", { name: /FDC/i }).length).toBeGreaterThan(0);
  });
});
