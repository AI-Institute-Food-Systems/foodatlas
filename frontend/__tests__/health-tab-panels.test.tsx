// Health tab: one filter panel per table, and exclusive Direction buckets.
//
// Direction filters only the literature table; Signal and Activity filter
// only the assay table. Each table therefore gets its own panel and its
// own search, and Improves + Mixed + Worsens = All.

import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});
afterEach(cleanup);

vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}), isSelectMode: false }),
}));
vi.mock("@/context/tabCountsContext", () => ({ usePublishTabCount: () => undefined }));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/utils/fetching", () => ({
  getDiseaseData: vi.fn(),
  getChemicalDiseaseAssociations: vi.fn(),
  getCorrelationDirectionCounts: vi
    .fn()
    .mockResolvedValue({ improves: 148, worsens: 7, mixed: 11, both: 166 }),
}));

import CorrelationEvidenceTab from "@/components/entities/shared/CorrelationEvidenceTab";
import { PaginationsProvider } from "@/context/paginationsContext";
import {
  getChemicalDiseaseAssociations,
  getCorrelationDirectionCounts,
  getDiseaseData,
} from "@/utils/fetching";

const mount = async () => {
  vi.mocked(getDiseaseData).mockResolvedValue({
    data: { associations: [] },
    metadata: { total_rows: 0, total_pages: 0 },
  } as never);
  vi.mocked(getChemicalDiseaseAssociations).mockResolvedValue({
    data: [],
    metadata: { row_count: 0 },
  } as never);
  render(
    <PaginationsProvider>
      <CorrelationEvidenceTab commonName="quercetin" anchor="chemical" />
    </PaginationsProvider>
  );
  await waitFor(() => expect(getCorrelationDirectionCounts).toHaveBeenCalled());
};

// Each FilterPanel renders its body once in the desktop sidebar (<aside>).
const panels = () => Array.from(document.body.querySelectorAll("aside"));

describe("Health tab filter panels", () => {
  it("gives each table its own panel with its own filters and search", async () => {
    await mount();
    const [literature, assays] = panels();
    expect(panels()).toHaveLength(2);
    expect(within(literature).getByText("Direction")).toBeTruthy();
    expect(within(literature).queryByText("Signal")).toBeNull();
    expect(within(assays).getByText("Signal")).toBeTruthy();
    expect(within(assays).queryByText("Direction")).toBeNull();
    expect(literature.querySelectorAll("input")).toHaveLength(1);
    expect(assays.querySelectorAll("input")).toHaveLength(1);
  });

  it("searches only its own table", async () => {
    await mount();
    const [, assays] = panels();
    const box = assays.querySelector("input") as HTMLInputElement;
    vi.mocked(getCorrelationDirectionCounts).mockClear();
    fireEvent.change(box, { target: { value: "asthma" } });
    // The literature counts refetch on the literature search only.
    await new Promise((r) => setTimeout(r, 400));
    expect(getCorrelationDirectionCounts).not.toHaveBeenCalled();
  });

  it("offers exclusive Direction buckets that sum to All", async () => {
    await mount();
    const [literature] = panels();
    const count = (label: string) => {
      const option = within(literature)
        .getAllByRole("radio")
        .find((r) => new RegExp(`^${label}\\s*[\\d,]+$`).test((r.textContent ?? "").trim()));
      expect(option, label).toBeTruthy();
      return Number((option!.textContent ?? "").replace(/\D/g, ""));
    };
    await waitFor(() => expect(count("All")).toBe(166));
    expect(count("Improves") + count("Mixed") + count("Worsens")).toBe(count("All"));
  });
});
