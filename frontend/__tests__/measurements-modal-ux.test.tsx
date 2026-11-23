// Measurements modal behaviour from the 2026-11-23 prod test:
// no flash of the capped sample, units by count, Value sort for one unit,
// and the filtered count as the headline.

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
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
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/utils/fetching", () => ({ getBioactivityMeasurements: vi.fn() }));

import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";
import { getBioactivityMeasurements } from "@/utils/fetching";

const m = (assay: string, unit: string, value: number | null) => ({
  assay,
  endpoint: "IC50",
  outcome: "Active",
  evidence_type: "in vitro",
  evidence_source: "Experimental",
  unit,
  value,
});

const FULL = [
  m("a1", "nM", 5),
  m("a2", "nM", 50),
  m("a3", "nM", null),
  m("a4", "uM", 1),
  m("a5", "nM", 500),
  m("a6", "mg/kg", 2),
];

const mount = (props: Record<string, unknown> = {}) =>
  render(
    <BioactivityMeasurementsModal
      isOpen
      onClose={() => {}}
      headLabel="quercetin"
      tailLabel="anticancer"
      initialMeasurements={[FULL[0]] as never}
      {...props}
    />
  );

const countAttr = () =>
  document.body.querySelector("[data-filtered-count]")?.getAttribute("data-filtered-count");

const assays = () =>
  Array.from(document.body.querySelectorAll("table tbody tr"))
    .map((tr) => tr.querySelector("td")?.textContent?.trim() ?? "")
    .filter(Boolean);

describe("measurements modal", () => {
  it("shows no count from the capped sample while the full set loads", async () => {
    let resolve: (v: unknown) => void = () => {};
    vi.mocked(getBioactivityMeasurements).mockReturnValue(
      new Promise((r) => {
        resolve = r;
      }) as never
    );
    mount({ anchorId: "b1", selectedId: "c1", relationship: "r6", initialUnits: ["nM"] });
    await act(() => new Promise((r) => setTimeout(r, 50)));
    expect(countAttr()).toBeUndefined();
    await act(async () => resolve({ data: FULL }));
    await waitFor(() => expect(countAttr()).toBe("4"));
  });

  it("lists units busiest first", async () => {
    mount({ initialMeasurements: FULL });
    await waitFor(() => expect(countAttr()).toBe("6"));
    const unitLabels = ["nM", "mg/kg", "uM"].map((u) =>
      screen
        .getAllByRole("button")
        .findIndex((b) => new RegExp(`^${u.replace("/", "\\/")}\\s*\\d+$`).test((b.textContent ?? "").trim()))
    );
    expect(unitLabels.every((i) => i >= 0)).toBe(true);
    expect(unitLabels).toEqual([...unitLabels].sort((a, b) => a - b));
  });

  it("sorts by Value only when the rows share one unit", async () => {
    mount({ initialMeasurements: FULL });
    await waitFor(() => expect(countAttr()).toBe("6"));
    expect(screen.queryByRole("button", { name: /^value$/i })).toBeNull();

    cleanup();
    mount({ initialMeasurements: FULL, initialUnits: ["nM"] });
    await waitFor(() => expect(countAttr()).toBe("4"));
    fireEvent.click(screen.getByRole("button", { name: /^value$/i }));
    await waitFor(() => expect(assays().slice(0, 4)).toEqual(["a5", "a2", "a1", "a3"]));
    fireEvent.click(screen.getByRole("button", { name: /^value$/i }));
    await waitFor(() => expect(assays().slice(0, 4)).toEqual(["a1", "a2", "a5", "a3"]));
  });

  it("states the filtered count as the headline", async () => {
    mount({ initialMeasurements: FULL, initialUnits: ["nM"] });
    await waitFor(() => expect(countAttr()).toBe("4"));
    expect(screen.getByText(/^4 of$/)).toBeTruthy();
  });
});
