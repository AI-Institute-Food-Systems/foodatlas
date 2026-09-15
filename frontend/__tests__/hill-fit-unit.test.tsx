// The Hill-fit panel says the AC50 in the row's unit.
//
// PubChem's efficacy_logac50_value is log10 molar. The panel printed
// 10^logAC50 with the row's unit appended: L-lysine × cardioprotective
// (AID 588834) is 14.1 uM in its row and read "AC50 0.0000141 uM" in the
// panel, a million times too potent.

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/context/reportModeContext", () => ({
  useReportRows: () => ({ getRowProps: () => ({}), isSelectMode: false }),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("@/utils/fetching", () => ({
  getBioactivityMeasurements: vi.fn().mockResolvedValue(null),
}));

import BioactivityMeasurementsModal from "@/components/entities/bioactivity/BioactivityMeasurementsModal";

const fitted = (unit: string | null) => ({
  assay: "AID 588834",
  endpoint: "Potency",
  outcome: "active",
  evidence_type: "in vitro",
  evidence_source: "experimental",
  unit,
  value: 14.1254,
  efficacy_logac50_value: -4.85,
  efficacy_hillslope: 1.2,
  efficacy_zeroactivity: 0,
  efficacy_infiniteactivity: 100,
});

const openFit = async (unit: string | null) => {
  render(
    <BioactivityMeasurementsModal
      isOpen
      onClose={() => {}}
      headLabel="L-lysine"
      tailLabel="cardioprotective"
      initialMeasurements={[fitted(unit)] as never}
    />
  );
  const chips = await screen.findAllByRole("button", { name: /show hill curve/i });
  fireEvent.click(chips[0]);
  await waitFor(() => expect(screen.getAllByText("AC50").length).toBeGreaterThan(0));
  // The <dd> after the AC50 <dt>.
  const dt = screen.getAllByText("AC50")[0];
  return (dt.nextElementSibling?.textContent ?? "").replace(/\s+/g, " ").trim();
};

describe("Hill-fit panel AC50", () => {
  it("prints the AC50 in the row's unit, agreeing with the row", async () => {
    expect(await openFit("uM")).toBe("14.1 uM");
  });

  it("says molar when the row has no convertible unit", async () => {
    expect(await openFit("None")).toBe("1.41e-5 M");
  });
});
