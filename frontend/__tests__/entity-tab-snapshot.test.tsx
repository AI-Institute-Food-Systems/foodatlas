import { fireEvent, render, screen } from "@testing-library/react";
import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/chemical/quercetin",
  useSearchParams: () => new URLSearchParams(),
}));

import EntityTabs, { TabSpec } from "@/components/entities/EntityTabs";
import TabSnapshot from "@/components/entities/shared/TabSnapshot";
import { TabCountsProvider } from "@/context/tabCountsContext";
import {
  SNAPSHOT_ROW_CAP,
  assayInferredSection,
  bioactivityDiseasesSection,
  bioactivityListSection,
  inferredBioactivitySection,
  literatureSection,
} from "@/utils/tabSnapshots";

const tabs = (): TabSpec[] => [
  { id: "composition", label: "Composition", content: <div>composition live</div> },
  {
    id: "bioactivities",
    label: "Bioactivities",
    content: <div>bioactivities live</div>,
    snapshot: (
      <TabSnapshot
        sections={[
          bioactivityListSection("Bioactivities", "bioactivity", {
            data: [{ name: "anti inflammatory", measurement_count: 12 }],
          }),
        ]}
      />
    ),
  },
];

const renderTabs = () =>
  render(
    <TabCountsProvider>
      <EntityTabs entityType="chemical" tabs={tabs()} defaultTabId="composition" />
    </TabCountsProvider>
  );

describe("EntityTabs snapshots", () => {
  beforeEach(() => window.history.replaceState(null, "", "/chemical/quercetin"));

  it("renders an unopened tab's snapshot links, not its live content", () => {
    renderTabs();
    expect(screen.queryByText("bioactivities live")).toBeNull();
    const link = document.querySelector('[data-tab-snapshot] a');
    expect(link?.getAttribute("href")).toBe("/bioactivity/anti--inflammatory");
    // Inside the inactive panel, so hidden from users.
    expect(link?.closest("[hidden]")).not.toBeNull();
  });

  it("swaps the snapshot for the live tab on first open", () => {
    renderTabs();
    fireEvent.click(screen.getAllByRole("tab", { name: /bioactivities/i })[0]);
    expect(screen.getByText("bioactivities live")).toBeDefined();
    expect(document.querySelector("[data-tab-snapshot]")).toBeNull();
  });
});

describe("snapshot row builders", () => {
  it("caps unpaginated payloads at one page", () => {
    const data = Array.from({ length: 60 }, (_, i) => ({
      disease_name: `d${i}`,
      n_chemicals: i,
    }));
    const section = bioactivityDiseasesSection({ data } as never);
    expect(section.rows).toHaveLength(SNAPSHOT_ROW_CAP);
    expect(section.rows[0][0]).toEqual({ label: "d0", href: "/disease/d0" });
  });

  it("links both sides of an inferred row", () => {
    const section = inferredBioactivitySection({
      data: [{ bioactivity: "antioxidant", chemical: "vitamin c" }],
    });
    expect(section.rows[0]).toEqual([
      { label: "antioxidant", href: "/bioactivity/antioxidant" },
      { label: "vitamin c", href: "/chemical/vitamin--c" },
    ]);
  });

  it("names the peer side of literature and assay rows", () => {
    const lit = literatureSection("disease", {
      data: {
        associations: [
          { id: "x", name: "obesity", sources: [], improves_evidences: [{}, {}] as never },
        ],
      },
    });
    expect(lit.rows[0]).toEqual([{ label: "obesity", href: "/disease/obesity" }, 2]);

    const assay = assayInferredSection("chemical", {
      data: [{ chemical_name: "quercetin", disease_name: "obesity", n_assays: 3 }] as never,
    });
    expect(assay.rows[0]).toEqual([{ label: "quercetin", href: "/chemical/quercetin" }, 3]);
  });

  it("tolerates a failed fetch", () => {
    expect(bioactivityListSection("x", "food", null).rows).toEqual([]);
    const { container } = render(
      <TabSnapshot sections={[bioactivityListSection("x", "food", null)]} />
    );
    expect(container.innerHTML).toBe("");
  });
});
