import { useState } from "react";
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
  usePathname: () => "/food/tomato",
  useSearchParams: () => new URLSearchParams(),
}));

import EntityTabs, { TabSpec } from "@/components/entities/EntityTabs";
import { TabCountsProvider } from "@/context/tabCountsContext";

// Counts how many times each panel body actually mounts, which is the whole
// point: a remount is what re-runs the sections' data fetches.
const mounts: Record<string, number> = {};

const Panel = ({ id }: { id: string }) => {
  // A useState initializer runs exactly once per mounted instance, so this
  // counts mounts rather than renders.
  useState(() => {
    mounts[id] = (mounts[id] ?? 0) + 1;
    return null;
  });
  return <div data-testid={`panel-${id}`}>panel {id} body</div>;
};

const tabs = (): TabSpec[] => [
  { id: "composition", label: "Composition", content: <Panel id="composition" /> },
  { id: "bioactivities", label: "Bioactivities", content: <Panel id="bioactivities" /> },
  { id: "overview", label: "Overview", content: <Panel id="overview" /> },
];

const renderTabs = () =>
  render(
    <TabCountsProvider>
      <EntityTabs entityType="food" tabs={tabs()} defaultTabId="composition" />
    </TabCountsProvider>
  );

const clickTab = (label: string) => {
  // The chip strip and the mobile Listbox both render the label; the strip's
  // button is the one bound to Headless UI's tab state in jsdom.
  const hits = screen.getAllByRole("tab", { name: new RegExp(label, "i") });
  fireEvent.click(hits[0]);
};

describe("EntityTabs keep-alive", () => {
  beforeEach(() => {
    for (const k of Object.keys(mounts)) delete mounts[k];
    window.history.replaceState(null, "", "/food/tomato");
  });

  it("mounts only the landing tab's content on first paint", () => {
    renderTabs();
    expect(screen.getByTestId("panel-composition")).toBeDefined();
    expect(screen.queryByTestId("panel-bioactivities")).toBeNull();
    expect(screen.queryByTestId("panel-overview")).toBeNull();
    expect(mounts).toEqual({ composition: 1 });
  });

  it("keeps a visited panel in the DOM after switching away", () => {
    renderTabs();
    clickTab("Bioactivities");

    // Both are now mounted; the inactive one is hidden, not destroyed.
    expect(screen.getByTestId("panel-composition")).toBeDefined();
    expect(screen.getByTestId("panel-bioactivities")).toBeDefined();
    // Never-opened tab is still unmounted.
    expect(screen.queryByTestId("panel-overview")).toBeNull();
  });

  it("does not remount a panel when returning to it", () => {
    renderTabs();
    clickTab("Bioactivities");
    clickTab("Composition");
    clickTab("Bioactivities");

    // One mount each, no matter how often we switch back and forth. If
    // panels unmounted, composition would be at 2+ here.
    expect(mounts.composition).toBe(1);
    expect(mounts.bioactivities).toBe(1);
  });

  it("writes the tab id to the URL without a router navigation", () => {
    renderTabs();
    clickTab("Bioactivities");
    expect(window.location.search).toContain("tab=bioactivities");
  });

  it("preserves unrelated query params when switching", () => {
    window.history.replaceState(null, "", "/food/tomato?highlight=quercetin");
    renderTabs();
    clickTab("Bioactivities");
    expect(window.location.search).toContain("highlight=quercetin");
    expect(window.location.search).toContain("tab=bioactivities");
  });
});
