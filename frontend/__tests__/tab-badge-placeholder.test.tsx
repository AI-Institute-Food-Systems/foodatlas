import { render } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";

beforeAll(() => {
  globalThis.ResizeObserver ??= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  } as unknown as typeof ResizeObserver;
});

vi.mock("@/context/tabCountsContext", () => ({
  useTabCounts: () => ({ counts: {} }),
  TabCountsProvider: ({ children }: { children: React.ReactNode }) => children,
}));
vi.mock("@/context/paginationsContext", () => ({
  usePaginations: () => ({ resetAllPaginations: vi.fn() }),
}));
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/",
  useSearchParams: () => new URLSearchParams(),
}));

import EntityTabs, { type TabSpec } from "@/components/entities/EntityTabs";

const tab = (over: Partial<TabSpec> = {}): TabSpec => ({
  id: "composition",
  label: "Composition",
  hasCount: true,
  count: null,
  content: <div />,
  ...over,
});

const renderTabs = (tabs: TabSpec[]) =>
  render(
    <EntityTabs entityType="food" tabs={tabs} defaultTabId={tabs[0].id} />
  );

// The chip strip lives in the desktop wrapper; each chip's badge is the
// only rounded-full descendant.
const badges = (container: HTMLElement) =>
  container.querySelectorAll(".rounded-full");

describe("tab count badges", () => {
  it("reserves width while the count is pending", () => {
    // A badge that pops in widens its chip, which re-runs the overflow
    // ResizeObserver and can flip the whole desktop strip into the mobile
    // Listbox mid-load. Reserving the box keeps the width final.
    const { container } = renderTabs([tab({ count: null })]);
    expect(badges(container).length).toBeGreaterThan(0);
  });

  it("shows the real count once it lands", () => {
    const { getAllByText } = renderTabs([tab({ count: 42 })]);
    expect(getAllByText("42").length).toBeGreaterThan(0);
  });

  it("renders no badge slot for a tab that never counts", () => {
    // Overview is metadata, not a collection — it should not reserve a
    // badge box it will never fill.
    const { container } = renderTabs([
      tab({ id: "overview", label: "IDs & Metadata", hasCount: false }),
    ]);
    expect(badges(container)).toHaveLength(0);
  });

  it("does not disable an uncounted tab", () => {
    // `count === 0` used to disable a tab; an uncounted tab has no count
    // at all and must stay reachable.
    const { container } = renderTabs([
      tab({ id: "overview", label: "IDs & Metadata", hasCount: false }),
    ]);
    expect(container.querySelector("[data-disabled]")).toBeNull();
  });
});
