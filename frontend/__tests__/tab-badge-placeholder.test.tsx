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

  it("reserves the same width the real badge will occupy", () => {
    // The placeholder previously reserved w-6 (1.5rem) against a real
    // badge of 1.9–2.3rem, so the chip still grew when the count landed —
    // the exact reflow the placeholder exists to prevent. Pin the two to
    // the same min-width so they cannot drift apart again.
    const pending = renderTabs([tab({ count: null })]);
    const landed = renderTabs([tab({ count: 1234 })]);

    const widthClass = (el: Element | null) =>
      Array.from(el?.classList ?? []).find((c) => c.startsWith("min-w-"));

    // Scoped to the desktop chip strip: the mobile Listbox renders its own
    // pill-shaped placeholder at a different (text-sm) width, so an
    // unscoped `.rounded-full` compares two different elements.
    const desktopBadge = (c: HTMLElement) =>
      c.querySelector('[role="tab"] .rounded-full');

    expect(widthClass(desktopBadge(pending.container))).toBeDefined();
    expect(widthClass(desktopBadge(pending.container))).toBe(
      widthClass(desktopBadge(landed.container))
    );
  });

  it("keeps a four-character count inside one badge box", () => {
    // 1,234 formats to "1.2k" — the widest string formatCount emits in
    // practice, and the case that looked cramped before the min-width.
    const { getAllByText } = renderTabs([tab({ count: 1234 })]);
    expect(getAllByText("1.2k").length).toBeGreaterThan(0);
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
