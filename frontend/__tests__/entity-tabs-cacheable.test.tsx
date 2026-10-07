// Entity pages are cached (ISR), so their render can't depend on the query
// string: a useSearchParams call anywhere in the tree drops the page out of
// the server HTML (0 entity links instead of 79 on /disease/obesity). The
// open tab is the default on the server and follows ?tab= after mount.

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { render, screen } from "@testing-library/react";
import { renderToString } from "react-dom/server";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

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
}));

import EntityTabs, { TabSpec } from "@/components/entities/EntityTabs";
import { TabCountsProvider } from "@/context/tabCountsContext";

const tabs = (): TabSpec[] => [
  { id: "composition", label: "Composition", content: <p>composition body</p> },
  { id: "bioactivities", label: "Bioactivities", content: <p>bioactivities body</p> },
];

const tree = () => (
  <TabCountsProvider>
    <EntityTabs entityType="food" tabs={tabs()} defaultTabId="composition" />
  </TabCountsProvider>
);

afterEach(() => window.history.replaceState(null, "", "/"));

describe("EntityTabs on a cached page", () => {
  it("server-renders the default tab whatever the URL says", () => {
    window.history.replaceState(null, "", "/food/tomato?tab=bioactivities");
    const html = renderToString(tree());
    expect(html).toContain("composition body");
    expect(html).not.toContain("bioactivities body");
  });

  it("opens the ?tab= tab once mounted", () => {
    window.history.replaceState(null, "", "/food/tomato?tab=bioactivities");
    render(tree());
    expect(screen.getByText("bioactivities body")).toBeVisible();
  });

  it("falls back to the default tab for an unknown ?tab=", () => {
    window.history.replaceState(null, "", "/food/tomato?tab=nope");
    render(tree());
    expect(screen.getByText("composition body")).toBeVisible();
  });
});

describe("nothing under an entity page reads the query during render", () => {
  it("has no useSearchParams in components/entities or the entity routes", () => {
    const root = join(__dirname, "..");
    const hits: string[] = [];
    for (const dir of [
      "components/entities",
      "app/(everything-else)/food",
      "app/(everything-else)/chemical",
      "app/(everything-else)/disease",
      "app/(everything-else)/bioactivity",
    ]) {
      for (const f of readdirSync(join(root, dir), { recursive: true })) {
        const path = join(dir, String(f));
        if (!/\.tsx?$/.test(path)) continue;
        const src = readFileSync(join(root, path), "utf8");
        // A call or an import; comments may still name it.
        if (/useSearchParams\s*\(|import[^;]*\buseSearchParams\b/.test(src)) {
          hits.push(path);
        }
      }
    }
    expect(hits).toEqual([]);
  });
});
