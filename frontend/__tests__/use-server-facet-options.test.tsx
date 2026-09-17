// useServerFacetOptions: a facet's full option set from an unfiltered
// fetch, with the faceted counts laid over it. See the hook for why the
// list and the counts are two calls.

import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useServerFacetOptions } from "@/hooks/useServerFacetOptions";

type Filters = { kind?: string };
const NONE: Filters = {};

// A stand-in for /bioactivity/endpoints: like the real endpoint it is a
// GROUP BY, so it only returns values with rows under the given filters,
// and it returns them busiest-first.
const server = (filters: Filters) => {
  const all = [
    { value: "uM", count: 40, kind: "experimental" },
    { value: "nM", count: 5, kind: "predicted" },
    { value: "ug/mL", count: 12, kind: "experimental" },
  ];
  return Promise.resolve(
    all
      .filter((o) => !filters.kind || o.kind === filters.kind)
      .sort((a, b) => b.count - a.count)
      .map(({ value, count }) => ({ value, count }))
  );
};

describe("useServerFacetOptions", () => {
  it("lists every value the entity has, alphabetically", async () => {
    const fetch = vi.fn(server);
    const { result } = renderHook(() => useServerFacetOptions(fetch, NONE, NONE));
    await waitFor(() => expect(result.current.loaded).toBe(true));
    expect(result.current.options.map((o) => o.value)).toEqual([
      "nM",
      "ug/mL",
      "uM",
    ]);
  });

  it("keeps a value the filters zero out, at zero, in place", async () => {
    // This is the food page's Assay Source bug: picking Experimental made
    // the predicted-only unit vanish from the sidebar.
    const fetch = vi.fn(server);
    const { result, rerender } = renderHook(
      ({ f }: { f: Filters }) => useServerFacetOptions(fetch, f, NONE),
      { initialProps: { f: NONE } }
    );
    await waitFor(() => expect(result.current.loaded).toBe(true));

    const experimental: Filters = { kind: "experimental" };
    rerender({ f: experimental });
    await waitFor(() =>
      expect(result.current.options.find((o) => o.value === "nM")?.count).toBe(0)
    );
    expect(result.current.options.map((o) => o.value)).toEqual([
      "nM",
      "ug/mL",
      "uM",
    ]);
    expect(result.current.options.map((o) => o.count)).toEqual([0, 12, 40]);
  });

  it("fetches the universe once per fetcher, not per filter change", async () => {
    const fetch = vi.fn(server);
    const { result, rerender } = renderHook(
      ({ f }: { f: Filters }) => useServerFacetOptions(fetch, f, NONE),
      { initialProps: { f: NONE } }
    );
    await waitFor(() => expect(result.current.loaded).toBe(true));
    const before = fetch.mock.calls.filter(([f]) => f === NONE).length;

    rerender({ f: { kind: "predicted" } });
    await waitFor(() =>
      expect(result.current.options.find((o) => o.value === "uM")?.count).toBe(0)
    );
    const after = fetch.mock.calls.filter(([f]) => f === NONE).length;
    expect(after).toBe(before);
  });

  it("refetches the universe when the fetcher changes (a new entity)", async () => {
    const first = vi.fn(server);
    const second = vi.fn(() =>
      Promise.resolve([{ value: "mg/kg", count: 1 }])
    );
    const { result, rerender } = renderHook(
      ({ fetch }: { fetch: typeof first }) =>
        useServerFacetOptions(fetch, NONE, NONE),
      { initialProps: { fetch: first } }
    );
    await waitFor(() => expect(result.current.loaded).toBe(true));

    await act(async () => {
      rerender({ fetch: second });
    });
    await waitFor(() =>
      expect(result.current.options.map((o) => o.value)).toEqual(["mg/kg"])
    );
  });
});
