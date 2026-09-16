// Regression tests for the results page and the URL term.
//
// Cold-loading /results?term=garlic (deep link, refresh, shared URL)
// rendered "No matches found" although the API had rows: the page wrote
// the autocomplete term directly while SearchBar's own mirror effect
// wrote its still-empty input over it, nulling the SWR key. And a bare
// /results threw, because `searchParams.term` was undefined by the time
// it reached `.length`.

import { act, render, screen, waitFor } from "@testing-library/react";
import type { ReactElement } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), back: vi.fn() }),
  usePathname: () => "/results",
  useSearchParams: () => new URLSearchParams(),
}));

// The bar's own dropdown and chips fetch on their own; they are not what
// is under test and would double the request log.
vi.mock("@/components/search/SearchSuggestions", () => ({
  default: () => null,
}));
vi.mock("@/components/search/TryChips", () => ({ default: () => null }));

import ResultsPage from "@/app/(everything-else)/results/page";
import SearchBar from "@/components/search/SearchBar";
import { AutocompleteProvider } from "@/context/autocompleteContext";
import { NavigationProvider } from "@/context/navigationContext";
import { PaginationsProvider } from "@/context/paginationsContext";
import { SearchProvider } from "@/context/searchContext";

const garlicPayload = {
  data: [
    {
      foodatlas_id: "e1595",
      common_name: "garlic bulb",
      entity_type: "food",
      synonyms: [],
      associations: 344,
    },
  ],
  metadata: { total_rows: 1, current_page: 1, total_pages: 1 },
};

const renderApp = (node: ReactElement) =>
  render(
    <NavigationProvider>
      <PaginationsProvider>
        <SearchProvider>
          <AutocompleteProvider>
            {/* Same order as app/layout.tsx: the page is `children`,
                the bar is mounted AFTER it — so the bar's mount effects
                run last, which is exactly how its empty input used to
                win over the page's term. */}
            {node}
            <SearchBar />
          </AutocompleteProvider>
        </SearchProvider>
      </PaginationsProvider>
    </NavigationProvider>
  );

describe("/results cold load", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockReset();
    fetchMock.mockResolvedValue({ json: async () => garlicPayload });
    vi.stubGlobal("fetch", fetchMock);
    window.matchMedia = vi.fn().mockReturnValue({
      matches: true,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    }) as never;
  });
  afterEach(() => vi.unstubAllGlobals());

  it("fetches the URL term and renders its results", async () => {
    await act(async () => {
      renderApp(<ResultsPage searchParams={{ term: "garlic" }} />);
    });
    await waitFor(() =>
      expect(screen.getByText(/1 results for "garlic"/)).toBeInTheDocument()
    );
    const urls = fetchMock.mock.calls.map((c) => String(c[0]));
    expect(urls.some((u) => u.includes("term=garlic"))).toBe(true);
    expect(screen.queryByText(/No matches found/)).not.toBeInTheDocument();
  });

  it("puts the URL term in the search box", async () => {
    await act(async () => {
      renderApp(<ResultsPage searchParams={{ term: "garlic" }} />);
    });
    await waitFor(() =>
      expect(screen.getByRole("textbox")).toHaveValue("garlic")
    );
  });

  it("does not throw on /results without a term, and prompts instead", async () => {
    await act(async () => {
      renderApp(<ResultsPage searchParams={{}} />);
    });
    expect(screen.getByText(/Type something above/)).toBeInTheDocument();
    expect(screen.queryByText(/No matches found/)).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
