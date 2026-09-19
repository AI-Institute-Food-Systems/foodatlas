import { useContext, useState } from "react";
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

// One file per contract: every client-side umami event, checked against the
// `window.umami.track` spy the tracker script would install in prod.

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

// SWR-backed search hook: hand each test its own settled result.
const searchResult = { suggestions: undefined as unknown, totalRows: 0, isLoading: false };
vi.mock("@/hooks/useSearchAutocompleteOptions", () => ({
  default: () => searchResult,
}));

import Link from "@/components/basic/Link";
import ContactForm from "@/components/contact/ContactForm";
import ErrorPageBeacon from "@/components/misc/ErrorPageBeacon";
import SearchBar from "@/components/search/SearchBar";
import SearchSuggestions from "@/components/search/SearchSuggestions";
import SuggestionItem from "@/components/search/SuggestionItem";
import {
  AutocompleteContext,
  AutocompleteProvider,
} from "@/context/autocompleteContext";
import { NavigationProvider } from "@/context/navigationContext";
import { PaginationsProvider } from "@/context/paginationsContext";
import { SearchProvider } from "@/context/searchContext";
import { apiFetch, clearApiCache } from "@/utils/apiFetch";
import { outboundLinkAttrs } from "@/utils/outboundLink";
import { NO_RESULTS_SETTLE_MS } from "@/utils/searchEvents";

let trackSpy: ReturnType<typeof vi.fn>;

beforeEach(() => {
  trackSpy = vi.fn();
  window.umami = { track: trackSpy };
  clearApiCache();
});

afterEach(() => {
  delete window.umami;
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

const events = (name: string) =>
  trackSpy.mock.calls.filter(([n]) => n === name).map(([, d]) => d);

describe("outbound_link", () => {
  it("Link tags external hrefs with hostname only", () => {
    render(<Link href="https://pubmed.ncbi.nlm.nih.gov/123">x</Link>);
    const a = screen.getByRole("link");
    expect(a).toHaveAttribute("data-umami-event", "outbound_link");
    expect(a).toHaveAttribute("data-umami-event-host", "pubmed.ncbi.nlm.nih.gov");
    expect(a).toHaveAttribute(
      "data-umami-event-url",
      "https://pubmed.ncbi.nlm.nih.gov/123"
    );
  });

  it("Link leaves internal links untagged", () => {
    render(
      <Link href="/contact" isExternal={false}>
        x
      </Link>
    );
    expect(screen.getByRole("link")).not.toHaveAttribute("data-umami-event");
  });

  it("outboundLinkAttrs ignores non-URL hrefs", () => {
    expect(outboundLinkAttrs("mailto:a@b.c")).toEqual({});
    expect(outboundLinkAttrs("/relative")).toEqual({});
    expect(outboundLinkAttrs("not a url")).toEqual({});
  });
});

describe("contact_submit", () => {
  it("reports topic and outcome, never the message", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true }));
    render(<ContactForm isApiAccessRequest />);
    fireEvent.submit(screen.getByRole("button", { name: /send/i }).closest("form")!);
    await waitFor(() => expect(events("contact_submit")).toHaveLength(1));
    expect(events("contact_submit")[0]).toEqual({
      topic: "API Access Request",
      outcome: "sent",
    });
  });

  it("reports error when the request throws", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("down")));
    render(<ContactForm isApiAccessRequest={false} />);
    fireEvent.submit(screen.getByRole("button", { name: /send/i }).closest("form")!);
    await waitFor(() => expect(events("contact_submit")).toHaveLength(1));
    expect(events("contact_submit")[0]).toEqual({
      topic: "General Inquiry",
      outcome: "error",
    });
  });
});

describe("error_page", () => {
  it("fires once on mount with kind and path", () => {
    window.history.replaceState(null, "", "/food/does-not-exist");
    const { rerender } = render(<ErrorPageBeacon kind="not_found" />);
    rerender(<ErrorPageBeacon kind="not_found" />);
    expect(events("error_page")).toEqual([
      { kind: "not_found", path: "/food/does-not-exist" },
    ]);
  });
});

describe("api_fetch_error", () => {
  it("reports the id-stripped path and HTTP status on a non-ok response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({ ok: false, status: 503, json: async () => ({}) })
    );
    await apiFetch("https://api.example/food/e12345/profile?tab=x");
    expect(events("api_fetch_error")).toEqual([
      { path: "/food/{id}/profile", status: 503 },
    ]);
  });

  it("reports status=network when fetch throws", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("offline")));
    await expect(apiFetch("/_proxy-api/v1/foods/42")).rejects.toThrow("offline");
    expect(events("api_fetch_error")).toEqual([
      { path: "/_proxy-api/v1/foods/{id}", status: "network" },
    ]);
  });

  it("stays silent on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({}) })
    );
    await apiFetch("/food/x");
    expect(events("api_fetch_error")).toEqual([]);
  });
});

const suggestion = {
  foodatlas_id: "e123",
  associations: "12",
  entity_type: "food",
  common_name: "Tomato",
  scientific_name: "Solanum lycopersicum",
  synonyms: [],
  external_references: {},
};

const WithTerm = ({ term, children }: { term: string; children: React.ReactNode }) => (
  <AutocompleteContext.Provider value={{ autocompleteTerm: term, setAutocompleteTerm: () => {} }}>
    <SearchProvider>{children}</SearchProvider>
  </AutocompleteContext.Provider>
);

describe("search_select", () => {
  it("reports the normalised query, entity type and id on pick", () => {
    render(
      <WithTerm term="  ToMaTo ">
        <SuggestionItem suggestion={suggestion} isSelected={false} onMouseMove={() => {}} />
      </WithTerm>
    );
    fireEvent.click(screen.getByRole("button"));
    expect(events("search_select")).toEqual([
      { query: "tomato", entity_type: "food", id: "e123" },
    ]);
  });
});

describe("search_submit", () => {
  beforeEach(() => {
    window.matchMedia = vi.fn().mockReturnValue({
      matches: true,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    });
  });

  // The mocked pathname is an entity page, where the bar is mounted but
  // faded out (aria-hidden) — query with hidden: true.
  const input = () => screen.getByRole("textbox", { hidden: true });

  const renderBar = () =>
    render(
      <NavigationProvider>
        <PaginationsProvider>
          <SearchProvider>
            <AutocompleteProvider>
              <SearchBar />
            </AutocompleteProvider>
          </SearchProvider>
        </PaginationsProvider>
      </NavigationProvider>
    );

  it("fires on Enter with no suggestion selected", () => {
    renderBar();
    fireEvent.change(input(), { target: { value: "  Vitamin C " } });
    fireEvent.keyDown(input(), { key: "Enter" });
    expect(events("search_submit")).toEqual([{ query: "vitamin c" }]);
    expect(events("search_select")).toEqual([]);
  });

  it("fires on the Search button", () => {
    renderBar();
    fireEvent.change(input(), { target: { value: "garlic" } });
    fireEvent.focus(input());
    fireEvent.click(
      screen.getByRole("button", { name: /^search$/i, hidden: true })
    );
    expect(events("search_submit")).toEqual([{ query: "garlic" }]);
  });

  it("does not fire on Enter with an empty term", () => {
    renderBar();
    fireEvent.keyDown(input(), { key: "Enter" });
    expect(events("search_submit")).toEqual([]);
  });
});

describe("search_no_results", () => {
  const Harness = () => {
    const { autocompleteTerm } = useContext(AutocompleteContext);
    return <div data-term={autocompleteTerm}><SearchSuggestions /></div>;
  };

  const Typing = ({ initial }: { initial: string }) => {
    const [term, setTerm] = useState(initial);
    return (
      <AutocompleteContext.Provider value={{ autocompleteTerm: term, setAutocompleteTerm: setTerm }}>
        <SearchProvider>
          <button onClick={() => setTerm(term + "e")}>type</button>
          <Harness />
        </SearchProvider>
      </AutocompleteContext.Provider>
    );
  };

  beforeEach(() => {
    vi.useFakeTimers();
    searchResult.suggestions = [];
  });

  it("fires once after the empty result settles, not per keystroke", () => {
    render(<Typing initial="tomatoxx" />);
    // Two more keystrokes before the settle window elapses.
    act(() => vi.advanceTimersByTime(NO_RESULTS_SETTLE_MS / 2));
    fireEvent.click(screen.getByText("type"));
    act(() => vi.advanceTimersByTime(NO_RESULTS_SETTLE_MS / 2));
    fireEvent.click(screen.getByText("type"));
    expect(events("search_no_results")).toEqual([]);
    act(() => vi.advanceTimersByTime(NO_RESULTS_SETTLE_MS));
    expect(events("search_no_results")).toEqual([{ query: "tomatoxxee" }]);
  });

  it("does not fire while results exist or the term is empty", () => {
    searchResult.suggestions = [suggestion];
    const { unmount } = render(<Typing initial="tomato" />);
    act(() => vi.advanceTimersByTime(NO_RESULTS_SETTLE_MS * 2));
    unmount();
    searchResult.suggestions = [];
    render(<Typing initial="   " />);
    act(() => vi.advanceTimersByTime(NO_RESULTS_SETTLE_MS * 2));
    expect(events("search_no_results")).toEqual([]);
  });
});
