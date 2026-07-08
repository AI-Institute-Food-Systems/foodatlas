"use client";

import { useContext, useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { ThreeDot } from "react-loading-indicators";
import { MdClose, MdSearch } from "react-icons/md";

import Button from "@/components/basic/Button";
import SearchSuggestions from "@/components/search/SearchSuggestions";
import TryChips from "@/components/search/TryChips";
import { AutocompleteContext } from "@/context/autocompleteContext";
import { SearchContext } from "@/context/searchContext";
import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";
import { usePaginations } from "@/context/paginationsContext";
import { encodeSpace } from "@/utils/utils";

// Static placeholder — replaces the previous typewriter-cycler effect.
// Curated examples now live in the `<TryChips>` row below the input.
const SEARCH_PLACEHOLDER =
  "Search bioactivities, foods, chemicals, diseases…";

const SearchBar = () => {
  const router = useRouter();
  const pathname = usePathname();
  const { setAutocompleteTerm } = useContext(AutocompleteContext);
  const {
    inputRef,
    containerRef,
    offsetTop,
    setOffsetTop,
    isFocused,
    setIsFocused,
    searchTerm,
    setSearchTerm,
    placeholder,
    setPlaceholder,
    selectedSuggestion,
    setSelectedSuggestion,
    cachedSuggestions,
    isVisible,
    setIsVisible,
  } = useContext(SearchContext);
  const { isLoading } = useSearchAutocompleteOptions();
  const { setTablePaginations, getTablePaginations } = usePaginations();
  const { autocompleteTerm } = useContext(AutocompleteContext);

  // useEffect(() => {
  //   const handleScroll = () => {
  //     if (containerRef.current) {
  //       const rect = containerRef.current.getBoundingClientRect();
  //       setOffsetTop(rect.top);
  //     }
  //   };

  //   handleScroll();
  //   window.addEventListener("scroll", handleScroll);
  //   return () => window.removeEventListener("scroll", handleScroll);
  // }, []);

  useEffect(() => {
    setAutocompleteTerm(searchTerm);
  }, [searchTerm, setAutocompleteTerm]);

  // Route-change cleanup. Runs ONLY when pathname changes (no
  // `isVisible` dep) — otherwise the effect would also fire when the
  // navbar search button flips isVisible true on an entity page, and
  // the !hostsSearch branch would immediately undo it.
  // - Hosting route arrival (`/`, `/results`): reset focus/selection
  //   so the bar lands in its compact unfocused state.
  // - Entity page arrival: fade the bar out (it stayed mounted from
  //   the previous page), leave focus state untouched so the fade
  //   doesn't visually morph mid-transition.
  // Also: clear the search term on every route change EXCEPT
  // `/results` (which needs to keep the term to render the result
  // list). Otherwise a stale term persists across navigations.
  useEffect(() => {
    const hostsSearch = pathname === "/" || pathname.startsWith("/results");
    if (!pathname.startsWith("/results")) {
      setSearchTerm("");
    }
    if (hostsSearch) {
      setIsFocused(false);
      setSelectedSuggestion(-1);
    } else {
      setIsVisible(false);
    }
  }, [
    pathname,
    setIsVisible,
    setIsFocused,
    setSelectedSuggestion,
    setSearchTerm,
  ]);

  // On entity pages the bar is an overlay with no anchor section — if
  // the user blurs out of it (Esc, click outside) the bar should
  // dismiss entirely instead of sitting around dim and inactive.
  // Hosting routes (`/`, `/results`) keep the bar permanently mounted.
  useEffect(() => {
    const hostsSearch = pathname === "/" || pathname.startsWith("/results");
    if (!hostsSearch && !isFocused && isVisible) {
      setIsVisible(false);
    }
  }, [pathname, isFocused, isVisible, setIsVisible]);

  // Set the static placeholder once. The SearchContext still owns the
  // value so the input can read it through the same prop as before;
  // removing the typewriter loop just leaves a single useEffect.
  useEffect(() => {
    setPlaceholder(SEARCH_PLACEHOLDER);
  }, [setPlaceholder]);

  // @ts-ignore
  const handleInputChange = (event) => {
    event.preventDefault();
    setSearchTerm(event.target.value);
  };

  const handleSearchBarClear = () => setSearchTerm("");

  const handleSearchButtonClick = () => {
    setTablePaginations("results-page", 1, 20);
    // The Search button carries id="foodatlas-search" so handleBlur
    // intentionally SKIPS tearing down focus when the user clicks it
    // (otherwise the dropdown would unmount mid-click and the
    // navigation would never fire). That same suppression means we
    // have to clean up focus state ourselves here — without this,
    // clicking Search a second time on /results does nothing because
    // pathname is unchanged and the route-change effect doesn't fire.
    setIsFocused(false);
    setSelectedSuggestion(-1);
    inputRef.current?.blur();
    router.push(`/results?term=${searchTerm}`);
  };

  // @ts-ignore
  const handleKeyDown = (event) => {
    if (event.key === "Enter") {
      event.currentTarget.blur();
      if (selectedSuggestion !== -1) {
        setIsVisible(false);
        router.push(
          `/${
            cachedSuggestions[selectedSuggestion].entity_type
          }/${encodeURIComponent(
            encodeSpace(cachedSuggestions[selectedSuggestion].common_name)
          )}`
        );
      } else {
        if (searchTerm.length > 0) {
          setOffsetTop(96);
          router.push(`/results?term=${searchTerm}`);
        }
      }
    } else if (event.key === "ArrowDown") {
      event.preventDefault();
      // @ts-ignore
      setSelectedSuggestion((prevIndex) =>
        prevIndex < cachedSuggestions.length - 1 ? prevIndex + 1 : prevIndex
      );
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      // @ts-ignore
      setSelectedSuggestion((prevIndex) =>
        prevIndex > 0 ? prevIndex - 1 : prevIndex
      );
    } else if (event.key === "Escape") {
      // Was: `handleBlur()` directly, which threw because handleBlur
      // dereferences `e.relatedTarget`. Calling .blur() on the input
      // dispatches a real blur event (with relatedTarget=null), which
      // falls through to handleBlur's close branch and dismisses the
      // suggestion dropdown / unfocuses the bar.
      inputRef.current?.blur();
    }
  };

  const handleFocus = () => {
    // Track focus directly off the input's onFocus event. The previous
    // approach polled `document.activeElement` through a useEffect with
    // a non-reactive dependency, which only re-synced on unrelated
    // re-renders — so after a programmatic blur (Esc, X) clicking the
    // input back wouldn't re-flip `isFocused` until the user typed.
    setSelectedSuggestion(-1);
    setIsFocused(true);
  };

  const handleBlur = (e: React.MouseEvent<HTMLDivElement>) => {
    // @ts-ignore
    if (e.relatedTarget?.id !== "foodatlas-search") {
      setSelectedSuggestion(-1);
      // Do NOT call setTablePaginations("results-page") here — that
      // update cascades through PaginationsContext and causes every
      // usePaginations() consumer (BioactivityTable etc.) to re-render,
      // which re-triggers their data-fetch useEffect and shows a
      // loading skeleton. Resetting results-page pagination only needs
      // to happen when actually navigating to /results (handled in
      // handleSearchButtonClick and the Enter branch).
      setIsFocused(false);
    }
  };

  const isResultsPage = pathname.startsWith("/results");

  // Keep the bar mounted across route transitions and fade it via
  // opacity instead — without this, navigating from / to an entity
  // page unmounts the bar instantly while the destination is still
  // loading, so the user sees a bar-less landing for a few hundred
  // milliseconds. `pointer-events-none` while hidden so the invisible
  // markup doesn't intercept clicks behind it on entity pages.
  return (
    <div
      role="search"
      aria-label="Site search"
      className={`transition-opacity duration-300 ${
        isVisible ? "opacity-100" : "opacity-0 pointer-events-none"
      }`}
      aria-hidden={!isVisible}
    >
      <div
        className={`z-50 w-full absolute px-3 md:px-12 ${
          isFocused ? "absolute inset-0 top-24 -right-4" : ""
        } ${isResultsPage ? "" : "duration-[250ms]"}`}
        ref={containerRef}
        style={{ top: offsetTop || 50 }}
      >
          <div className="px-3 md:px-12">
            <div className="mx-auto max-w-5xl" id="search-component">
              {/* search input */}
              <div className="relative flex items-center select-none">
                {/* search icon */}
                <div className="absolute left-3.5 my-auto w-6 h-6 flex items-center justify-center z-10">
                  {/* loading icon */}
                  {isLoading ? (
                    <ThreeDot
                      color="#a3a3a3"
                      style={{
                        maxWidth: "full",
                        maxHeight: "full",
                        fontSize: "8px",
                        zIndex: "10",
                      }}
                    />
                  ) : (
                    // search icon
                    <MdSearch className="my-auto text-light-400 w-full h-full" />
                  )}
                </div>
                {/* search input clear & search button */}
                <div className="absolute right-3 flex items-center gap-3">
                  {/* search input clear */}
                  {searchTerm && (
                    <button
                      type="button"
                      aria-label="Clear search"
                      className="z-50 cursor-pointer focus:outline-none"
                      // Stop mousedown so the input doesn't blur
                      // BEFORE the click reaches us — without this the
                      // bar collapses mid-click and the X handler
                      // misses. Then in the click: clear the term and
                      // explicitly blur the input so the bar
                      // deactivates after — clicking X means "I'm done
                      // with this query", not "focus the empty input".
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={(e) => {
                        e.stopPropagation();
                        handleSearchBarClear();
                        inputRef.current?.blur();
                      }}
                    >
                      <MdClose className="w-6 h-6 text-light-400 hover:text-light-300 transition duration-300 ease-in-out" />
                    </button>
                  )}
                  {/* search button */}
                  {isFocused && (
                    <Button
                      className="z-50"
                      variant="filled"
                      onClick={handleSearchButtonClick}
                      id="foodatlas-search"
                      isDisabled={searchTerm.length === 0}
                    >
                      Search
                    </Button>
                  )}
                </div>
                {/* search input */}
                <input
                  ref={inputRef}
                  className={`pl-12 w-full h-12 rounded-lg border-[1.5px] border-light-600 bg-light-950/50 backdrop-blur-3xl saturate-150 hover:outline-white text-light-100 transition duration-100 ease-in-out outline-light-50/60 placeholder-light-500 ${
                    isFocused &&
                    cachedSuggestions.length > 0 &&
                    autocompleteTerm.length > 0
                      ? "rounded-b-none"
                      : ""
                  }`}
                  type="text"
                  value={searchTerm}
                  placeholder={placeholder}
                  aria-label="Search foods, chemicals and diseases"
                  onChange={handleInputChange}
                  onFocus={handleFocus}
                  // @ts-ignore
                  onBlur={handleBlur}
                  onKeyDown={handleKeyDown}
                />
              </div>
              {/* Try-out chips — always visible so the entry-points
               * are discoverable even mid-session. */}
              <div className="mt-3">
                <TryChips />
              </div>
              {/* Suggestions appear below the chips when focused,
               * with a small gap so the two rows feel distinct. */}
              {isFocused && (
                <div className="mt-3">
                  <SearchSuggestions />
                </div>
              )}
            </div>
          </div>
        </div>
      <div
        className={`fixed inset-0 z-30 transition-all duration-300 backdrop-blur-md bg-black/30 saturate-150 pointer-events-none ${
          isFocused ? "opacity-100" : "opacity-0"
        }`}
        aria-hidden="true"
      />
    </div>
  );
};

SearchBar.displayName = "SearchBar";

export default SearchBar;
