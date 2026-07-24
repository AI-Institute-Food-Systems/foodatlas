"use client";

import Link from "next/link";
import { useContext, useEffect, useLayoutEffect, useRef, useState } from "react";
import { MdArrowForward } from "react-icons/md";

import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";
import SuggestionItem from "@/components/search/SuggestionItem";
import { AutocompleteContext } from "@/context/autocompleteContext";
import { SearchContext } from "@/context/searchContext";
import { Suggestion } from "@/types/Suggestion";

// Safe bottom gap so the dropdown never kisses the viewport edge or
// gets clipped by mobile browser chrome.
const BOTTOM_GAP_PX = 16;

// One-shot batch size for the dropdown — big enough that most queries
// fit entirely, small enough that the initial paint stays snappy.
// When there are more matches than this, the "See all N results" footer
// routes to /results for full pagination.
const SUGGESTION_BATCH_SIZE = 30;

const SearchSuggestions = () => {
  const { autocompleteTerm } = useContext(AutocompleteContext);
  const { suggestions, totalRows, isLoading } = useSearchAutocompleteOptions(
    SUGGESTION_BATCH_SIZE,
  );
  const {
    selectedSuggestion,
    setSelectedSuggestion,
    cachedSuggestions,
    setCachedSuggestions,
  } = useContext(SearchContext);
  // Cache the total alongside suggestions so the "See all N" footer
  // survives brief refetches (typing keeps the previous list visible
  // until the new payload lands).
  const [cachedTotal, setCachedTotal] = useState<number>(0);
  useEffect(() => {
    if (typeof totalRows === "number") setCachedTotal(totalRows);
  }, [totalRows]);

  const scrollRef = useRef<HTMLDivElement | null>(null);
  const [maxHeight, setMaxHeight] = useState<number | null>(null);

  // TODO: we could clear the cache every time the searchbar is cleared completely; e.g. user types "toma", deletes it all and starts search for "chi"
  // update cache
  useEffect(() => {
    if (suggestions) setCachedSuggestions(suggestions);
  }, [setCachedSuggestions, suggestions]);

  // Fill whatever vertical space is available between the dropdown's
  // top and the bottom of the visual viewport. visualViewport.height
  // shrinks when the mobile keyboard opens, so we track that too.
  useLayoutEffect(() => {
    if (typeof window === "undefined") return;
    const el = scrollRef.current;
    if (!el) return;
    const recompute = () => {
      const node = scrollRef.current;
      if (!node) return;
      const top = node.getBoundingClientRect().top;
      const vh = window.visualViewport?.height ?? window.innerHeight;
      const next = Math.max(160, Math.floor(vh - top - BOTTOM_GAP_PX));
      setMaxHeight(next);
    };
    recompute();
    window.addEventListener("resize", recompute);
    window.visualViewport?.addEventListener("resize", recompute);
    window.addEventListener("scroll", recompute, { passive: true });
    return () => {
      window.removeEventListener("resize", recompute);
      window.visualViewport?.removeEventListener("resize", recompute);
      window.removeEventListener("scroll", recompute);
    };
  }, [cachedSuggestions.length, autocompleteTerm.length]);

  // TODO: add feature where hitting enter performs different actions depending whether the target was selected via mouse or keyboard
  const handleMouseMove = (index: number) => {
    setSelectedSuggestion(index);
  };

  return (
    <div className="w-full rounded z-50 foodatlas-search">
      {cachedSuggestions?.length > 0 && autocompleteTerm.length > 0 && (
        <div
          ref={scrollRef}
          style={maxHeight ? { maxHeight } : undefined}
          className="flex flex-col rounded-xl border border-light-50/10 bg-light-950/80 backdrop-blur-xl shadow-xl shadow-black/40 overflow-hidden"
        >
          <div
            className="flex flex-col overflow-y-auto flex-1 min-h-0"
            onMouseLeave={() => setSelectedSuggestion(-1)}
          >
            {cachedSuggestions?.map(
              (suggestion: Suggestion, index: number) => (
                <SuggestionItem
                  key={index}
                  suggestion={suggestion}
                  isSelected={selectedSuggestion === index}
                  onMouseMove={() => handleMouseMove(index)}
                />
              ),
            )}
          </div>
          {/* "See all N results" footer — only when the total exceeds
           * what we fetched, so the user knows to escape to /results
           * for the full paginated view. Sticky at the bottom of the
           * dropdown so it's discoverable without scrolling. */}
          {cachedTotal > cachedSuggestions.length && (
            <Link
              // id="foodatlas-search" opts this element out of the
              // SearchBar's blur-dismiss (see SearchBar.tsx handleBlur).
              // Without it, focus leaves the input, the dropdown
              // unmounts before the Link's onClick fires, and Next's
              // client-side navigation is lost — the user sees the
              // suggestions disappear with no page change.
              id="foodatlas-search"
              href={`/results?term=${encodeURIComponent(autocompleteTerm)}`}
              className="flex items-center justify-between gap-2 px-4 py-2.5 border-t border-light-50/10 bg-light-900/40 text-sm text-light-200 hover:bg-light-800/60 hover:text-light-50 transition-colors"
            >
              <span>
                See all{" "}
                <span className="font-mono tabular-nums">{cachedTotal}</span>{" "}
                results
              </span>
              <MdArrowForward className="size-4" />
            </Link>
          )}
        </div>
      )}
      {cachedSuggestions.length === 0 &&
        !isLoading &&
        autocompleteTerm.length > 0 && (
          <p className="w-full text-center py-12 text-sm">
            No associations found
          </p>
        )}
    </div>
  );
};

SearchSuggestions.displayName = "SearchSuggestions";

export default SearchSuggestions;
