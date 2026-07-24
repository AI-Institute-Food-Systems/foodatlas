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

// Approximate rendered height of a SuggestionItem row (px). Used to
// size the initial fetch to what the current viewport can display
// without vertical scroll — anything beyond that goes to /results via
// the footer link, not into a scrollable in-dropdown list. Height comes
// from `px-4 py-2.5` (20px vertical padding) + two-line content
// (~34px) + 1px divider ≈ 55.
const ITEM_HEIGHT_PX = 55;

// Rough vertical chrome above the dropdown (nav + input + fly-up gap).
// Slightly generous so we err on fetching a hair fewer than fits,
// rather than one too many that would introduce scroll.
const DROPDOWN_TOP_OFFSET_PX = 180;

// Space reserved for the sticky "See all N" footer so we don't fetch
// items that would be occluded by it. ~42px in practice.
const FOOTER_RESERVE_PX = 42;

// API caps for the fetch batch. Floor keeps very short viewports from
// returning near-zero suggestions; ceiling matches the backend's `le=100`.
const MIN_BATCH = 8;
const MAX_BATCH = 100;

const estimateBatchSize = (): number => {
  if (typeof window === "undefined") return 20;
  const vh = window.visualViewport?.height ?? window.innerHeight;
  const available =
    vh - DROPDOWN_TOP_OFFSET_PX - BOTTOM_GAP_PX - FOOTER_RESERVE_PX;
  const items = Math.floor(available / ITEM_HEIGHT_PX);
  return Math.max(MIN_BATCH, Math.min(MAX_BATCH, items));
};

const SearchSuggestions = () => {
  const { autocompleteTerm } = useContext(AutocompleteContext);
  // Computed once at mount so the initial fetch matches viewport
  // capacity. Not tracked across resize — the batch stays put; the
  // dropdown's dynamic maxHeight already handles small viewport growth
  // by capping display, and shrinkage via the mobile keyboard is
  // handled the same way.
  const [batchSize] = useState<number>(estimateBatchSize);
  const { suggestions, totalRows, isLoading } =
    useSearchAutocompleteOptions(batchSize);
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
