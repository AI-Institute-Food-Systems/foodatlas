"use client";

import { useContext, useEffect, useLayoutEffect, useRef, useState } from "react";

import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";
import SuggestionItem from "@/components/search/SuggestionItem";
import { AutocompleteContext } from "@/context/autocompleteContext";
import { SearchContext } from "@/context/searchContext";
import { Suggestion } from "@/types/Suggestion";

// Safe bottom gap so the dropdown never kisses the viewport edge or
// gets clipped by mobile browser chrome.
const BOTTOM_GAP_PX = 16;

const SearchSuggestions = () => {
  const { autocompleteTerm } = useContext(AutocompleteContext);
  const { suggestions, isLoading } = useSearchAutocompleteOptions();
  const {
    selectedSuggestion,
    setSelectedSuggestion,
    cachedSuggestions,
    setCachedSuggestions,
  } = useContext(SearchContext);

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
          className="flex flex-col overflow-y-auto rounded-xl border border-light-50/10 bg-light-950/80 backdrop-blur-xl shadow-xl shadow-black/40"
          onMouseLeave={() => setSelectedSuggestion(-1)}
        >
          {cachedSuggestions?.map((suggestion: Suggestion, index: number) => (
            <SuggestionItem
              key={index}
              suggestion={suggestion}
              isSelected={selectedSuggestion === index}
              onMouseMove={() => handleMouseMove(index)}
            />
          ))}
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
