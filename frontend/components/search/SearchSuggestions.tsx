"use client";

import { useContext, useEffect } from "react";

import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";
import SuggestionItem from "@/components/search/SuggestionItem";
import { AutocompleteContext } from "@/context/autocompleteContext";
import { SearchContext } from "@/context/searchContext";
import { Suggestion } from "@/types/Suggestion";

const SearchSuggestions = () => {
  const { autocompleteTerm } = useContext(AutocompleteContext);
  const { suggestions, isLoading } = useSearchAutocompleteOptions();
  const {
    selectedSuggestion,
    setSelectedSuggestion,
    cachedSuggestions,
    setCachedSuggestions,
  } = useContext(SearchContext);

  // TODO: we could clear the cache every time the searchbar is cleared completely; e.g. user types "toma", deletes it all and starts search for "chi"
  // update cache
  useEffect(() => {
    if (suggestions) setCachedSuggestions(suggestions);
  }, [setCachedSuggestions, suggestions]);

  // TODO: add feature where hitting enter performs different actions depending whether the target was selected via mouse or keyboard
  const handleMouseMove = (index: number) => {
    setSelectedSuggestion(index);
  };

  return (
    <div className="w-full rounded z-50 foodatlas-search">
      {cachedSuggestions?.length > 0 && autocompleteTerm.length > 0 && (
        <div className="flex flex-col max-h-[80vh] overflow-y-auto border-[1.5px] border-t-0 bg-light-950/50 border-light-600 rounded-t-none rounded-lg backdrop-blur-3xl">
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
