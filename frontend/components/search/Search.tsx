"use client";

import { useRef, useState } from "react";

import SearchSuggestions from "@/components/search/SearchSuggestions";
import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";

interface SearchProps {
  onFocusIn: () => void;
  onFocusOut: () => void;
}

const Search = ({ onFocusIn, onFocusOut }: SearchProps) => {
  const { suggestions, isLoading } = useSearchAutocompleteOptions();
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [isDropdownFocused, setIsDropdownFocused] = useState(false);
  const [keyboardSelectedSuggestionIndex, setKeyboardSelectedSuggestionIndex] =
    useState(-1);

  const suggestionItemClickedRef = useRef(false);

  const handleFocusIn = () => {
    setIsDropdownFocused(true);
    onFocusIn();
  };

  const handleFocusOut = () => {
    if (!suggestionItemClickedRef.current) {
      setIsDropdownFocused(false);
      onFocusOut();
    }
  };

  const shouldShowDropdown = isDropdownFocused && searchTerm.length > 0;

  return (
    <>
      <div className="relative" onFocus={handleFocusIn} onBlur={handleFocusOut}>
        {shouldShowDropdown && <SearchSuggestions />}
      </div>
    </>
  );
};

Search.displayName = "Search";

export default Search;
