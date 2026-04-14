import React, { createContext, useState, useRef } from "react";

import { Suggestion } from "@/types/Suggestion";

type SearchContextValue = {
  selectedSuggestion: number;
  setSelectedSuggestion: (number: number) => void;
  cachedSuggestions: Suggestion[];
  setCachedSuggestions: (suggestions: Suggestion[]) => void;
  isFocused: boolean;
  setIsFocused: (focused: boolean) => void;
  searchTerm: string;
  setSearchTerm: (term: string) => void;
  placeholder: string;
  setPlaceholder: (placeholder: string) => void;
  isVisible: boolean;
  setIsVisible: (visible: boolean) => void;
  containerRef: React.RefObject<HTMLDivElement>;
  inputRef: React.RefObject<HTMLInputElement>;
  isSearchFocussed: boolean;
  setIsSearchFocussed: (focussed: boolean) => void;
  offsetTop: number;
  setOffsetTop: (offset: number) => void;
};

export const SearchContext = createContext<SearchContextValue>({
  selectedSuggestion: -1,
  setSelectedSuggestion: () => {},
  cachedSuggestions: [],
  setCachedSuggestions: () => {},
  isFocused: false,
  setIsFocused: () => {},
  searchTerm: "",
  setSearchTerm: () => {},
  placeholder: "",
  setPlaceholder: () => {},
  isVisible: false,
  setIsVisible: () => {},
  containerRef: { current: null },
  inputRef: { current: null },
  isSearchFocussed: false,
  setIsSearchFocussed: () => {},
  offsetTop: 0,
  setOffsetTop: () => {},
});

interface SearchContextProviderProps {
  children: React.ReactNode;
}

export const SearchProvider = ({ children }: SearchContextProviderProps) => {
  const [selectedSuggestion, setSelectedSuggestion] = useState<number>(-1);
  const [cachedSuggestions, setCachedSuggestions] = useState<Suggestion[]>([]);
  const [isFocused, setIsFocused] = useState<boolean>(false);
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [placeholder, setPlaceholder] = useState<string>("");
  const [isVisible, setIsVisible] = useState<boolean>(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [isSearchFocussed, setIsSearchFocussed] = useState<boolean>(false);
  const [offsetTop, setOffsetTop] = useState<number>(0);

  const value: SearchContextValue = {
    selectedSuggestion,
    setSelectedSuggestion,
    cachedSuggestions,
    setCachedSuggestions,
    isFocused,
    setIsFocused,
    searchTerm,
    setSearchTerm,
    placeholder,
    setPlaceholder,
    isVisible,
    setIsVisible,
    containerRef,
    inputRef,
    isSearchFocussed,
    setIsSearchFocussed,
    offsetTop,
    setOffsetTop,
  };

  return (
    <SearchContext.Provider value={value}>{children}</SearchContext.Provider>
  );
};
