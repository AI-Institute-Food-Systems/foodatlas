"use client";

import { useContext, useEffect } from "react";

import { SearchContext } from "@/context/searchContext";

const SearchWrapper = () => {
  const { containerRef, setIsVisible, setIsFocused, setOffsetTop } =
    useContext(SearchContext);

  useEffect(() => {
    setOffsetTop(540);
    setIsVisible(true);
    setIsFocused(false);
  }, [setIsFocused, setIsVisible, setOffsetTop]);

  return <></>;
};

SearchWrapper.displayName = "SearchWrapper";

export default SearchWrapper;
