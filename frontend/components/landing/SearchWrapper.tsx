"use client";

import { useContext, useEffect, useRef } from "react";

import { SearchContext } from "@/context/searchContext";

// Reserves space for the SearchBar inside the hero's normal flow and
// measures its own position so the portaled <SearchBar/> can align
// itself via SearchContext.offsetTop. Removes the previous magic
// pixel offsets — when the hero copy resizes (longer h1, smaller
// breakpoint, etc.) the search overlay tracks automatically.
//
// Height covers: 3rem search input (h-12) + ~0.75rem mt-3 gap + ~2rem
// TryChips row + ~0.5rem breathing room ≈ 6.5rem on desktop.
const SearchWrapper = () => {
  const ref = useRef<HTMLDivElement>(null);
  const { setOffsetTop, setIsVisible, setIsFocused } = useContext(SearchContext);

  useEffect(() => {
    setIsVisible(true);
    setIsFocused(false);
  }, [setIsVisible, setIsFocused]);

  useEffect(() => {
    const measure = () => {
      if (!ref.current) return;
      const rect = ref.current.getBoundingClientRect();
      setOffsetTop(Math.round(rect.top + window.scrollY));
    };
    measure();
    // Re-measure on resize + after fonts settle so the overlay tracks
    // any reflow of the hero copy above the spacer.
    window.addEventListener("resize", measure);
    const raf = requestAnimationFrame(measure);
    return () => {
      window.removeEventListener("resize", measure);
      cancelAnimationFrame(raf);
    };
  }, [setOffsetTop]);

  return (
    <div
      ref={ref}
      aria-hidden
      className="w-full max-w-2xl h-[6.5rem] md:h-[7rem]"
    />
  );
};

SearchWrapper.displayName = "SearchWrapper";

export default SearchWrapper;
