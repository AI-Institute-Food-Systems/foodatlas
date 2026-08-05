"use client";

import { useContext, useEffect, useRef } from "react";
import { MdSearch } from "react-icons/md";

import { SearchContext } from "@/context/searchContext";

// Same placeholder text the real SearchBar uses. Duplicated here (not
// imported) so this file can render the static preview during SSR
// without pulling in the SearchBar module. Short variant on phones,
// full variant on md+ — swapped via CSS classes so both ship in the
// SSR HTML and pick correctly per viewport without JS.
const PLACEHOLDER_MD =
  "Search bioactivities, foods, chemicals, diseases…";
const PLACEHOLDER_SM = "Search FoodAtlas…";

// Reserves space for the SearchBar inside the hero's normal flow and
// measures its own position so the portaled <SearchBar/> can align
// itself via SearchContext.offsetTop. Removes the previous magic
// pixel offsets — when the hero copy resizes (longer h1, smaller
// breakpoint, etc.) the search overlay tracks automatically.
//
// Height covers: 3rem search input (h-12) + ~0.75rem mt-3 gap + ~2rem
// TryChips row + ~0.5rem breathing room ≈ 6.5rem on desktop.
//
// The wrapper also renders a static, non-interactive PREVIEW of the
// search input. Rationale: the portaled <SearchBar/> starts at
// `opacity-0` until this component's `useEffect` fires and flips
// `isVisible`, which only happens after React hydration completes —
// 500-1500ms on mid-tier mobile. The preview ships in the SSR HTML so
// the user sees a search field the moment the page paints. Once the
// real bar fades in on top (pixel-identical styling + position via
// measured offsetTop), the swap is imperceptible.
const SearchWrapper = () => {
  const ref = useRef<HTMLDivElement>(null);
  const {
    setOffsetTop,
    setIsVisible,
    setIsFocused,
    setSuppressTransition,
    isVisible,
  } = useContext(SearchContext);

  useEffect(() => {
    // Suppress the container's top-transition for one frame so the
    // freshly-measured offsetTop lands without animating from whatever
    // stale value the context is holding (e.g. 72/84 pinned by a prior
    // /results submission). Re-enable next frame so the intentional
    // fly-up on search submit still animates.
    setSuppressTransition(true);
    setIsVisible(true);
    setIsFocused(false);
    const raf = requestAnimationFrame(() => setSuppressTransition(false));
    return () => cancelAnimationFrame(raf);
  }, [setIsVisible, setIsFocused, setSuppressTransition]);

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
      className="relative w-full max-w-2xl h-[6.5rem] md:h-[7rem]"
    >
      {/* The wrapper is capped at max-w-2xl inside the hero's centered
       * column, but the real portaled SearchBar breaks out to w-full
       * viewport with `px-4 md:px-24` + `max-w-5xl mx-auto` inside.
       * Mirror that exactly here so the preview matches the real bar's
       * rendered width on every viewport — otherwise the user sees a
       * narrower preview "grow" into the wider real bar during the fade.
       *
       * Fade the preview out when the real bar's isVisible flips on;
       * same 300ms duration as the real bar's opacity transition so the
       * swap stays imperceptible, and by the time the user taps and the
       * real bar flies up (isFocused=true), the preview is gone — no
       * static ghost bar left visible underneath. */}
      <div
        className={`absolute top-0 left-1/2 -translate-x-1/2 w-screen px-4 md:px-24 pointer-events-none transition-opacity duration-300 ${
          isVisible ? "opacity-0" : "opacity-100"
        }`}
      >
        <div className="mx-auto max-w-5xl">
          <div className="relative flex items-center h-12 select-none">
            <div className="absolute left-3.5 w-6 h-6 flex items-center justify-center">
              <MdSearch className="text-light-400 w-full h-full" />
            </div>
            <div className="pl-12 pr-4 flex items-center w-full h-12 rounded-xl border-[1.5px] border-light-50/[0.08] bg-light-950/70 backdrop-blur-2xl saturate-150 shadow-[inset_0_2px_4px_rgba(255,249,242,0.03)]">
              <span className="font-serif italic text-light-500 truncate md:hidden">
                {PLACEHOLDER_SM}
              </span>
              <span className="font-serif italic text-light-500 truncate hidden md:inline">
                {PLACEHOLDER_MD}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

SearchWrapper.displayName = "SearchWrapper";

export default SearchWrapper;
