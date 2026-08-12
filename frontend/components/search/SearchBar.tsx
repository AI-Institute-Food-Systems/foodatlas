"use client";

import { useContext, useEffect, useRef, useState } from "react";
import { flushSync } from "react-dom";
import { usePathname, useRouter } from "next/navigation";
import { ThreeDot } from "react-loading-indicators";
import { MdClose, MdSearch } from "react-icons/md";

import Button from "@/components/basic/Button";
import SearchSuggestions from "@/components/search/SearchSuggestions";
import TryChips from "@/components/search/TryChips";
import { AutocompleteContext } from "@/context/autocompleteContext";
import { useNavigationSignal } from "@/context/navigationContext";
import { SearchContext } from "@/context/searchContext";
import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";
import { usePaginations } from "@/context/paginationsContext";
import { encodeSpace } from "@/utils/utils";

// Static placeholder — replaces the previous typewriter-cycler effect.
// Curated examples now live in the `<TryChips>` row below the input.
// Shorter variant on phones since the full placeholder overflows the
// input width. Kept in sync with the SSR preview in <SearchWrapper/>.
const SEARCH_PLACEHOLDER_MD =
  "Search bioactivities, foods, chemicals, diseases…";
const SEARCH_PLACEHOLDER_SM = "Search FoodAtlas…";

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
    suppressTransition,
  } = useContext(SearchContext);
  const { isLoading } = useSearchAutocompleteOptions();
  const { setTablePaginations, getTablePaginations } = usePaginations();
  const { autocompleteTerm } = useContext(AutocompleteContext);
  const { startNav } = useNavigationSignal();

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

  // Cooldown after an outside-tap dismiss. When the fly-up collapses
  // the input snaps back to its docked position, and the residual
  // tap can land on it and refocus. During the cooldown window,
  // bounce any incoming focus by immediately blurring.
  const outsideDismissRef = useRef(false);

  // Dismiss on outside tap. iOS Safari is inconsistent: sometimes it
  // dismisses the keyboard WITHOUT firing blur on the input, leaving
  // the fly-up stuck open. Update `isFocused` directly here instead
  // of relying on the blur-event chain. `preventDefault` blocks the
  // subsequent mouse/click cascade so the residual tap can't land
  // on the now-docked input and refocus it.
  useEffect(() => {
    if (!isFocused) return;
    const dismiss = (e: Event) => {
      const content = document.getElementById("search-component");
      if (!content || content.contains(e.target as Node)) return;
      e.preventDefault();
      outsideDismissRef.current = true;
      inputRef.current?.blur();
      setSelectedSuggestion(-1);
      setIsFocused(false);
      window.setTimeout(() => {
        outsideDismissRef.current = false;
      }, 400);
    };
    document.addEventListener("pointerdown", dismiss);
    return () => document.removeEventListener("pointerdown", dismiss);
  }, [isFocused, inputRef, setIsFocused, setSelectedSuggestion]);

  // Lock page scroll while the search is focused. Uses the same
  // `.modal-open` class the Modal component toggles (see globals.css) —
  // <html> is the scroll container in this app (`overflow-y: scroll
  // !important`), so anything less specific gets overridden. The base
  // rule already reserves the scrollbar gutter, so there's no
  // horizontal jump at the moment of focus. Skipped on mobile — the
  // on-screen keyboard already collapses the viewport, and a hard
  // scroll lock there fights iOS's own gestures.
  useEffect(() => {
    if (!isFocused) return;
    if (typeof window === "undefined") return;
    if (!window.matchMedia("(min-width: 768px)").matches) return;
    document.documentElement.classList.add("modal-open");
    return () => {
      document.documentElement.classList.remove("modal-open");
    };
  }, [isFocused]);

  // Detect keyboard dismissal that happens OUTSIDE our webview — e.g.
  // taps on Safari's URL bar or its side gutters, or the keyboard's
  // own down-arrow key. Those never reach our pointerdown listener,
  // but they do shrink+grow the visualViewport. Track "keyboard is
  // open" via a viewport shrink, then dismiss the fly-up when it
  // grows back to (approximately) the pre-keyboard height.
  useEffect(() => {
    if (!isFocused) return;
    const vv = window.visualViewport;
    if (!vv) return;
    const initialHeight = vv.height;
    let keyboardOpen = false;
    const onResize = () => {
      const shrunk = initialHeight - vv.height;
      if (shrunk > 100) {
        keyboardOpen = true;
      } else if (keyboardOpen && shrunk < 20) {
        inputRef.current?.blur();
        setSelectedSuggestion(-1);
        setIsFocused(false);
      }
    };
    vv.addEventListener("resize", onResize);
    return () => vv.removeEventListener("resize", onResize);
  }, [isFocused, inputRef, setIsFocused, setSelectedSuggestion]);

  // Track viewport width so we can swap the placeholder between the
  // short mobile variant and the full desktop variant. matchMedia
  // handles resize + orientation change; `change` event fires once
  // per breakpoint cross so it's cheap.
  useEffect(() => {
    if (typeof window === "undefined") return;
    const mq = window.matchMedia("(min-width: 768px)");
    const apply = () => {
      setPlaceholder(mq.matches ? SEARCH_PLACEHOLDER_MD : SEARCH_PLACEHOLDER_SM);
    };
    apply();
    mq.addEventListener("change", apply);
    return () => mq.removeEventListener("change", apply);
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
    startNav();
    router.push(`/results?term=${searchTerm}`);
  };

  // @ts-ignore
  const handleKeyDown = (event) => {
    if (event.key === "Enter") {
      event.currentTarget.blur();
      if (selectedSuggestion !== -1) {
        setIsVisible(false);
        startNav();
        router.push(
          `/${
            cachedSuggestions[selectedSuggestion].entity_type
          }/${encodeURIComponent(
            encodeSpace(cachedSuggestions[selectedSuggestion].common_name)
          )}`
        );
      } else {
        if (searchTerm.length > 0) {
          // Intermediate hint before the results page's own useEffect
          // takes over. Same half-navbar-gap values so the bar
          // doesn't visibly jump during the navigation.
          setOffsetTop(
            typeof window !== "undefined" &&
              window.matchMedia("(min-width: 768px)").matches
              ? 84
              : 72,
          );
          startNav();
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
    // If focus arrives during the outside-dismiss cooldown, it's the
    // residual click from the same tap that just dismissed us —
    // bounce it instead of re-opening the fly-up.
    if (outsideDismissRef.current) {
      inputRef.current?.blur();
      return;
    }
    // Track focus directly off the input's onFocus event. The previous
    // approach polled `document.activeElement` through a useEffect with
    // a non-reactive dependency, which only re-synced on unrelated
    // re-renders — so after a programmatic blur (Esc, X) clicking the
    // input back wouldn't re-flip `isFocused` until the user typed.
    //
    // `flushSync` forces React to apply the fly-up class synchronously
    // before this handler returns; without it iOS Safari sees the
    // input at its pre-fly-up position when computing "scroll into
    // view" and yanks the page down.
    flushSync(() => {
      setSelectedSuggestion(-1);
      setIsFocused(true);
    });
    // Fallback: on iOS Safari the keyboard-appear animation still
    // triggers a scroll even with `interactive-widget=resizes-content`
    // (which only lands on iOS 16.4+). Reset scrollY across BOTH the
    // window scroll event AND the visualViewport resize event — the
    // latter is what fires when the soft keyboard opens.
    if (typeof window !== "undefined") {
      const y = window.scrollY;
      const reset = () => {
        if (window.scrollY !== y) window.scrollTo(0, y);
      };
      window.addEventListener("scroll", reset, { passive: true });
      window.visualViewport?.addEventListener("resize", reset);
      window.visualViewport?.addEventListener("scroll", reset);
      window.setTimeout(() => {
        window.removeEventListener("scroll", reset);
        window.visualViewport?.removeEventListener("resize", reset);
        window.visualViewport?.removeEventListener("scroll", reset);
      }, 800);
    }
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
        className={`z-50 w-full px-4 md:px-24 ${
          // Exactly ONE position utility, chosen here rather than layered.
          // This used to emit `absolute` unconditionally and append `fixed`
          // when focused; with both classes present Tailwind's source order
          // decides, and it emits `.absolute` after `.fixed`, so the intended
          // fixed positioning always lost. At scroll top the two are visually
          // identical, so it only surfaced when opening search partway down a
          // page — the bar stayed pinned to its document position and scrolled
          // away instead of sitting under the navbar.
          //
          // Fixed also matters on iOS: Safari's "scroll input into view" sees
          // a fixed input as already in view, whereas an absolute one at the
          // same visual spot still has a document-space Y it tries to scroll
          // to, yanking the page down on tap.
          //
          // 72 mobile / 84 desktop = navbar bottom + half a navbar height.
          isFocused
            ? "fixed inset-0 top-[72px] md:top-[84px] -right-4"
            : "absolute"
        } ${isResultsPage || suppressTransition ? "" : "duration-[250ms]"}`}
        ref={containerRef}
        // Inline top wins over the responsive `top-*` classes, so it must not
        // be applied while focused — otherwise the docked offset would drag
        // the fixed overlay back to a stale document-space position.
        style={isFocused ? undefined : { top: offsetTop || 50 }}
      >
          {/* Outer wrapper already applies `px-3 md:px-12` (matches
           * navbar / footer / page layout inset). The inner content
           * only needs the max-width cap + centering. */}
          <div>
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
                  // Apothecary/cabinet vocab: same thin light-tint
                  // border + subtle inset shadow the Card uses, serif-
                  // italic placeholder that echoes the News eyebrow and
                  // subhead, and an accent focus ring instead of the
                  // gray browser outline. Kept the input body sans so
                  // the typed query stays legible at glance.
                  className="pl-12 pr-4 w-full h-12 rounded-xl border-[1.5px] border-light-50/[0.08] bg-light-950/70 backdrop-blur-2xl saturate-150 shadow-[inset_0_2px_4px_rgba(255,249,242,0.03)] text-light-100 placeholder:font-serif placeholder:italic placeholder:text-light-500 transition-colors duration-200 hover:border-light-50/20 focus:outline-none focus:border-accent-500/50 focus:shadow-[inset_0_2px_4px_rgba(255,249,242,0.03),0_0_0_3px_rgba(255,87,34,0.12)]"
                  type="text"
                  value={searchTerm}
                  placeholder={placeholder}
                  aria-label="Search foods, chemicals and diseases"
                  onChange={handleInputChange}
                  onFocus={handleFocus}
                  // Chrome + Safari (desktop and mobile) both scroll
                  // the focused input into view on click. That's what
                  // makes the page appear to "fly up" — content shifts
                  // under the fixed navbar. Preempt by preventing the
                  // default (which is what triggers the focus + auto-
                  // scroll) and calling focus manually with
                  // `preventScroll: true`.
                  onMouseDown={(e) => {
                    if (document.activeElement === e.currentTarget) return;
                    e.preventDefault();
                    e.currentTarget.focus({ preventScroll: true });
                  }}
                  // iOS Safari's touch→focus path can bypass mousedown
                  // entirely, so mirror the preempt here. preventDefault
                  // on touchend (not touchstart) — touchstart preventDefault
                  // kills scroll gestures, whereas touchend only blocks the
                  // synthesized focus-triggered scroll.
                  onTouchEnd={(e) => {
                    if (document.activeElement === e.currentTarget) return;
                    e.preventDefault();
                    e.currentTarget.focus({ preventScroll: true });
                  }}
                  // @ts-ignore
                  onBlur={handleBlur}
                  onKeyDown={handleKeyDown}
                />
              </div>
              {/* Try-out chips — only while the input is empty. Once
               * the user starts typing, suggestions take the row so
               * the two lists don't compete for the same visual slot. */}
              {autocompleteTerm.length === 0 && (
                <div className="mt-3">
                  <TryChips />
                </div>
              )}
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
