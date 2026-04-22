"use client";

import { useContext, useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { ThreeDot } from "react-loading-indicators";
import { MdClose, MdSearch } from "react-icons/md";

import Button from "@/components/basic/Button";
import SearchInfo from "@/components/search/SearchInfo";
import SearchSuggestions from "@/components/search/SearchSuggestions";
import { AutocompleteContext } from "@/context/autocompleteContext";
import { SearchContext } from "@/context/searchContext";
import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";
import { usePaginations } from "@/context/paginationsContext";
import { encodeSpace } from "@/utils/utils";

const placeholderTexts = ["Tomato", "Sodium", "Alzheimer"];

const SearchBar = () => {
  const router = useRouter();
  const pathname = usePathname();
  const [activeElement, setActiveElement] = useState<Element | null>(null);
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
  } = useContext(SearchContext);
  const { isLoading } = useSearchAutocompleteOptions();
  const { setTablePaginations, getTablePaginations } = usePaginations();
  const { autocompleteTerm } = useContext(AutocompleteContext);

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

  useEffect(() => {
    if (isFocused) return;

    let currentTextIndex = 0;
    let isDeleting = false;
    let placeholderText = "";
    let charIndex = 0;

    const updatePlaceholder = () => {
      const currentPlaceholder = placeholderTexts[currentTextIndex];

      if (isDeleting) {
        if (charIndex > 0) {
          placeholderText = currentPlaceholder.slice(0, charIndex - 1);
          charIndex--;
        } else {
          isDeleting = false;
          currentTextIndex = (currentTextIndex + 1) % placeholderTexts.length;
        }
      } else {
        if (charIndex < currentPlaceholder.length) {
          placeholderText = currentPlaceholder.slice(0, charIndex + 1);
          charIndex++;
        } else {
          isDeleting = true;
        }
      }

      setPlaceholder(placeholderText);
    };

    const interval = setInterval(updatePlaceholder, 200);
    return () => clearInterval(interval);
  }, [isFocused, setPlaceholder]);

  // @ts-ignore
  const handleInputChange = (event) => {
    event.preventDefault();
    setSearchTerm(event.target.value);
  };

  const handleSearchBarClear = () => setSearchTerm("");

  const handleSearchButtonClick = () => {
    setTablePaginations("results-page", 1, 20);
    router.push(`/results?term=${searchTerm}`);
  };

  // @ts-ignore
  const handleKeyDown = (event) => {
    if (event.key === "Enter") {
      event.currentTarget.blur();
      if (selectedSuggestion !== -1) {
        setIsVisible(false);
        router.push(
          `/${
            cachedSuggestions[selectedSuggestion].entity_type
          }/${encodeURIComponent(
            encodeSpace(cachedSuggestions[selectedSuggestion].common_name)
          )}`
        );
      } else {
        if (searchTerm.length > 0) {
          setOffsetTop(96);
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
      // @ts-ignore
      handleBlur();
    }
  };

  const handleFocus = () => {
    setSelectedSuggestion(-1);
  };

  const handleBlur = (e: React.MouseEvent<HTMLDivElement>) => {
    // @ts-ignore
    if (e.relatedTarget?.id !== "foodatlas-search") {
      setSelectedSuggestion(-1);
      // @ts-ignore
      setActiveElement(null);
      setTablePaginations("results-page", 1, 20);
      setIsFocused(false);
    }
  };

  // TODO: can this be moved to onfocus?
  useEffect(() => {
    if (activeElement === inputRef.current) {
      setIsFocused(true);
    } else {
      setIsFocused(false);
    }
  }, [setIsFocused, inputRef, activeElement]);

  useEffect(() => {
    setActiveElement(document.activeElement);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [document.activeElement]);

  const isResultsPage = pathname.startsWith("/results");

  return (
    isVisible && (
      <div role="search" aria-label="Site search">
        <div
          className={`z-10 w-full absolute ] px-3md:px-12 ${
            isFocused ? "absolute inset-0 top-24 -right-4" : ""
          } ${isResultsPage ? "" : "duration-[250ms]"}`}
          ref={containerRef}
          style={{ top: offsetTop || 50 }}
        >
          <div className="px-3 md:px-12">
            <div className="mx-auto max-w-6xl" id="search-component">
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
                    <div
                      className="z-50 cursor-pointer"
                      onClick={handleSearchBarClear}
                    >
                      <MdClose className="w-6 h-6 text-light-400 hover:text-light-300 transition duration-300 ease-in-out" />
                    </div>
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
                  tabIndex={1}
                  className={`pl-12 w-full h-12 rounded-lg border-[1.5px] border-light-600 bg-light-950/50 backdrop-blur-3xl saturate-150 hover:outline-white text-light-100 transition duration-100 ease-in-out outline-light-50/60 placeholder-light-500 ${
                    isFocused &&
                    cachedSuggestions.length > 0 &&
                    autocompleteTerm.length > 0
                      ? "rounded-b-none"
                      : ""
                  }`}
                  type="text"
                  value={searchTerm}
                  placeholder={placeholder}
                  onChange={handleInputChange}
                  onFocus={handleFocus}
                  // @ts-ignore
                  onBlur={handleBlur}
                  onKeyDown={handleKeyDown}
                />
              </div>
              {/* search info  */}
              {!isFocused && (
                <div className="mt-3">
                  <SearchInfo />
                </div>
              )}
              {/* search suggestions */}
              {isFocused && (
                <div>
                  <SearchSuggestions />
                </div>
              )}
            </div>
          </div>
        </div>
        <div
          className={`absolute inset-0 h-screen transition-all duration-300 backdrop-blur-md bg-black/30 saturate-150 pointer-events-none ${
            isFocused ? "opacity-100" : "opacity-0"
          }`}
          aria-hidden="true"
        />
      </div>
    )
  );
};

SearchBar.displayName = "SearchBar";

export default SearchBar;
