"use client";

import { useContext, useEffect } from "react";
import { MdError, MdWarning } from "react-icons/md";

import ResultItem from "@/components/search/ResultItem";
import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";
import { AutocompleteContext } from "@/context/autocompleteContext";
import Card from "@/components/basic/Card";
import Skeleton from "@/components/basic/Skeleton";
import Pagination from "@/components/basic/Pagination";
import { usePaginations } from "@/context/paginationsContext";
import { SearchContext } from "@/context/searchContext";
import { Suggestion } from "@/types";

interface SearchParams {
  query: string;
  term: string;
}

const ResultsPage = ({ searchParams }: { searchParams: SearchParams }) => {
  const {
    suggestions,
    currentPage,
    totalRows,
    totalPages,
    isLoading,
    isError,
    errorMessage,
  } = useSearchAutocompleteOptions();
  const { setOffsetTop, setIsVisible } = useContext(SearchContext);
  const { setAutocompleteTerm } = useContext(AutocompleteContext);
  const { setTablePaginations, getTablePaginations } = usePaginations();

  useEffect(() => {
    // Anchor with a half-navbar gap under the navbar bottom —
    // 48 + 24 = 72 mobile / 56 + 28 = 84 md+. Matches the focused
    // fly-up so nothing moves on focus.
    setOffsetTop(
      typeof window !== "undefined" &&
        window.matchMedia("(min-width: 768px)").matches
        ? 84
        : 72,
    );
    setIsVisible(true);
  }, [setIsVisible, setOffsetTop]);

  useEffect(() => {
    setAutocompleteTerm(searchParams.term);
  }, [searchParams.term, setAutocompleteTerm]);

  return (
    // Reserves vertical space under the portaled SearchBar (anchored
    // at ~72 mobile / ~84 md + h-12 input = ~120/132). mt-36 gives
    // ~24px breathing on phones; md:mt-44 matches on desktop.
    <div className="mt-36 md:mt-44">
      {/* error indicator */}
      {isError ? (
        <div className="w-full mt-32 flex justify-center gap-1.5 items-center">
          <MdError />
          <span>Error fetching data for &apos;{searchParams.term}&apos;</span>
        </div>
      ) : // loading indicator
      isLoading ? (
        <div className="flex flex-col gap-3 mt-6">
          <div className="flex justify-between">
            <Skeleton className="mt-8 h-5 w-56" />
            <Skeleton className="mt-8 h-5 w-24" />
          </div>
          <div className="mt-3 flex flex-col gap-3">
            {/* Each card previews ResultItem's structure: header row
             * (name + id chip on the right), meta row, and a synonym
             * strip. Reserves the same vertical footprint so the page
             * doesn't jump when results land. */}
            {Array.from({ length: 7 }, (_, index) => (
              <Card key={index}>
                <div className="flex flex-col gap-2">
                  <div className="flex justify-between items-center">
                    <Skeleton className="h-5 w-1/3" />
                    <Skeleton className="h-4 w-24" />
                  </div>
                  <Skeleton className="h-3 w-1/2" />
                  <Skeleton className="h-3 w-2/3 mt-1" />
                </div>
              </Card>
            ))}
          </div>
        </div>
      ) : // results container
      suggestions && suggestions.length > 0 ? (
        <div>
          {/* results & page indicator */}
          <div className="flex justify-between">
            {/* # results indicator */}
            <div className="mt-8 text-light-300">{`${totalRows} results for "${searchParams.term}"`}</div>
            {/* pagination */}
            <div className="mt-8 text-light-300">{`Page ${currentPage} of ${totalPages}`}</div>
          </div>
          <div className="mt-3 flex flex-col gap-3">
            {suggestions?.map((suggestion: Suggestion) => (
              <ResultItem
                key={suggestion.foodatlas_id}
                suggestion={suggestion}
              />
            ))}
          </div>
          <div className="max-w-3xl mx-auto mt-10">
            <Pagination
              tableId={"results-page"}
              numberOfPages={totalPages}
              isLoading={isLoading}
            />
          </div>
        </div>
      ) : (
        // no results container
        <div className="w-full mt-32 flex justify-center gap-1.5 items-center">
          <MdWarning />
          <span>No matches found for &apos;{searchParams.term}&apos;</span>
        </div>
      )}
    </div>
  );
};

export default ResultsPage;
