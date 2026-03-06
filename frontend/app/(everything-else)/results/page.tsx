"use client";

import { useContext, useEffect } from "react";
import { MdError, MdWarning } from "react-icons/md";

import ResultItem from "@/components/search/ResultItem";
import useSearchAutocompleteOptions from "@/hooks/useSearchAutocompleteOptions";
import { AutocompleteContext } from "@/context/autocompleteContext";
import Card from "@/components/basic/Card";
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
    setOffsetTop(96);
    setIsVisible(true);
  }, [setIsVisible, setOffsetTop]);

  useEffect(() => {
    setAutocompleteTerm(searchParams.term);
  }, [searchParams.term, setAutocompleteTerm]);

  return (
    <div className="mt-52">
      {/* error indicator */}
      {isError ? (
        <div className="w-full mt-32 flex justify-center gap-1.5 items-center">
          <MdError />
          <span>Error fetching data for &apos;{searchParams.term}&apos;</span>
        </div>
      ) : // loading indicator
      isLoading ? (
        <div className="flex flex-col gap-3 mt-6">
          <div className="w-52 h-6 bg-zinc-900/80 animate-pulse shadow-light-700/20 border border-light-50/10 rounded " />
          <div className="mt-3 flex flex-col gap-3">
            {Array.from({ length: 7 }, (_, index) => (
              <Card key={index}>
                <div className="h-[7.4rem] animate-pulse" />
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
