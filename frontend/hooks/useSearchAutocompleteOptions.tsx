import { useContext } from "react";
import useSWR from "swr";

import { AutocompleteContext } from "@/context/autocompleteContext";
import { usePaginations } from "@/context/paginationsContext";

const fetcher = async (url: string) => {
  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
    },
  });
  const json = await response.json();
  return json;
};

const useSearchAutocompleteOptions = () => {
  const { autocompleteTerm } = useContext(AutocompleteContext);
  const { getTablePaginations } = usePaginations();
  const { currentPage } = getTablePaginations("results-page");

  // base url
  const baseUrl = `${process.env.NEXT_PUBLIC_API_URL}/metadata/search?`;

  // full url
  const url =
    baseUrl +
    "term=" +
    encodeURIComponent(autocompleteTerm) +
    "&page=" +
    currentPage;

  const { data, error, isLoading } = useSWR(
    autocompleteTerm.length > 0 ? url : null,
    fetcher,
    {
      revalidateOnFocus: false,
      dedupingInterval: 2147483647, // set to max value (~24d) so we don't refetch ever unless site is reloaded
    }
  );

  return {
    suggestions: data && data.data,
    totalRows: data && data?.metadata?.total_rows,
    currentPage: data && data.metadata.current_page,
    totalPages: data && data.metadata.total_pages,
    isLoading: isLoading,
    isError: error || data?.message === "Service Unavailable",
    errorMessage: data?.message === "Service Unavailable" ? data?.message : "",
  };
};

export default useSearchAutocompleteOptions;
