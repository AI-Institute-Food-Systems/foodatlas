"use client";

import { Analytics } from "@vercel/analytics/react";

import { PaginationsProvider } from "@/context/paginationsContext";
import { AutocompleteProvider } from "@/context/autocompleteContext";
import { SearchProvider } from "@/context/searchContext";

interface ProvidersProps {
  children: React.ReactNode;
}

const Providers = ({ children }: ProvidersProps) => {
  return (
    <PaginationsProvider>
      <AutocompleteProvider>
        <SearchProvider>
          <Analytics />
          {children}
        </SearchProvider>
      </AutocompleteProvider>
    </PaginationsProvider>
  );
};

export default Providers;

Providers.displayName = "Providers";
