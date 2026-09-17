"use client";

import { Analytics } from "@vercel/analytics/react";

import ReportFab from "@/components/basic/ReportFab";
import { PaginationsProvider } from "@/context/paginationsContext";
import { AutocompleteProvider } from "@/context/autocompleteContext";
import { NavigationProvider } from "@/context/navigationContext";
import { ReportModeProvider } from "@/context/reportModeContext";
import { SearchProvider } from "@/context/searchContext";

interface ProvidersProps {
  children: React.ReactNode;
}

const Providers = ({ children }: ProvidersProps) => {
  return (
    <NavigationProvider>
      <PaginationsProvider>
        <AutocompleteProvider>
          <SearchProvider>
            <ReportModeProvider>
              <Analytics />
              {children}
              {/* Global "Report an issue" floating trigger + modal.
               * Every table's rows opt in via useReportRows(). */}
              <ReportFab />
            </ReportModeProvider>
          </SearchProvider>
        </AutocompleteProvider>
      </PaginationsProvider>
    </NavigationProvider>
  );
};

export default Providers;

Providers.displayName = "Providers";
