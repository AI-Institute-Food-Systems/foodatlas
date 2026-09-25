import type { Metadata } from "next";

// /results is the search-results page. It must be crawlable, because the
// WebSite SearchAction in utils/structuredData.ts targets it and Google only
// honours a sitelinks-searchbox target it can fetch — it was previously
// disallowed in robots.txt, so the markup could never take effect.
//
// Crawlable is not the same as indexable: a thin, query-driven results page
// has nothing to rank for, so it carries noindex while staying followable.
export const metadata: Metadata = {
  title: "Search results",
  robots: { index: false, follow: true },
};

const ResultsLayout = ({ children }: { children: React.ReactNode }) => (
  <>{children}</>
);

export default ResultsLayout;
