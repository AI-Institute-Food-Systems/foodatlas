import type { Metadata } from "next";

// /results is the search-results page. A thin, query-driven results page has
// nothing to rank for, so it carries noindex while staying followable. It
// stays crawlable (not disallowed in robots.txt), or Google could never read
// the noindex and might still list the bare URL.
export const metadata: Metadata = {
  title: "Search results",
  robots: { index: false, follow: true },
};

const ResultsLayout = ({ children }: { children: React.ReactNode }) => (
  <>{children}</>
);

export default ResultsLayout;
