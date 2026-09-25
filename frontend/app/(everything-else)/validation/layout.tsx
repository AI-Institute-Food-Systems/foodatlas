import type { Metadata } from "next";

// /validation is the internal curation tool: it sits behind a next-auth
// session and shows nothing useful to an anonymous visitor. It was listed in
// the sitemap and had no metadata at all, so it served no <title> — a
// guaranteed "missing title" flag in Search Console, on a page that should
// never have been indexed in the first place.
//
// noindex rather than a robots.txt Disallow on purpose: a disallowed page
// cannot be crawled, so Google never sees the noindex and can still list a
// bare URL it found elsewhere. Letting it crawl and read this is what
// actually keeps the page out.
export const metadata: Metadata = {
  title: "Validation",
  robots: { index: false, follow: false },
};

const ValidationLayout = ({ children }: { children: React.ReactNode }) => (
  <>{children}</>
);

export default ValidationLayout;
