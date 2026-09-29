// Chemical → food deep links carry the chemical to highlight in the fragment,
// not the query. A `?highlight=` link is its own crawlable URL — one per
// food-chemical pair, ~200k of them — so robots.txt has to block it, and that
// also blocked the only server-rendered links into food pages. A fragment
// never reaches the server: the link Google follows is the bare food URL the
// sitemap lists. `?highlight=` is still read for links already out there.
const PREFIX = "#highlight=";

export const foodHighlightHref = (foodPath: string, chemicalId: string) =>
  `${foodPath}${PREFIX}${encodeURIComponent(chemicalId)}`;

export const readHighlightHash = (hash: string): string => {
  if (!hash.startsWith(PREFIX)) return "";
  try {
    return decodeURIComponent(hash.slice(PREFIX.length));
  } catch {
    // A hand-edited fragment with a stray "%" — no highlight, not a crash.
    return "";
  }
};
