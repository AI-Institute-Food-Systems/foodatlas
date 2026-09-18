// Shared shaping for the search_* umami events, so the dropdown pick, the
// keyboard pick and the no-results beacon all report the same query string.

// The only events that carry user-typed text. Lowercased and capped so the
// Properties breakdown groups "Tomato" with "tomato" and a pasted paragraph
// can't become a 2 kB event.
export const QUERY_MAX_LEN = 100;

// How long an empty result has to hold still before it counts as a miss.
export const NO_RESULTS_SETTLE_MS = 1000;

export const normaliseQuery = (term: string): string =>
  term.trim().toLowerCase().slice(0, QUERY_MAX_LEN);
