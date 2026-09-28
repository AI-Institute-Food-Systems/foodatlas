// CorrelationTable's opening sort: most publications first. Shared so the
// server's first-page fetch (disease page, tab badge) requests exactly the
// URL the table would. Not in utils/fetching, which tests mock wholesale.
export const CORRELATION_DEFAULT_SORT = {
  by: "evidence_count",
  dir: "desc",
} as const;
