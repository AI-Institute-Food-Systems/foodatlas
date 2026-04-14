export type SearchOptions = {
  search_terms: string[];
  column: "both" | "head" | "tail";
  match_substring: boolean;
  match_all_synonyms: boolean;
  type: "and" | "or";
};
