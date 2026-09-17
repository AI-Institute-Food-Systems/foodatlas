// A bibliographic reference to a FoodAtlas paper. Stored field-by-field
// rather than as a pre-formatted string: the three copies of the npj
// reference that this replaced had all been typed while the paper was in
// press, so all three were missing its volume and article number and none
// of them noticed. Fields make the locator's absence visible.
export type Publication = {
  // APA author string, every author listed. APA 7 only elides at 21+.
  authors: string;
  year: number;
  // APA sentence case — first word after a colon capitalised.
  title: string;
  // Journal or workshop name. Rendered italic.
  venue: string;
  // "proceedings" prefixes the venue with "In ".
  kind: "journal" | "proceedings";
  volume?: string;
  issue?: string;
  articleNumber?: string;
  // Bare DOI, no https://doi.org/ prefix — see doiUrl() in utils/publications.
  doi: string;
};
