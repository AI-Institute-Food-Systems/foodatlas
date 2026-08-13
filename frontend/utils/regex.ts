/**
 * Escape a string for safe interpolation into a RegExp.
 *
 * Anything a user can type may end up inside a pattern, and entity names in
 * this dataset are full of regex metacharacters — `linoleic acid(d4)`,
 * `5-(1,3-benzodioxol-5-yl)-1-(1-piperidinyl)-1-penta-2,4-dienone`. Typing a
 * bare `(` used to throw "Invalid regular expression: missing )" and take the
 * page down with it.
 */
export function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
