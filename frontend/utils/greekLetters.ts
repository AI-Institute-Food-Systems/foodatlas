/**
 * Greek-letter ↔ Latin-name mapping for highlight matching.
 *
 * The KGC pipeline standardizes Greek letters to their Latin names
 * (e.g. α → alpha) in attestation chemical names. The original
 * evidence sentences still use Unicode Greek characters, so we need
 * to generate alternate name variants for regex matching.
 */

const GREEK_LETTER_MAP: readonly [string, string][] = [
  ["alpha", "α"],
  ["beta", "β"],
  ["gamma", "γ"],
  ["delta", "δ"],
  ["epsilon", "ε"],
  ["omega", "ω"],
];

/**
 * Escape special regex characters in a string.
 */
function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Return all Greek-letter variants of a name.
 *
 * For a name like "alpha-tocopherol", this returns
 * ["alpha\\-tocopherol", "α\\-tocopherol"]. All returned strings
 * are regex-escaped so they can be safely interpolated into a
 * RegExp pattern.
 *
 * Returns an empty array for null/undefined/empty input.
 */
export function greekVariants(name: string | null | undefined): string[] {
  if (!name) return [];

  const escaped = escapeRegex(name);
  const variants = new Set<string>([escaped]);

  for (const [latin, greek] of GREEK_LETTER_MAP) {
    // Latin → Greek variant (case-insensitive replacement)
    const latinPattern = new RegExp(escapeRegex(latin), "gi");
    if (latinPattern.test(name)) {
      const greekName = name.replace(latinPattern, greek);
      variants.add(escapeRegex(greekName));
    }

    // Greek → Latin variant
    const greekPattern = new RegExp(escapeRegex(greek), "g");
    if (greekPattern.test(name)) {
      const latinName = name.replace(greekPattern, latin);
      variants.add(escapeRegex(latinName));
    }
  }

  return Array.from(variants);
}

/**
 * Check whether a sentence part matches an extraction name,
 * accounting for Greek-letter variants.
 *
 * Returns true if `part` case-insensitively equals `name` or any
 * of its Greek/Latin variants.
 */
export function matchesWithGreek(
  part: string | null | undefined,
  name: string | null | undefined
): boolean {
  if (!part || !name) return false;

  const lower = part.toLowerCase();
  if (lower === name.toLowerCase()) return true;

  for (const [latin, greek] of GREEK_LETTER_MAP) {
    const latinPattern = new RegExp(escapeRegex(latin), "gi");
    const greekPattern = new RegExp(escapeRegex(greek), "g");

    const withGreek = name.replace(latinPattern, greek).toLowerCase();
    if (lower === withGreek) return true;

    const withLatin = name.replace(greekPattern, latin).toLowerCase();
    if (lower === withLatin) return true;
  }

  return false;
}
