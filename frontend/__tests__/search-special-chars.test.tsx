// Regression tests for the search bar crashing on regex metacharacters.
//
// Typing "(" threw "Invalid regular expression: missing )" and took the page
// down. Entity names in this dataset are full of these characters —
// linoleic acid(d4), 5-(1,3-benzodioxol-5-yl)-… — so they are exactly what a
// user types when searching by name.

import { describe, expect, it } from "vitest";

import { escapeRegExp } from "@/utils/regex";

// Mirrors ResultItem's highlightMatch: split on a single capture group and
// take the odd indices as matches. Kept in sync deliberately — the component
// itself returns JSX, which this asserts on as plain segments.
const segments = (text: string, searchTerm: string) => {
  if (!searchTerm) return [text];
  const regex = new RegExp(`(${escapeRegExp(searchTerm)})`, "gi");
  return text.split(regex);
};

const highlighted = (text: string, searchTerm: string) =>
  segments(text, searchTerm).filter((_, i) => i % 2 === 1);

describe("escapeRegExp", () => {
  it("escapes every regex metacharacter", () => {
    for (const ch of [".", "*", "+", "?", "^", "$", "{", "}", "(", ")", "|", "[", "]", "\\"]) {
      expect(() => new RegExp(escapeRegExp(ch))).not.toThrow();
    }
  });

  it("leaves ordinary text untouched", () => {
    expect(escapeRegExp("tomato")).toBe("tomato");
  });

  it("matches the literal character, not its regex meaning", () => {
    // Unescaped, "." would match any character.
    expect(new RegExp(escapeRegExp(".")).test("a")).toBe(false);
    expect(new RegExp(escapeRegExp(".")).test(".")).toBe(true);
  });
});

describe("search highlighting with special characters", () => {
  it.each([
    ["(", "linoleic acid(d4)"],
    [")", "linoleic acid(d4)"],
    ["(d4)", "linoleic acid(d4)"],
    ["[", "chemical [x]"],
    ["+", "vitamin b+"],
    ["*", "star*anise"],
    ["?", "what?"],
    ["\\", "back\\slash"],
    ["$", "cost$"],
    ["|", "a|b"],
  ])("does not throw on %j", (term, text) => {
    expect(() => segments(text, term)).not.toThrow();
  });

  it("highlights a parenthesised chemical name", () => {
    expect(highlighted("linoleic acid(d4)", "acid(d4)")).toEqual(["acid(d4)"]);
  });

  it("is case-insensitive", () => {
    expect(highlighted("Cavendish Banana", "banana")).toEqual(["Banana"]);
  });

  it("highlights every occurrence", () => {
    // Pins the behaviour the index-parity rewrite has to preserve. The old
    // regex.test() version passed this too — its /g lastIndex was reset by the
    // failing separator tests in between — but nothing guaranteed that.
    expect(highlighted("onion onion onion", "onion")).toEqual([
      "onion",
      "onion",
      "onion",
    ]);
  });

  it("returns the text unchanged for an empty term", () => {
    expect(segments("tomato", "")).toEqual(["tomato"]);
  });

  it("treats the term as a literal, not a pattern", () => {
    // "." must not behave as "any character" — searching it should highlight
    // nothing in a string that has no literal dot.
    expect(highlighted("tomato", ".")).toEqual([]);
  });
});
