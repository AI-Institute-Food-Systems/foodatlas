// The composition sources, named once.
//
// Frontend mirror of the backend's repositories/_sources.py. Both the
// table's Source filter and the data-points modal need to talk about the
// same three sources, and the section imports the modal — so the list
// can't live in either of them without one importing the other.
//
// Deliberately NOT utils' SOURCE_LOOKUP: that maps *raw attestation*
// sources ("lit2kg:gpt-4" → "FoodAtlas") and has no "foodatlas" key, so
// looking a filter value up in it yields undefined rather than a label.

export const SOURCE_OPTIONS = [
  { value: "fdc", label: "FDC" },
  { value: "foodatlas", label: "FoodAtlas" },
  { value: "ptfi", label: "PTFI" },
];

// Every source starts selected. Derived rather than hardcoded — the
// initial state and the reset handler both used to list sources
// literally, so adding PTFI silently left it unselected on load and
// un-selected it again on reset.
export const ALL_SOURCE_VALUES = SOURCE_OPTIONS.map((o) => o.value);

// Filter key → the display casing the API uses in `reference.source_name`.
export const SOURCE_DISPLAY_NAMES: Record<string, string> = Object.fromEntries(
  SOURCE_OPTIONS.map((o) => [o.value, o.label])
);
