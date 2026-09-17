// Bioactivity↔disease, attributed through the assay that actually bridges
// the two. Backed by:
//   GET /bioactivity/diseases?common_name=<bioactivity>
//
// The disease-side grains of this view (/disease/bioactivities and
// /disease/bioactivity-chemicals) no longer have a frontend consumer: the
// disease page listed the same (chemical, disease) pairs as
// /disease/chemical-associations, one row per activity instead of one per
// chemical, so the activity became a facet and a cell on that table.
//
// The attribution matters. Reaching a bioactivity the loose way — disease →
// chemical → every bioactivity that chemical was ever measured for — credits
// melanoma with 1,571 "antiviral" chemicals. Going through the bridging assay
// gives 3. This endpoint does the latter.
//
// Assay counts only, by design. An earlier revision also carried the food
// where each chemical's dietary dose came closest to its AC50; it was pulled
// because the AC50 is constant across foods within a (chemical, bioactivity)
// pair, making "best food" just "most concentrated food", with runners-up
// usually within noise. See the API repository docstring.

import type { AssayTarget } from "./AssayInferred";

// The direction split, reported as counts of chemicals rather than chips.
// At a grain that rolls up hundreds or thousands of chemicals, "does any of
// this involve therapeutic evidence?" is nearly always yes and says nothing;
// "271 of 2,038" says how much. A chemical classified both ways counts in
// both, so these do not sum to n_chemicals.
type DirectionCounts = {
  n_therapeutic: number;
  n_marker: number;
  // Chemicals whose link CTD literature also records, in any direction.
  n_literature: number;
};

// Flat rather than two-level — a bioactivity reaches at most 1,282 diseases,
// few enough to list without a chip-and-drilldown shape.

export type BioactivityDisease = DirectionCounts & {
  disease_name: string;
  disease_foodatlas_id: string;
  // Distinct chemicals linking this disease to the bioactivity.
  n_chemicals: number;
  n_assays: number;
  n_active_measurements: number;
  // The targets the most chemicals converge on, ranked by chemical count —
  // not the union, which over thousands of chemicals is a laundry list.
  target_genes: string[];
  targets: AssayTarget[];
};
