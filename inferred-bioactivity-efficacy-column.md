# Inferred bioactivity: swap "Top measurement" → "Efficacy at food concentration"

Context: on food pages, the "Inferred via composition" table currently shows a **Top measurement** column — the max-by-value endpoint reading for the chemical against the bioactivity (e.g. `IC50: 12.3 µM`). That's a chemical-level property; it ignores how much of the chemical the food actually contains. Tracker item ETA 7/17.

## Why the swap is the right move

The section's whole purpose is transitive inference: *food contains X → X does Y → food inherits some Y*. The magnitude of the inference depends on the food's concentration of X interacting with X's dose–response curve.

- **Top measurement** answers *"how potent is the pure chemical in the lab?"* — dose-agnostic, chemical-first.
- **Efficacy at food_conc** answers *"given how much X this food has, how much Y should we expect?"* — food-first, actually useful on a food page.

Sorting the column then reflects inferred-effect magnitude, which is what a food-page reader wants.

## What data we already have

`BioactivityMeasurementFull` (see `frontend/types/Bioactivity.ts:27–45`) already carries per-measurement Hill parameters from ToxCast tcpl:

- `efficacy_zeroactivity` (baseline y at x → 0)
- `efficacy_infiniteactivity` (E_max, y at x → ∞)
- `efficacy_logac50_value` (log10 AC50)
- `efficacy_hillslope` (n)
- `evidence_fit_r2` (fit quality)
- `evidence_fit_curveclass` (tcpl class, 1.x–5)

So we have curves, not single points. The math to evaluate `y(x)` is already there.

## Issues to resolve before shipping

### 1. Unit mismatch — food concentration doesn't plug directly into the curve

- Food conc is **mass/mass** (µg/g food, mg/100g).
- Hill curves are fit against **log10 molar concentration** (M) of pure compound in the assay well.

Bridging requires:

1. `mass → mol` via molecular weight — pure arithmetic, easy if MW is in the KG.
2. `mol/g_food → mol/L` in a biological system — an assumption about intake volume, distribution, bioavailability. Not a math problem; a biology assumption.

**Pragmatic path:** pick a fixed reference assumption (e.g. "1 g food dissolved in 1 L") and label the column as *relative expected efficacy at the food's inherent concentration*. Absolute number isn't a biological prediction, but ranking across the table is preserved — which is the point.

### 2. Endpoint / efficacy semantics — "top efficacy" isn't well-defined across mixed endpoints

Different endpoints live on different y-axes:

| Endpoint | Is it efficacy? |
|---|---|
| IC50 / EC50 / GI50 / AC50 / Ki | Potency (x-axis). Not efficacy. |
| % inhibition @ X µM | Efficacy, bounded 0–100. |
| % max response | Efficacy, bounded 0–100, referenced to a positive control. |
| Fold change | Efficacy, unbounded, scale varies per assay. |

Evaluating the Hill gives `y = e_zero + (e_inf − e_zero) · x^n / (AC50^n + x^n)`. Units of y are the units of `e_zero`/`e_inf` — could be % inhibition, % max, fold change. Taking "top" across those is comparing 50% inhibition to a fold-change of 2.3. Meaningless.

Options:

- **(a) Fractional response** — `(y − e_zero) / (e_inf − e_zero)`, unitless 0–1, cross-assay comparable. Flattens "50% of a strong response" and "50% of a weak response" to the same number.
- **(b) Bucket by endpoint family** — only take "top" within compatible endpoints; the row surfaces multiple values.
- **(c) ToxCast hit call at x** — is `food_conc` above AC50? yes/marginal/no. Avoids the y-unit problem.
- **(d) Flip it: `food_conc / AC50`** — how many multiples of the half-maximal dose the food's concentration reaches. Unitless, potency-anchored, cross-assay comparable, biologically legible. **Probably the cleanest answer.**

### 3. Fit quality — which curves are trustworthy enough to interpolate

`evidence_fit_curveclass` already exists — tcpl class:

- **1.x** — inactive, no real fit → efficacy interpolation meaningless.
- **2.x** — top point above cutoff but bad/single-point fit → marginal.
- **3.x** — good Hill fit → trustworthy.
- **4** — gain-loss (biphasic) → a single Hill misdescribes it.
- **5** — narcosis / cytotoxic burst → non-specific.

Plus `evidence_fit_r2` for continuous quality.

**Suggested rule:** include only `curveclass ∈ {3.1, 3.2, 3.3, 3.4}` **and** `r2 ≥ 0.7`. Everything else — don't interpolate, exclude from the "top efficacy" pick. If nothing on the row qualifies, cell shows `—` and the assay count/link still stands.

### 4. Number-of-points concern is moot

tcpl rejects fits with too few concentration points before assigning a curveclass at all. Any measurement that HAS a `curveclass` + `hillslope` already cleared that bar. `curveclass` + `r2` are the fit-quality signals ToxCast baked in. No separate n needed.

### 5. Null concentration → em-dash

If `median_concentration.value` is null for a row, the cell shows `—`. **Don't** fall back to "top measurement" — mixing two column semantics (efficacy for some rows, potency for others) is worse than an em-dash.

## Concrete ask for Pranav

If option **(d)** — `food_conc / AC50` in molar terms — is the direction, the backend/pipeline work is:

1. **mass → molar conversion.** For each `(chemical, food)` row, use the chemical's molecular weight + the fixed reference assumption to convert `median_concentration` into a molar value comparable to the Hill x-axis. Attach as `food_conc_molar` (or similar).
2. **AC50 in linear molar.** From `efficacy_logac50_value`, expose `ac50_molar = 10^logac50`.
3. **Filter to qualifying curves.** Keep only measurements with `curveclass ∈ {3.1, 3.2, 3.3, 3.4}` and `evidence_fit_r2 ≥ 0.7`.
4. **Per `(chemical, bioactivity)` row: max ratio.** Compute `food_conc_molar / ac50_molar` for every qualifying measurement, take the max, expose as `top_efficacy_ratio` alongside `n_qualifying_curves`.

Frontend column then reads e.g. `1.4× AC50 (n=3)`. If `n_qualifying_curves == 0` or `food_conc_molar` is null, cell shows `—`.

## Frontend work (blocked until the above lands)

Trivial once the data exists:

- Add `top_efficacy_ratio: number | null` and `n_qualifying_curves: number` to `InferredRow` in `frontend/components/entities/bioactivity/FoodInferredBioactivitiesSection.tsx`.
- Rename column header "Top measurement" → "Efficacy" (or "× AC50" — TBD).
- Swap the cell renderer from `formatTopMeasurement(top)` to `formatEfficacyRatio(row.top_efficacy_ratio, row.n_qualifying_curves)`.
- Update the sort key from `top_measurement_value` to `top_efficacy_ratio`.
- Mobile card row: same swap.

## Related

- Bioactivity endpoint/unit cleanup already in flight with Pranav — same "make bioactivity data biologically meaningful, not just extracted" theme.
- Tracker: `foodatlas-tracker-w29`, W29 pending item (ETA 7/17, slipped).
