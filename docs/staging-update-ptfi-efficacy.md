# Staging update — bioactivity refresh + PTFI + new efficacy & disease-association endpoints

**For:** the FoodAtlas frontend developer
**Environment:** bioactivity **staging** (the demo site) — production is untouched
**Date:** 2026-07-31
**Status:** ✅ live on staging and verified

---

## TL;DR

1. **Response shapes are unchanged; same URL + key.** One behavior change: `/bioactivity/chemicals` + `/bioactivity/foods` are now **paginated** (default 50/page — see §4).
2. **New endpoints:** `GET /food/efficacy`, plus `GET /chemical/disease-associations` and `GET /disease/chemical-associations` (bioactivity-inferred).
3. **Data changed (additively):** bioactivity data refreshed, and **PTFI** foods/chemicals/edges added.

---

## 1. Connecting — unchanged

- **API URL:** `http://FoodAt-ApiSe-9NnFUsQyTUdH-1350162260.us-west-1.elb.amazonaws.com`
- **Auth:** same `Authorization: Bearer <key>` (staging key, unchanged).
- No `.env` / config changes needed — just refresh.

## 2. New endpoint — `GET /food/efficacy?common_name=<food>`

Per food, returns one row per **chemical × bioactivity**: does the food's dietary concentration
of a chemical clear that chemical's active threshold for a bioactivity. Same envelope as the
other endpoints (`{ data, metadata: { row_count } }`), ordered by `efficacy_response` **desc**,
behind the same Bearer key.

**Real response row** (`/food/efficacy?common_name=onion` → `row_count: 181`; first row):

```json
{
  "food_name": "onion", "food_foodatlas_id": "e9305",
  "chemical_name": "luteolin", "chemical_foodatlas_id": "e60182", "cid": 5280445,
  "bioactivity_name": "antiviral", "bioactivity_foodatlas_id": "e227396",
  "bioactivity_id_raw": "E300016",
  "food_conc_mg_per_100g": 39.19, "food_conc_mass_fraction_pct": 0.03919,
  "conc_quality_flag": "ok", "molecular_weight": 286.24,
  "food_conc_m": 0.001369, "food_conc_logm": -2.863555,
  "rep_source_assay_id": "AID: 1347158",
  "endpoint_type": "Potency", "endpoint_class": "potency", "curve_method": "4-point",
  "logac50": -4.85, "hillslope": 2.0437, "zeroactivity": 6.4741, "infiniteactivity": 455.5964,
  "n_curves": 53, "n_curves_4param": 17, "curve_agreement": "wide",
  "ac50_spread_log": 5.4173, "logac50_median": -4.924, "logac50_min": -9.1173, "logac50_max": -3.7,
  "dose_over_ac50_log": 1.986445, "conc_vs_ac50": "above",
  "efficacy_fraction": 0.999913, "efficacy_response": 455.557259, "saturated": true
}
```

Field notes for the UI:
- **Key display fields:** `chemical_name`, `bioactivity_name`, `efficacy_response`, `conc_vs_ac50`
  (`above`/`below`), `food_conc_mg_per_100g`, `logac50`.
- `bioactivity_id_raw` may be the literal **`"UNCLASSIFIED"`** — in that case `bioactivity_name`
  and `bioactivity_foodatlas_id` are **empty strings**. Handle that (e.g. label "unclassified").
- Numeric fields can be `null` (missing curve params). `saturated` is a bool.
- **Not every food has rows** — e.g. `onion` → 181, `enoki mushroom` → 46, `garlic` → 0.
  Treat `row_count: 0` as "no efficacy data for this food," not an error.
- ~864 foods and ~709 chemicals are covered (61,119 rows total).

## 3. New endpoints — chemical↔disease associations (bioactivity-inferred)

`GET /chemical/disease-associations?common_name=<chemical>` and
`GET /disease/chemical-associations?common_name=<disease>` return chemical↔disease
associations **inferred from shared bioactivity assays**: a chemical is associated with a
disease when it has ≥1 *Active* measurement in an assay the bioactivity disease-bridge ties
to that disease (through the disease's target genes / mechanism).

> **Not the same as `/chemical/correlation`.** That existing endpoint serves CTD *literature*
> correlations. These are a *different* association from a *different* method (lab-assay
> inference) — extra mechanism/target evidence, not a replacement. ~347k associations across
> ~7.6k chemicals / ~1.6k diseases.

Standard `{ data, metadata: { row_count } }` envelope, ordered by `n_assays` desc. Real row
(`/chemical/disease-associations?common_name=vorinostat` → 181 rows):

```json
{
  "chemical_name": "vorinostat", "chemical_foodatlas_id": "e20808",
  "disease_name": "carcinoma, squamous cell", "disease_foodatlas_id": "e204503",
  "n_assays": 837, "n_active_measurements": 837,
  "relationships": ["marker/mechanism"],
  "target_genes": ["NCBIGene: 1956", "NCBIGene: 3065", "NCBIGene: 3066", "..."],
  "assays": ["AID: 1055355", "AID: 1061954", "..."]
}
```

- `n_assays` / `n_active_measurements` — distinct shared assays / active measurements backing
  the link (a strength signal; results are ordered by these).
- `relationships` — the bridge relationship type(s), e.g. `marker/mechanism`.
- `target_genes` — the assay target genes (`NCBIGene:` / `UniProt:` ids), capped at 50;
  `assays` capped at 25.
- `/disease/chemical-associations` returns the same associations keyed the other way
  (e.g. `melanoma` → 1,931 chemicals: vorinostat, celecoxib, rosiglitazone, …).

## 4. Bioactivity list endpoints — now paginated (+ stats fix)

Two updates to *existing* bioactivity endpoints:

**Pagination (behavior change).** `GET /bioactivity/chemicals` and `GET /bioactivity/foods` are
now **paginated**. Large bioactivities (e.g. anticancer ≈ 31k chemicals, each row carrying its
`measurements`) previously produced 50–100 MB responses that timed out (**502**). They now accept
`page` (default 1) and `limit` (default 50, max 500) and return `total_rows` + `total_pages`:

```
GET /bioactivity/chemicals?common_name=anticancer&page=1&limit=50
```
```json
{ "data": [ /* ≤ limit chemical rows — same shape as before (name, id, *_count, measurements) */ ],
  "metadata": { "row_count": 50, "page": 1, "rows_per_page": 50,
                "total_rows": 31792, "total_pages": 636 } }
```
Row shape and ordering (`active_count` desc) are unchanged. **Action:** send `page`/`limit` and
page through `total_pages` — the default now returns 50 rows, not the whole list.
(`/chemical/bioactivities` and `/food/bioactivities` — one chemical's/food's bioactivities — are
small and **not** paginated.)

**`/metadata/statistics` now includes bioactivities.** It previously dropped them; it now returns
`bioactivities` (21) and `bioactivity_measurements` (1,557,037) alongside foods/chemicals/etc.

> **Data note:** `/bioactivity/foods` only has rows for **antioxidant** and **antidiabetic** — the
> food→bioactivity model only predicted those two, so 0 foods for other bioactivities (e.g.
> anticancer) is correct data, not an error. Chemicals cover all 21.

## 5. What changed in the existing data (no breaking API changes)

- **Bioactivity data refreshed** to the newer measurement set (Jul 2026). Counts/values differ
  from what you saw before. **Concept IDs are stable** (e.g. antioxidant = `e227382`,
  antiviral = `e227396`), so any IDs you've referenced still resolve.
- **PTFI data added** — ~1,065 new entities (300→ deduped to 43 new foods + 1,022 new chemicals)
  and ~11k new food→chemical `contains` edges (relative-abundance concentrations).
  - New PTFI foods now appear in food lookups/search — e.g. `GET /food/metadata?common_name=bee pollen`
    → `e227402`.
  - PTFI foods that matched an **existing** FoodOn food were **merged into it** (no duplicate foods).
  - PTFI concentrations use `conc_unit = "relative_abundance"` and `source = "ptfi"` — the mg/100g
    composition medians are **not** affected by them.
- **No existing endpoint or response shape changed.** The efficacy endpoint is purely additive.

## 6. Heads-up

- Any **hardcoded counts / test fixtures** pinned to the old numbers will differ after the refresh.
- This is a **dev/staging** build. PTFI is currently ingested via an interim post-build delta merge;
  a production-grade PTFI **source adapter** is a planned follow-up — it will **not** change the API.
- The staging DB was reloaded during deploy (brief windows only); it's now stable.

## 7. Quick test

```bash
API=http://FoodAt-ApiSe-9NnFUsQyTUdH-1350162260.us-west-1.elb.amazonaws.com
KEY=<staging key>
curl -s -G "$API/food/efficacy" --data-urlencode "common_name=onion" \
  -H "Authorization: Bearer $KEY" | python3 -m json.tool | head -40
```
