# Staging update — bioactivity refresh + PTFI + new efficacy endpoint

**For:** the FoodAtlas frontend developer
**Environment:** bioactivity **staging** (the demo site) — production is untouched
**Date:** 2026-07-31
**Status:** ✅ live on staging and verified

---

## TL;DR

1. **Nothing breaks.** Same URL + key, and every existing endpoint/response shape is unchanged.
2. **One new endpoint:** `GET /food/efficacy?common_name=<food>`.
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

## 3. What changed in the existing data (no breaking API changes)

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

## 4. Heads-up

- Any **hardcoded counts / test fixtures** pinned to the old numbers will differ after the refresh.
- This is a **dev/staging** build. PTFI is currently ingested via an interim post-build delta merge;
  a production-grade PTFI **source adapter** is a planned follow-up — it will **not** change the API.
- The staging DB was reloaded during deploy (brief windows only); it's now stable.

## 5. Quick test

```bash
API=http://FoodAt-ApiSe-9NnFUsQyTUdH-1350162260.us-west-1.elb.amazonaws.com
KEY=<staging key>
curl -s -G "$API/food/efficacy" --data-urlencode "common_name=onion" \
  -H "Authorization: Bearer $KEY" | python3 -m json.tool | head -40
```
