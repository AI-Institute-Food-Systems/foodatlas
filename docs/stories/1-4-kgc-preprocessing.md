# Story 1-4: Preprocessing — Name and Concentration Standardization

## Goal

Port the preprocessing modules that standardize chemical names, concentrations, and food parts. Split the oversized concentration file into parser + converter.

## Depends On

- Story 1-1 (preprocessing/constants already ported)

## Acceptance Criteria

- [ ] `preprocessing/chemical_name.py` — ported from `_standardize_chemical_name.py`
  - Greek letter normalization, punctuation standardization
  - Uses constants from `preprocessing/constants/`
- [ ] `preprocessing/chemical_conc.py` — orchestrator (calls parser + converter)
- [ ] `preprocessing/conc_parser.py` — regex patterns + `parse_conc_string` extracted from `_standardize_chemical_conc.py`
- [ ] `preprocessing/conc_converter.py` — unit conversion logic extracted from `_standardize_chemical_conc.py`
- [ ] `preprocessing/food_part.py` — ported from `_standardize_food_part.py`
- [ ] Original `_standardize_chemical_conc.py` (305 lines) is split so each new file is under 300 lines
- [ ] All files pass `ruff check` and `mypy`
- [ ] Tests for:
  - Chemical name standardization (Greek letters, punctuation, case)
  - Concentration parsing (ranges "1-2 mg", approximations "~50 ug", units "ppm")
  - Unit conversion
  - Food part standardization
  - Edge cases from the original codebase

## Source Files

| Target | Source |
|--------|--------|
| `preprocessing/chemical_name.py` | `FoodAtlas-KGv2/food_atlas/kg/preprocessing/_standardize_chemical_name.py` |
| `preprocessing/conc_parser.py` | `FoodAtlas-KGv2/food_atlas/kg/preprocessing/_standardize_chemical_conc.py` (parse logic) |
| `preprocessing/conc_converter.py` | `FoodAtlas-KGv2/food_atlas/kg/preprocessing/_standardize_chemical_conc.py` (conversion logic) |
| `preprocessing/chemical_conc.py` | `FoodAtlas-KGv2/food_atlas/kg/preprocessing/_standardize_chemical_conc.py` (top-level orchestrator) |
| `preprocessing/food_part.py` | `FoodAtlas-KGv2/food_atlas/kg/preprocessing/_standardize_food_part.py` |

## Notes

- These are pure functions with no external dependencies (no DB, no API calls) — easy to test thoroughly.
- The preprocessing constants were already ported in Story 1-1.
- Input format is JSON preferred, pkl supported for backward compatibility. See `examples/inputs/` for sample files. Each record has `triplets: [[food, food_part, chemical, conc], ...]`.
- Can verify correctness by comparing preprocessing output against `examples/kg/_metadata_new.tsv` (the processed metadata).
