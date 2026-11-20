"""Per-pair assay groups for the Health tab's assay-inferred table.

The page filters that table by Signal (bridge relationship) and Activity (what
an assay measures). Both are properties of single assays, so a pair-level
union cannot say how many of a pair's assays match a filter. Each pair gets
its assays grouped by their exact (activities, relationships) combination;
every assay lands in one group, so summing the matching groups' ``n_assays``
is an exact filtered count. Each group also keeps a capped assay sample and
its target genes for the modals.
"""

import pandas as pd

from .materializer_bioactivity_bridge import ASSAY_CAP, GENE_CAP

KEYS = ["chemical_id", "disease_id"]
_ASSAY_KEYS = [*KEYS, "source_assay_id"]
_BDM = "bioactivity_disease_metadata_id"


def _as_tuple(value: object) -> tuple:
    return value if isinstance(value, tuple) else ()


def _per_assay(frame: pd.DataFrame, col: str, name: str) -> pd.Series:
    """Distinct ``col`` values per (chemical, disease, assay), as a sorted tuple."""
    flat = frame[[*_ASSAY_KEYS, col]].explode(col).dropna(subset=[col])
    return (
        flat.groupby(_ASSAY_KEYS)[col].agg(lambda s: tuple(sorted(set(s)))).rename(name)
    )


def assay_groups(
    evidence: pd.DataFrame,
    target_map: dict,
    assay_bio: pd.DataFrame,
    name_map: dict,
) -> pd.Series:
    """(chemical_id, disease_id) → list of assay-group dicts."""
    genes = evidence[[*_ASSAY_KEYS, _BDM]].explode(_BDM).dropna(subset=[_BDM])
    genes["genes"] = genes[_BDM].map(target_map)
    activities = (
        assay_bio.assign(name=assay_bio["bioactivity_id"].map(name_map))
        .dropna(subset=["name"])
        .groupby("source_assay_id")["name"]
        .agg(lambda s: tuple(sorted(set(s))))
    )
    per_assay = (
        evidence[_ASSAY_KEYS]
        .drop_duplicates()
        .set_index(_ASSAY_KEYS)
        .join(_per_assay(evidence, "relationship", "relationships"))
        .join(_per_assay(genes, "genes", "genes"))
        .reset_index()
    )
    per_assay["activities"] = per_assay["source_assay_id"].map(activities)
    for col in ("activities", "relationships", "genes"):
        per_assay[col] = per_assay[col].apply(_as_tuple)

    grouped = (
        per_assay.groupby([*KEYS, "activities", "relationships"])
        .agg(
            n_assays=("source_assay_id", "nunique"),
            assays=("source_assay_id", lambda s: sorted(set(s))[:ASSAY_CAP]),
            target_genes=(
                "genes",
                lambda s: sorted({g for t in s for g in t})[:GENE_CAP],
            ),
        )
        .reset_index()
    )
    grouped["group"] = [
        {
            "activities": list(acts),
            "relationships": list(rels),
            "n_assays": int(n),
            "assays": list(assays),
            "target_genes": list(tg),
        }
        for acts, rels, n, assays, tg in zip(
            grouped["activities"],
            grouped["relationships"],
            grouped["n_assays"],
            grouped["assays"],
            grouped["target_genes"],
            strict=True,
        )
    ]
    return grouped.groupby(KEYS)["group"].agg(list).rename("assay_groups")
