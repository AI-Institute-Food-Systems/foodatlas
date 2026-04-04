"""Tests for ambiguity tracking — attestations_ambiguous.parquet."""

from pathlib import Path

import pandas as pd
from src.pipeline.triplets.ambiguity import write_ambiguous_attestations
from src.stores.attestation_store import AttestationStore
from src.stores.schema import FILE_ATTESTATIONS, FILE_ATTESTATIONS_AMBIGUOUS


class TestWriteAmbiguousAttestations:
    def test_writes_ambiguous(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                {
                    "attestation_id": "at1",
                    "evidence_id": "ev1",
                    "source": "ie",
                    "head_name_raw": "fruit",
                    "tail_name_raw": "vitamin c",
                    "head_candidates": ["e0", "e99"],
                    "tail_candidates": ["e1"],
                },
                {
                    "attestation_id": "at2",
                    "evidence_id": "ev2",
                    "source": "fdc",
                    "head_name_raw": "apple",
                    "tail_name_raw": "vitamin c",
                    "head_candidates": ["e0"],
                    "tail_candidates": ["e1"],
                },
            ]
        )
        df.to_parquet(tmp_path / FILE_ATTESTATIONS, index=False)
        store = AttestationStore(path=tmp_path / FILE_ATTESTATIONS)
        write_ambiguous_attestations(store, tmp_path)

        out = tmp_path / FILE_ATTESTATIONS_AMBIGUOUS
        assert out.exists()
        result = pd.read_parquet(out)
        assert len(result) == 1
        assert result.iloc[0]["attestation_id"] == "at1"

    def test_empty_store(self, tmp_path: Path) -> None:
        pd.DataFrame().to_parquet(tmp_path / FILE_ATTESTATIONS)
        store = AttestationStore(path=tmp_path / FILE_ATTESTATIONS)
        write_ambiguous_attestations(store, tmp_path)
        assert not (tmp_path / FILE_ATTESTATIONS_AMBIGUOUS).exists()
