"""Tests for ambiguity tracking — extractions_ambiguous.parquet."""

from pathlib import Path

import pandas as pd
from src.pipeline.triplets.ambiguity import write_ambiguous_extractions
from src.stores.extraction_store import ExtractionStore
from src.stores.schema import FILE_EXTRACTIONS, FILE_EXTRACTIONS_AMBIGUOUS


class TestWriteAmbiguousExtractions:
    def test_writes_ambiguous(self, tmp_path: Path) -> None:
        df = pd.DataFrame(
            [
                {
                    "extraction_id": "ex1",
                    "evidence_id": "ev1",
                    "extractor": "ie",
                    "head_name_raw": "fruit",
                    "tail_name_raw": "vitamin c",
                    "head_candidates": ["e0", "e99"],
                    "tail_candidates": ["e1"],
                },
                {
                    "extraction_id": "ex2",
                    "evidence_id": "ev2",
                    "extractor": "fdc",
                    "head_name_raw": "apple",
                    "tail_name_raw": "vitamin c",
                    "head_candidates": ["e0"],
                    "tail_candidates": ["e1"],
                },
            ]
        )
        df.to_parquet(tmp_path / FILE_EXTRACTIONS, index=False)
        store = ExtractionStore(path=tmp_path / FILE_EXTRACTIONS)
        write_ambiguous_extractions(store, tmp_path)

        out = tmp_path / FILE_EXTRACTIONS_AMBIGUOUS
        assert out.exists()
        result = pd.read_parquet(out)
        assert len(result) == 1
        assert result.iloc[0]["extraction_id"] == "ex1"

    def test_empty_store(self, tmp_path: Path) -> None:
        pd.DataFrame().to_parquet(tmp_path / FILE_EXTRACTIONS)
        store = ExtractionStore(path=tmp_path / FILE_EXTRACTIONS)
        write_ambiguous_extractions(store, tmp_path)
        assert not (tmp_path / FILE_EXTRACTIONS_AMBIGUOUS).exists()
