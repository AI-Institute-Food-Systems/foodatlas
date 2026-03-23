import argparse
import glob
import os

import pandas as pd


def aggregate_food_chem_sentences(
    input_dir: str, aggregated_path: str, ie_input_path: str, reference_dir: str, threshold: float
) -> None:
    pattern = os.path.join(input_dir, "*.tsv")
    tsv_files = sorted(glob.glob(pattern))

    if not tsv_files:
        print(f"No TSV files found in {input_dir}")
        return

    print(f"Found {len(tsv_files)} TSV files")

    all_dataframes = []
    for file_path in tsv_files:
        df = pd.read_csv(file_path, sep="\t")
        all_dataframes.append(df)

    all_sentences = pd.concat(all_dataframes, ignore_index=True)

    passed = all_sentences[all_sentences["answer"] >= threshold]

    if passed.empty:
        print(f"No rows passed threshold {threshold}")
    else:
        os.makedirs(os.path.dirname(os.path.abspath(aggregated_path)), exist_ok=True)
        passed.to_csv(aggregated_path, sep="\t", index=False)
        print(f"Saved {len(passed)} rows (threshold={threshold}) to {aggregated_path}")

    # Collect PMCIDs from the reference (old) file.
    # The reference uses bare numeric IDs (e.g. 7794732) while the new data
    # uses the "PMC" prefix (e.g. PMC7794732), so normalise both to bare integers.
    def strip_pmc(series: pd.Series) -> pd.Series:
        return series.astype(str).str.removeprefix("PMC").astype(int)

    reference_pmcids = set()
    ref_files = sorted(glob.glob(os.path.join(reference_dir, "text_parser_predicted_*")))
    if ref_files:
        ref_frames = [pd.read_csv(f, sep="\t") for f in ref_files]
        reference_pmcids = set(strip_pmc(pd.concat(ref_frames, ignore_index=True)["pmcid"]))
        print(f"Loaded {len(reference_pmcids)} reference PMCIDs from {len(ref_files)} file(s) in {reference_dir}")
    else:
        print(f"No reference files found in {reference_dir}")

    unseen = passed[~strip_pmc(passed["pmcid"]).isin(reference_pmcids)]
    os.makedirs(os.path.dirname(os.path.abspath(ie_input_path)), exist_ok=True)
    unseen.to_csv(ie_input_path, sep="\t", index=False)
    print(
        f"Saved {len(unseen)} new sentences "
        f"({unseen['pmcid'].nunique()} PMCIDs not shared with reference) "
        f"to {ie_input_path}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate food-chemical sentences filtered by BioBERT probability"
    )
    parser.add_argument(
        "--input_dir",
        type=str,
        default="outputs/text_parser/sentence_filtering",
        help="Directory containing BioBERT output chunk TSV files",
    )
    parser.add_argument(
        "--aggregated_path",
        type=str,
        default="outputs/text_parser/filtered_sentences/filtered_sentence_aggregated.tsv",
        help="Path where the aggregated TSV file will be saved",
    )
    parser.add_argument(
        "--ie_input_path",
        type=str,
        default="outputs/text_parser/filtered_sentences/information_extraction_input.tsv",
        help="Path where sentences from unseen PMCIDs will be saved",
    )
    parser.add_argument(
        "--reference_dir",
        type=str,
        default="outputs/past_sentence_filtering_preds",
        help="Directory containing reference TSVs (text_parser_predicted_*) used to identify known PMCIDs",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.99,
        help="Minimum BioBERT probability to keep a row (default: 0.99)",
    )

    args = parser.parse_args()
    aggregate_food_chem_sentences(
        args.input_dir, args.aggregated_path, args.ie_input_path, args.reference_dir, args.threshold
    )
