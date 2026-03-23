import argparse
from ast import literal_eval
from datetime import date
from functools import partial
from glob import glob
from itertools import islice
import json
from multiprocessing import Pool, cpu_count
import os
from pathlib import Path
import subprocess
import sys
import time

from thefuzz import fuzz
from nltk.tokenize.punkt import  PunktSentenceTokenizer
import numpy as np
import pandas as pd
from tqdm import tqdm

DEFAULT_CHUNK_SIZE = 5000
DEFAULT_SAVE_EVERY = 50
DEFAULT_LAST_SEARCH_DATE_FILEPATH = 'outputs/text_parser/last_search_date.txt'
DEFAULT_EMAIL = 'kcxie@ucdavis.edu'
DEFAULT_FILEPATH_BIOC_PMC = '/mnt/data/shared/BioC-PMC'
DEFAULT_FILEPATH_FOOD_NAMES = '/mnt/share/kaichixie/foodatlas_pipeline/data/translated_food_terms.txt'
DEFAULT_FILTERED_SENTENCES_FILEPATH = 'outputs/text_parser/retrieved_sentences/result_{i}.tsv'
QUERY_TEMPLATE = '{food} AND ((compound) OR (nutrient))'

ALLOWED_SECTIONS = [
    'INTRO',
    'METHODS',
    'CONCL',
    'TITLE',
    'ABSTRACT',
    'RESULTS',
    'DISCUSS',
]

ALLOWED_TYPES = [
    'abstract',
    'paragraph',
]


def parse_argument() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='')

    parser.add_argument(
        '--query',
        type=str,
        required=True,
        help='Single query (cocoa), multiple queries (cocoa,banana), or '
             '.txt file containing one query for each line.',
    )

    parser.add_argument(
        '--save_every',
        type=int,
        default=DEFAULT_SAVE_EVERY,
        help=f'Save to results once every {DEFAULT_SAVE_EVERY} queries.',
    )

    parser.add_argument(
        '--query_uid_results_filepath',
        type=str,
        # required=True,
        default='outputs/text_parser/query_uid_results.tsv',
        help='Output directory',
    )

    parser.add_argument(
        '--email',
        type=str,
        default=DEFAULT_EMAIL,
        help='Email associated with the E-utility API key.',
    )

    parser.add_argument(
        '--filepath_BioC_PMC',
        type=str,
        default=DEFAULT_FILEPATH_BIOC_PMC,
        help=f'Filepath containing the .tar.gz files (Default: {DEFAULT_FILEPATH_BIOC_PMC}).',
    )

    parser.add_argument(
        '--filepath_food_names',
        type=str,
        default=DEFAULT_FILEPATH_FOOD_NAMES,
        help=f'Filepath containing all food names (Default: {DEFAULT_FILEPATH_FOOD_NAMES}).',
    )

    parser.add_argument(
        '--filtered_sentences_filepath',
        type=str,
        default=DEFAULT_FILTERED_SENTENCES_FILEPATH,
        help=f'Filepath to save the results (Default: {DEFAULT_FILTERED_SENTENCES_FILEPATH}).',
    )

    parser.add_argument(
        '--min_date',
        type=str,
        default=None,
        help='Minimum publication date filter passed to esearch -mindate. '
             'Formats: YYYY, YYYY/MM, or YYYY/MM/DD (e.g. 2023, 2023/06, 2023/06/15). '
             'If omitted, falls back to the value stored in --last_search_date_filepath.',
    )

    parser.add_argument(
        '--last_search_date_filepath',
        type=str,
        default=DEFAULT_LAST_SEARCH_DATE_FILEPATH,
        help=f'File that stores the date of the last run (Default: {DEFAULT_LAST_SEARCH_DATE_FILEPATH}). '
             'Auto-loaded as min_date when --min_date is not set; updated at the start of each run.',
    )

    return parser.parse_args()


def parse_query(query):
    if query.endswith('.txt'):
        df = pd.read_csv(query, sep='\t', keep_default_na=False)
        queries = df['query'].tolist()
    elif ',' in query:
        queries = query.split(',')
    else:
        queries = [query]

    print(f'Got {len(queries)} queries.')
    print(f'First five: {queries[:5]}')

    return queries


def parse_db(db):
    if db == 'pmc':
        return ['pmc']
    elif db == 'pubmed':
        return ['pubmed']
    elif db == 'both':
        return ['pmc', 'pubmed']
    else:
        raise ValueError()


def get_pmcid_pmid_mapping(filepath='data/NCBI/PMC-ids.csv'):
    print('Fetching PMCID-PMID mapping...')

    df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
    pmcid_pmid_dict = dict(zip(df['PMCID'], df['PMID']))
    pmid_pmcid_dict = dict(zip(df['PMID'], df['PMCID']))

    return pmcid_pmid_dict, pmid_pmcid_dict

# (pmid,pmcid): [queries]
def load_data(filepath, pmcid_pmid_dict, pmid_pmcid_dict):
    print(f'Loading data from {filepath}...')

    if not Path(filepath).is_file():
        return {}, []

    df = pd.read_csv(filepath, sep='\t', dtype=str, keep_default_na=False)

    # make sure the pmid and pmcid is up to date
    print('Checking pmid & pmcid integrity...')
    for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
        pmid = row['pmid']
        pmcid = row['pmcid']

        if pmid and pmcid:
            assert pmid_pmcid_dict[row['pmid']] == pmcid
            assert pmcid_pmid_dict[row['pmcid']] == pmid
        elif pmid == '':
            if pmcid in pmcid_pmid_dict:
                df.at[idx, 'pmid'] = pmcid_pmid_dict[pmcid]
        elif pmcid == '':
            if pmid in pmid_pmcid_dict:
                df.at[idx, 'pmcid'] = pmid_pmcid_dict[pmid]
        else:
            raise RuntimeError()

    df['key'] = tuple(zip(df['pmid'], df['pmcid']))
    df['queries'] = df['queries'].apply(literal_eval)

    data = dict(zip(df['key'], df['queries']))
    previous_queries = set([y for x in data.values() for y in x])

    return data, previous_queries


def save_data(data, filepath):
    df = pd.DataFrame({'key': data.keys(), 'queries': data.values()})
    df['pmid'] = df['key'].apply(lambda x: x[0])
    df['pmcid'] = df['key'].apply(lambda x: x[1])

    df = df[['pmid', 'pmcid', 'queries']]
    df.to_csv(filepath, sep='\t', index=False)


def get_all_foods(filepath):
    if not os.path.isfile(filepath):
        raise FileNotFoundError()

    df = pd.read_csv(filepath, sep='\t', keep_default_na=False)
    df['translation'] = df['translation'].apply(literal_eval)

    foods_trans_dict = dict(zip(df['query'], df['translation']))
    print(f'Got {len(foods_trans_dict)} foods.')

    return foods_trans_dict


def pmcid_to_filepath(pmcid, parent_dir):
    return os.path.join(parent_dir, f'{pmcid}.xml')


def get_filtered_sentences(sentence_tokenizer, filepath_BioC_PMC, foods_trans_dict, key_val_pair):
    key, queries = key_val_pair
    pmid, pmcid = key

    my_dict = {
        'pmcid': [],
        'section': [],
        'matched_query': [],
        'sentence': [],
    }

    if not pmcid.replace('PMC', '').isdigit():
        return pd.DataFrame(my_dict)

    filepath = pmcid_to_filepath(pmcid, filepath_BioC_PMC)

    if not os.path.isfile(filepath):
        return pd.DataFrame(my_dict)

    with open(filepath) as f:
        json_data = json.load(f)

    documents = json_data['documents']
    assert len(documents) == 1
    documents = documents[0]

    pmcid = documents['id']

    translated_queries = []
    for q in queries:
        if q in foods_trans_dict:
            translated_queries.extend(foods_trans_dict[q])
        else:
            translated_queries.extend(q)

    for passage in documents['passages']:
        if 'infons' not in passage:
            continue
        infons = passage['infons']

        if 'section_type' not in infons:
            continue
        infons_section_type = infons['section_type']

        if 'type' not in infons:
            continue
        infons_type = infons['type']

        if not (infons_section_type in ALLOWED_SECTIONS and infons_type in ALLOWED_TYPES):
            continue

        for sentence in sentence_tokenizer.tokenize(passage['text']):
            # similar to LitSense. Remove either too short or too long sentences.
            if len(sentence) < 20 or len(sentence) > 1000:
                continue

            for tq in translated_queries:
                if fuzz.token_set_ratio(sentence, tq) > 90:
                    my_dict['pmcid'].append(pmcid)
                    my_dict['section'].append(infons_section_type)
                    my_dict['matched_query'].append(tq)
                    my_dict['sentence'].append(sentence)
                    break

    return pd.DataFrame(my_dict)


def main():
    args = parse_argument()

    # ── Min-date: fall back to saved last-run date if not explicitly set ─────
    if args.min_date is None and os.path.isfile(args.last_search_date_filepath):
        args.min_date = Path(args.last_search_date_filepath).read_text().strip()
        print(f'Using saved min_date: {args.min_date}')

    # Record today as the new last-search date before querying starts
    today = date.today().strftime('%Y/%m/%d')
    Path(args.last_search_date_filepath).parent.mkdir(exist_ok=True, parents=True)
    Path(args.last_search_date_filepath).write_text(today)
    print(f'Recorded search date: {today} → {args.last_search_date_filepath}')

    pmcid_pmid_dict, pmid_pmcid_dict = get_pmcid_pmid_mapping()

    Path(args.query_uid_results_filepath).parent.mkdir(exist_ok=True, parents=True)
    Path(args.filtered_sentences_filepath).parent.mkdir(exist_ok=True, parents=True)
    data, previous_queries = load_data(
        args.query_uid_results_filepath,
        pmcid_pmid_dict=pmcid_pmid_dict,
        pmid_pmcid_dict=pmid_pmcid_dict,
    )

    #####################################################
    # search queries and save the query keyword and IDs #
    #####################################################
    print('\nSearching queries and saving the UIDs...')

    queries = parse_query(args.query)

    pbar = tqdm(queries)
    for idx, q in enumerate(pbar):
        pbar.set_description(f'Processing {q}')

        # skip this query if it was already searched before
        if q in previous_queries:
            continue

        # iterate over pmc or pubmed or both
        for db in ['pubmed', 'pmc']:
            try:
                esearch_args = ['esearch', '-db', db, '-query', QUERY_TEMPLATE.format(food=q),
                                '-email', args.email, '-datetype', 'pdat']
                if args.min_date:
                    esearch_args += ['-mindate', args.min_date]
                esearch = subprocess.Popen(esearch_args, stdout=subprocess.PIPE)
                efetch_args = ('efetch', '-format', 'uid', '-email', args.email)
                result = subprocess.check_output(efetch_args, stdin=esearch.stdout, text=True)
                esearch.wait()
            except Exception as e:
                print(f'Exception occured while processing {q}!')
                print(e)
                continue

            ids = [f'PMC{id}' if db == 'pmc' else id for id in result.rstrip().split('\n')]

            for id in ids:
                if db == 'pmc':
                    pmid = ''
                    pmcid = id
                    if id in pmcid_pmid_dict:
                        pmid = pmcid_pmid_dict[id]

                if db == 'pubmed':
                    pmid = id
                    pmcid = ''
                    if id in pmid_pmcid_dict:
                        pmcid = pmid_pmcid_dict[id]

                key = (pmid, pmcid)
                if key in data:
                    if q not in data[key]:
                        data[key].append(q)
                else:
                    data[key] = [q]

        if idx % args.save_every == 0:
            save_data(data, args.query_uid_results_filepath)

        time.sleep(0.33)

    save_data(data, args.query_uid_results_filepath)

    ###################################################
    # now search the actual text containing food query #
    ###################################################
    print('Using the query results to find the actual sentences...')

    foods_trans_dict = get_all_foods(args.filepath_food_names)

    print('Loading query results...')
    df = pd.read_csv(args.query_uid_results_filepath, sep='\t', dtype=str, keep_default_na=False)
    df['key'] = tuple(zip(df['pmid'], df['pmcid']))
    df['queries'] = df['queries'].apply(literal_eval)
    data = dict(zip(df['key'], df['queries']))

    sentence_tokenizer = PunktSentenceTokenizer()

    dict_df = {
        'pmcid': [],
        'section': [],
        'matched_query': [],
        'sentence': [],
    }

    def chunks(d, size=DEFAULT_CHUNK_SIZE):
        it = iter(d)
        for i in range(0, len(d), size):
            yield {k: d[k] for k in islice(it, size)}

    for idx, small_data in enumerate(tqdm(chunks(data), total=int(len(data)/DEFAULT_CHUNK_SIZE))):
        # use multiprocessing.Pool.imap for the next for loop
        with Pool(cpu_count()-1) as p:
            partial_function = partial(
                get_filtered_sentences,
                sentence_tokenizer,
                args.filepath_BioC_PMC,
                foods_trans_dict,
            )

            r = list(p.imap_unordered(partial_function, small_data.items()))

        df = pd.concat(r)
        df.to_csv(
            args.filtered_sentences_filepath.format(i=idx),
            sep='\t',
            index=False,
        )

    # Merge all chunk files into a single sentence_filtering_input.tsv
    chunk_dir = Path(args.filtered_sentences_filepath).parent
    chunk_files = sorted(chunk_dir.glob('result_*.tsv'))
    if chunk_files:
        merged = pd.concat(
            [pd.read_csv(f, sep='\t', dtype=str, keep_default_na=False) for f in chunk_files],
            ignore_index=True,
        )
    else:
        merged = pd.DataFrame(columns=['pmcid', 'section', 'matched_query', 'sentence'])
    out_path = chunk_dir / 'sentence_filtering_input.tsv'
    merged.to_csv(out_path, sep='\t', index=False)
    print(f'Merged {len(chunk_files)} chunk files → {len(merged)} rows → {out_path}')


if __name__ == '__main__':
    main()
