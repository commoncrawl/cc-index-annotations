#!/usr/bin/env python3
"""Convert NewsGuard metadata CSV to a Common Crawl host-index annotation.

Usage:
    python newsguard-convert.py <metadata.csv> [output.parquet]

The script reads NewsGuard's metadata feed CSV, filters out the "ALL" country
bucket (which contains duplicates), converts domains to SURT format for joining
against the Common Crawl host index, and writes a sorted parquet file.

Columns exported (all prefixed newsguard_ except the join key):
  - surt_host_name           join key (e.g. "com,example")
  - newsguard_domain         original domain
  - newsguard_rating         T/N/S/P/FL/C/L/A/G
  - newsguard_score          0-100 float (null for unscored categories)
  - newsguard_country        2-letter ISO country code
  - newsguard_orientation    Left / Right / N/A
  - newsguard_criteria_*     9 boolean criteria columns
  - newsguard_flag_*         boolean flag columns (covid, hlth, state, etc.)
  - newsguard_topics         comma-separated topic list
  - newsguard_targeted_audience  Local/Regional/National/International

Requires: duckdb, surt
"""

import sys
import os

# allow importing utils.py from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import csv
import duckdb

try:
    import utils as cc_utils
except ImportError:
    cc_utils = None

try:
    import surt as surt_lib
except ImportError:
    sys.exit('Please install the surt package: pip install surt')



def domain_to_surt_host_name(domain):
    """Convert a domain like 'example.com' to SURT host name 'com,example'."""
    if cc_utils:
        return cc_utils.thing_to_surt_host_name(domain)
    # fallback if utils.py is not on the path
    full = surt_lib.surt('http://' + domain)
    return full.split(')/')[0]


def yesno_to_bool(value):
    """Convert 'Yes'/'No' to Python bool, None for blanks."""
    if value in ('Yes', 'yes', 'YES'):
        return True
    if value in ('No', 'no', 'NO'):
        return False
    return None



# The nine NewsGuard criteria, mapped to short column names
CRITERIA_MAP = {
    'Does not repeatedly publish false or egregiously misleading content':
        'newsguard_criteria_no_false_content',
    'Gathers and presents information responsibly':
        'newsguard_criteria_responsible_reporting',
    'Has effective practices for correcting errors':
        'newsguard_criteria_error_correction',
    'Handles the difference between news and opinion responsibly':
        'newsguard_criteria_news_opinion_separation',
    'Avoids deceptive headlines':
        'newsguard_criteria_no_deceptive_headlines',
    'Website discloses ownership and financing':
        'newsguard_criteria_ownership_disclosure',
    'Clearly labels advertising':
        'newsguard_criteria_ad_labeling',
    "Reveals who's in charge, including any possible conflicts of interest":
        'newsguard_criteria_management_disclosure',
    'The site provides names of content creators, along with either contact or biographical information':
        'newsguard_criteria_content_creator_info',
}

# Known flag short codes from the NewsGuard data definitions
KNOWN_FLAGS = [
    'hideParty', 'PartisanLocal', 'state', 'plag', 'hoax', 'dead',
    'hlth', 'covid', 'vacc', 'elec', 'qannon', 'ukraine', 'abortion',
    'climate', 'israelhamas', 'uain', 'politics', 'stateinfluenced',
    'russiastate', 'chinastate', 'iranstate',
]



def convert(csv_path, output_path='newsguard.parquet'):
    rows_out = []
    skipped_all = 0
    skipped_surt = 0

    with open(csv_path, encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Best practice #1: filter out "ALL" country to avoid duplicates
            if row.get('Country', '') == 'ALL':
                skipped_all += 1
                continue

            domain = row.get('Domain', '').strip()
            if not domain:
                continue

            # Convert domain to SURT
            try:
                surt_host = domain_to_surt_host_name(domain)
            except (ValueError, TypeError, Exception) as e:
                skipped_surt += 1
                print(f'  skipping {domain}: {e}', file=sys.stderr)
                continue

            if not surt_host:
                skipped_surt += 1
                continue

            # Parse score
            raw_score = row.get('Score', '').strip()
            try:
                score = float(raw_score) if raw_score else None
            except ValueError:
                score = None

            # Parse criteria
            criteria = {}
            for csv_col, parquet_col in CRITERIA_MAP.items():
                criteria[parquet_col] = yesno_to_bool(row.get(csv_col, ''))

            # Parse flags into individual booleans
            raw_flags = row.get('Flags', '').strip()
            active_flags = set(
                f.strip() for f in raw_flags.split(',') if f.strip()
            )
            flags = {}
            for flag in KNOWN_FLAGS:
                flags[f'newsguard_flag_{flag.lower()}'] = flag in active_flags

            record = {
                'surt_host_name': surt_host,
                'newsguard_domain': domain,
                'newsguard_rating': row.get('Rating', '').strip() or None,
                'newsguard_score': score,
                'newsguard_country': row.get('Country', '').strip() or None,
                'newsguard_orientation': row.get('Orientation', '').strip() or None,
                'newsguard_topics': row.get('Topics', '').strip() or None,
                'newsguard_targeted_audience':
                    row.get('Targeted Audience', '').strip() or None,
                **criteria,
                **flags,
            }
            rows_out.append(record)

    if not rows_out:
        sys.exit('No rows after filtering — check that the CSV is valid.')

    print(f'Processed {len(rows_out)} sites '
          f'(skipped {skipped_all} ALL-country duplicates, '
          f'{skipped_surt} SURT failures)')

    # Load into DuckDB for sorting and parquet export
    import pyarrow as pa

    # Build pyarrow table from list of dicts
    columns = list(rows_out[0].keys())
    arrays = {}
    for col in columns:
        values = [r[col] for r in rows_out]
        if col.startswith('newsguard_flag_') or col.startswith('newsguard_criteria_'):
            arrays[col] = pa.array(values, type=pa.bool_())
        elif col == 'newsguard_score':
            arrays[col] = pa.array(values, type=pa.float32())
        else:
            arrays[col] = pa.array(values, type=pa.string())
    table = pa.table(arrays)

    con = duckdb.connect()
    con.register('ng', table)
    con.execute(f"""
        COPY (SELECT * FROM ng ORDER BY surt_host_name)
        TO '{output_path}' (FORMAT PARQUET)
    """)
    print(f'Wrote {output_path}')

    # Print a quick summary
    result = con.execute(
        'SELECT newsguard_rating, COUNT(*) as n FROM ng GROUP BY 1 ORDER BY 2 DESC'
    ).fetchall()
    print('Rating distribution:')
    for rating, n in result:
        print(f'  {rating}: {n}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} <metadata.csv> [output.parquet]',
              file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'newsguard.parquet'
    convert(csv_path, output_path)
