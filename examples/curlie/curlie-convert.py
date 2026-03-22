#!/usr/bin/env python3
"""Convert Curlie TSV files to a deduplicated parquet with SURT host names."""
import sys, os, duckdb
from pyarrow import csv as pa_csv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import utils

DEBUG = '-d' in sys.argv or '--debug' in sys.argv

con = duckdb.connect()

# LANGUAGE MAP — Curlie World/* language names to ISO codes
LANG_MAP = {
    'Afrikaans': 'af', 'Arabic': 'ar', 'Aragonés': 'an', 'Armenian': 'hy',
    'Asturianu': 'ast', 'Azerbaijani': 'az',
    'Bahasa_Indonesia': 'id', 'Bahasa_Malaysia': 'ms', 'Bahasa_Melayu': 'ms',
    'Bangla': 'bn', 'Bashkir': 'ba', 'Basque': 'eu', 'Belarusian': 'be',
    'Bokmål': 'nb', 'Bosanski': 'bs', 'Brezhoneg': 'br', 'Bulgarian': 'bg',
    'Bân-lâm-gú': 'nan', 'Català': 'ca', 'Cesky': 'cs', 'Chinese_Simplified': 'zh',
    'Chinese_Traditional': 'zh', 'Cymraeg': 'cy', 'Česky': 'cs',
    'Dansk': 'da', 'Deutsch': 'de',
    'Eesti': 'et', 'Ellada': 'el', 'Esperanto': 'eo', 'Español': 'es',
    'Euskara': 'eu', 'Faroese': 'fo', 'Filipino': 'fil', 'Français': 'fr',
    'Furlan': 'fur', 'Frysk': 'fy', 'Føroyskt': 'fo',
    'Gaeilge': 'ga', 'Galego': 'gl', 'Greek': 'el', 'Gujarati': 'gu',
    'Gàidhlig': 'gd', 'Hebrew': 'he', 'Hindi': 'hi', 'Hrvatski': 'hr',
    'Hungarian': 'hu', 'Icelandic': 'is', 'Interlingua': 'ia', 'Irish': 'ga',
    'Italiano': 'it', 'Íslenska': 'is',
    'Japanese': 'ja', 'Kannada': 'kn', 'Kaszëbsczi': 'csb', 'Kazakh': 'kk',
    'Kiswahili': 'sw', 'Korean': 'ko', 'Kurdish': 'ku', 'Kurdî': 'ku',
    'Kyrgyz': 'ky',
    'Latina': 'la', 'Latviešu': 'lv', 'Latviski': 'lv', 'Lietuvių': 'lt',
    'Lingua_Latina': 'la', 'Lëtzebuergesch': 'lb',
    'Macedonian': 'mk', 'Magyar': 'hu', 'Makedonski': 'mk', 'Marathi': 'mr',
    'Nederlands': 'nl', 'Nordfriisk': 'frr', 'Norsk': 'no', 'Norsk_Nynorsk': 'nn',
    'Occitan': 'oc', 'Ossetian': 'os', "O'zbekcha": 'uz',
    'Persian': 'fa', 'Polski': 'pl', 'Português': 'pt',
    'Punjabi_Gurmukhi': 'pa',
    'Românã': 'ro', 'Română': 'ro', 'Rumantsch': 'rm', 'Russian': 'ru',
    'Sardu': 'sc', 'Scots_Gaelic': 'gd', 'Seeltersk': 'stq', 'Serbian': 'sr',
    'Shqip': 'sq', 'Sicilianu': 'scn', 'Sinhala': 'si', 'Sinhalese': 'si',
    'Slovensko': 'sl', 'Slovensky': 'sk', 'Slovenčina': 'sk', 'Slovenščina': 'sl',
    'Srpski': 'sr', 'Suomi': 'fi', 'Svenska': 'sv',
    'Tagalog': 'tl', 'Taiwanese': 'nan', 'Tajik': 'tg', 'Tamil': 'ta',
    'Tatarça': 'tt', 'Telugu': 'te', 'Thai': 'th', 'Tiếng_Việt': 'vi',
    'Türkmençe': 'tk', 'Türkçe': 'tr',
    'Ukrainian': 'uk', 'Urdu': 'ur', 'Uyghurche': 'ug',
    'Welsh': 'cy',
    '中文': 'zh', '简体中文': 'zh', '繁體中文': 'zh',
    # Future-proofing — languages Curlie may add
    'Albanian': 'sq', 'Amharic': 'am', 'Bengali': 'bn', 'Burmese': 'my',
    'Cantonese': 'yue', 'Croatian': 'hr', 'Czech': 'cs', 'Danish': 'da',
    'Dutch': 'nl', 'Estonian': 'et', 'Finnish': 'fi', 'Georgian': 'ka',
    'German': 'de', 'Hausa': 'ha', 'Igbo': 'ig', 'Indonesian': 'id',
    'Italian': 'it', 'Javanese': 'jv', 'Khmer': 'km', 'Lao': 'lo',
    'Latin': 'la', 'Latvian': 'lv', 'Lithuanian': 'lt', 'Luxembourgish': 'lb',
    'Malagasy': 'mg', 'Malay': 'ms', 'Malayalam': 'ml', 'Maltese': 'mt',
    'Mandarin': 'zh', 'Mongolian': 'mn', 'Nepali': 'ne', 'Norwegian': 'no',
    'Pashto': 'ps', 'Polish': 'pl', 'Portuguese': 'pt', 'Romanian': 'ro',
    'Slovak': 'sk', 'Slovenian': 'sl', 'Somali': 'so', 'Spanish': 'es',
    'Sundanese': 'su', 'Swahili': 'sw', 'Swedish': 'sv', 'Turkish': 'tr',
    'Uzbek': 'uz', 'Vietnamese': 'vi', 'Yoruba': 'yo', 'Zulu': 'zu',
    'Chinese': 'zh', 'French': 'fr', 'Hindi_Devanagari': 'hi',
    'Maori': 'mi', 'Sanskrit': 'sa', 'Tibetan': 'bo', 'Yiddish': 'yi',
}
lang_cases = '\n'.join(f"    WHEN '{name.replace(chr(39), chr(39)+chr(39))}' THEN '{code}'" for name, code in LANG_MAP.items())

# READ SITES — join with categories to get path, derive language
print('Reading sites and categories...')
con.sql(f"""
CREATE TABLE sites AS
WITH raw_sites AS (
    SELECT url, title, category_id
    FROM read_csv('curlie-rdf/rdf-*-c.tsv', sep='\t', header=false,
      columns={{'url': 'VARCHAR', 'title': 'VARCHAR', 'description': 'VARCHAR', 'category_id': 'BIGINT'}},
      ignore_errors=true, null_padding=true, strict_mode=false, parallel=false)
    WHERE url IS NOT NULL AND length(url) > 5
),
raw_cats AS (
    SELECT category_id, category_path
    FROM read_csv('curlie-rdf/rdf-*-s.tsv', sep='\t', header=false,
      columns={{'category_id': 'BIGINT', 'category_path': 'VARCHAR', 'site_count': 'INTEGER', 'description': 'VARCHAR', 'geo1': 'VARCHAR', 'geo2': 'VARCHAR'}},
      ignore_errors=true, null_padding=true, strict_mode=false, parallel=false)
),
joined AS (
    SELECT s.url, s.title, c.category_path,
      CASE WHEN c.category_path LIKE 'World/%'
        THEN split_part(c.category_path, '/', 2)
        ELSE 'English'
      END as lang_name
    FROM raw_sites s LEFT JOIN raw_cats c ON s.category_id = c.category_id
)
SELECT url, title, category_path,
  CASE lang_name
{lang_cases}
    WHEN 'English' THEN 'en'
    ELSE lang_name
  END as lang
FROM joined
""")

total = con.sql("SELECT count(*) FROM sites").fetchone()[0]
print(f'  {total} site entries')

unmapped = con.sql("SELECT DISTINCT lang FROM sites WHERE length(lang) > 4 ORDER BY lang").fetchall()
if unmapped:
    print(f'  WARNING: {len(unmapped)} unmapped languages (add to LANG_MAP):')
    for row in unmapped:
        print(f'    {row[0]}')

# EXTRACT DOMAIN — one row per (url, category, lang)
print('Extracting domains...')
con.sql("""
CREATE TABLE extracted AS
SELECT DISTINCT
  url,
  split_part(split_part(url, '://', 2), '/', 1) as domain,
  lang,
  category_path as category
FROM sites
WHERE (url LIKE 'http://%' OR url LIKE 'https://%')
  AND category_path IS NOT NULL
  AND domain LIKE '%.%'
  AND length(domain) BETWEEN 4 AND 253
  AND domain NOT LIKE '%..%'
  AND NOT regexp_matches(domain, '[%\s<>"''\\\\{}|^`\[\]@!$&*()+=,;]')
""")

rows = con.sql("SELECT count(*) FROM extracted").fetchone()[0]
domains_unique = con.sql("SELECT count(DISTINCT domain) FROM extracted").fetchone()[0]
print(f'  {rows} (domain, category, lang) rows, {domains_unique} unique domains')

# ADD SURT
print('Adding SURT keys...')
import surt as surt_lib

rows = con.sql("SELECT url, domain FROM extracted").fetchall()
surt_data = []
for url, domain in rows:
    try:
        surt_host = utils.thing_to_surt_host_name(domain)
        url_surtkey = surt_lib.surt(url)
    except (ValueError, TypeError):
        continue
    if surt_host:
        surt_data.append((url, surt_host, url_surtkey))

con.execute("CREATE TABLE surt_lookup (url VARCHAR, surt_host_name VARCHAR, url_surtkey VARCHAR)")
con.executemany("INSERT INTO surt_lookup VALUES (?, ?, ?)", surt_data)

result = con.sql("""
SELECT s.surt_host_name, s.url_surtkey, e.domain, e.lang, e.category
FROM extracted e JOIN surt_lookup s ON e.url = s.url
ORDER BY s.surt_host_name
""")

table = result.fetch_arrow_table()
print(f'Rows: {table.num_rows}')

import pyarrow.parquet as pq
pq.write_table(table, 'curlie.parquet')
print('Wrote curlie.parquet')

if DEBUG:
    pa_csv.write_csv(table, 'curlie.csv')
    print('Wrote curlie.csv')
