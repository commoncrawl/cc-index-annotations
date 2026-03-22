# Wikipedia Categories (International) — website classification from all language Wikipedias

Domains extracted from Wikipedia category articles across all available languages. Uses the English Wikipedia categories as a starting point, then discovers international equivalents via the `langlinks` API.

The classifications reflect Wikipedia editors' categorization, not the opinions of Common Crawl.

## Quick start (default mode)

Fetches 6 curated categories across all languages that have equivalents (~500-2000 domains). Takes about 5 minutes.

```bash
# 1. From the project root, set up the example
make wikipedia-categories-intl

# 2. (optional), run the fetch script with deeper recursion (warning; runtime can exceed 24 hours to complete!)
python wikipedia-categories-intl-fetch.py --deep

# 3. Run the query
cd examples/wikipedia/categories-intl
python annotate.py left_host_index.yaml join_wikipedia_categories_intl.yaml action_star.yaml
```

### How it works

1. Starts from 6 English Wikipedia categories (fake news, fact-checking, satirical, etc.)
2. Uses the [langlinks API](https://www.mediawiki.org/wiki/API:Langlinks) to discover equivalents in all languages (e.g. English "Category:Fake news websites" → Japanese "Category:フェイクニュースサイト")
3. Fetches category members from each language Wikipedia
4. Resolves official website URLs via [Wikidata P856](https://www.wikidata.org/wiki/Property:P856)
5. Outputs a single parquet with `wiki_langs` showing which language editions contributed

### Default categories

| Column | English category | Languages available |
|--------|-----------------|-------------------|
| `wikipedia_cat_fake_news` | Fake news websites | ~6 (JA, ZH, KO, CS, FA, YUE) |
| `wikipedia_cat_fact_checking` | Fact-checking websites | ~8 (ES, PT, RU, TR, UK, CS, BN, FA) |
| `wikipedia_cat_satirical` | Satirical websites | Few |
| `wikipedia_cat_holocaust_denial` | Holocaust-denying websites | Few |
| `wikipedia_cat_alt_right` | Alt-right websites | Few |
| `wikipedia_cat_disinformation` | Disinformation operations | Few |

These 6 columns are always present, even in deep mode.

### Additional columns

- `wiki_langs` — semicolon-separated list of language editions that categorized this domain (e.g. `de;fr;nl`)
- `categories` — semicolon-separated category keys

## Deep mode (`--deep`)

Recursively walks [Category:Websites by topic](https://en.wikipedia.org/wiki/Category:Websites_by_topic) — 80+ subcategories — then discovers international equivalents for each. News and educational categories exist in 25-30+ languages.

**Warning**: This can take more than 24 hours to complete. Progress is cached in `.cache/`, so it resumes where it left off if interrupted.

You can gauge its process by keeping watch on the `.cache/` directory:
```bash
ls -lt .cache/qids_ro_* | head -5
```


### Step by step, how to enable Deep mode:

```bash
# 1. Run the deep fetch (takes hours, cached/resumable)
cd examples/wikipedia/categories-intl
python wikipedia-categories-intl-fetch.py --deep

# 2. Edit join_wikipedia_categories_intl.yaml:
#    Uncomment the additional columns you want (educational, news, science, etc.)

# 3. Edit action_star.yaml:
#    Replace the columns line with: columns: "*"

# 4. Run the query
python annotate.py left_host_index.yaml join_wikipedia_categories_intl.yaml action_star.yaml
```

Use `--deep --no-skip` to also include broad categories (blogs, social networking, streaming, etc.) that are skipped by default.

## Comparison with `categories/`

| | `categories/` | `categories-intl/` |
|---|---|---|
| Source | English Wikipedia only | All language Wikipedias |
| Discovery | Direct category enumeration | langlinks auto-discovery |
| Unique angle | English-centric depth | International breadth |
| Use together? (*)| Yes — different domains |  |

(*) `categories/`(English-only) and `categories-intl/` (all other languages) cover different sets of domains with minimal overlap, so you can stack both join YAMLs in one query to get broader coverage. Like:
```bash
python annotate.py left_host_index.yaml \
  join_wikipedia_categories.yaml \
  join_wikipedia_categories_intl.yaml \
  action_star.yaml
```
A site might appear in English Wikipedia's "Fake news websites" category AND in Russian Wikipedia's equivalent — so by joining them you'd get both labels.


## Data source

- Categories enumerated via [MediaWiki API](https://www.mediawiki.org/wiki/API:Categorymembers)
- Cross-language mapping via [langlinks API](https://www.mediawiki.org/wiki/API:Langlinks)
- Official website URLs resolved via [Wikidata P856](https://www.wikidata.org/wiki/Property:P856)
- Wikipedia content: [CC BY-SA 4.0](https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License)
- Wikidata: [CC0 (public domain)](https://www.wikidata.org/wiki/Wikidata:Licensing)
