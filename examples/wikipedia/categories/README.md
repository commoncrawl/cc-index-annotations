# Wikipedia Categories — website classification from English Wikipedia

Domains extracted from Wikipedia articles belonging to curated website categories, with official URLs resolved via Wikidata (P856 property).

The classifications in this dataset reflect Wikipedia editors' categorization of these websites, not the opinions of Common Crawl.

## Categories

| Column | Category | Articles |
|--------|----------|----------|
| `wikipedia_cat_fake_news` | [Fake news websites](https://en.wikipedia.org/wiki/Category:Fake_news_websites) | ~69 |
| `wikipedia_cat_fact_checking` | [Fact-checking websites](https://en.wikipedia.org/wiki/Category:Fact-checking_websites) | ~56 |
| `wikipedia_cat_satirical` | [Satirical websites](https://en.wikipedia.org/wiki/Category:Satirical_websites) | ~21 |
| `wikipedia_cat_holocaust_denial` | [Holocaust-denying websites](https://en.wikipedia.org/wiki/Category:Holocaust-denying_websites) | ~19 |
| `wikipedia_cat_alt_right` | [Alt-right websites](https://en.wikipedia.org/wiki/Category:Alt-right_websites) | ~29 |
| `wikipedia_cat_disinformation` | [Disinformation operations](https://en.wikipedia.org/wiki/Category:Disinformation_operations) | ~54 |

## Usage

```bash
python wikipedia-categories-fetch.py        # fetch and build parquet
python wikipedia-categories-fetch.py -d     # also write CSV for debugging

python annotate.py left_host_index.yaml join_wikipedia_categories.yaml action_star.yaml
```

## Data source

- Categories enumerated via [MediaWiki API](https://www.mediawiki.org/wiki/API:Categorymembers)
- Official website URLs resolved via [Wikidata P856](https://www.wikidata.org/wiki/Property:P856)
- Wikipedia content: [CC BY-SA 4.0](https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License)
- Wikidata: [CC0 (public domain)](https://www.wikidata.org/wiki/Wikidata:Licensing)
