# Wikipedia Categories — website classification from English Wikipedia

Domains extracted from Wikipedia articles belonging to website categories, with official URLs resolved via Wikidata (P856 property).

The classifications reflect Wikipedia editors' categorization, not the opinions of Common Crawl.

## Quick start (default mode)

Fetches 6 curated categories (~160 domains). Fast — takes about 2 minutes.

```bash
# 1. From the project root, set up the example
make wikipedia-categories

# 2. Run the query
cd examples/wikipedia/categories
python annotate.py left_host_index.yaml join_wikipedia_categories.yaml action_star.yaml
```

### Default categories

| Column | Category | Articles |
|--------|----------|----------|
| `wikipedia_cat_fake_news` | [Fake news websites](https://en.wikipedia.org/wiki/Category:Fake_news_websites) | ~69 |
| `wikipedia_cat_fact_checking` | [Fact-checking websites](https://en.wikipedia.org/wiki/Category:Fact-checking_websites) | ~56 |
| `wikipedia_cat_satirical` | [Satirical websites](https://en.wikipedia.org/wiki/Category:Satirical_websites) | ~21 |
| `wikipedia_cat_holocaust_denial` | [Holocaust-denying websites](https://en.wikipedia.org/wiki/Category:Holocaust-denying_websites) | ~19 |
| `wikipedia_cat_alt_right` | [Alt-right websites](https://en.wikipedia.org/wiki/Category:Alt-right_websites) | ~29 |
| `wikipedia_cat_disinformation` | [Disinformation operations](https://en.wikipedia.org/wiki/Category:Disinformation_operations) | ~54 |

These 6 columns are always present, even in deep mode.

## Deep mode (`--deep`)

Recursively walks [Category:Websites by topic](https://en.wikipedia.org/wiki/Category:Websites_by_topic) — 80+ subcategories, 4 levels deep — producing 10K-50K categorized domains with additional topic columns like `wikipedia_cat_educational`, `wikipedia_cat_news`, `wikipedia_cat_science`, `wikipedia_cat_health`, etc.

**Warning**: This takes several hours due to polite rate limiting across thousands of Wikipedia/Wikidata API calls. Progress is cached, so it resumes where it left off if interrupted.

### Step by step, how to enable Deep mode:

```bash
# 1. Run the deep fetch (takes hours, cached/resumable)
cd examples/wikipedia/categories
python wikipedia-categories-fetch.py --deep

# 2. Edit join_wikipedia_categories.yaml:
#    Uncomment the additional columns you want (educational, news, science, etc.)

# 3. Edit action_star.yaml:
#    Replace the columns line with: columns: "*"

# 4. Run the query
python annotate.py left_host_index.yaml join_wikipedia_categories.yaml action_star.yaml
```

## Data source

- Categories enumerated via [MediaWiki API](https://www.mediawiki.org/wiki/API:Categorymembers)
- Official website URLs resolved via [Wikidata P856](https://www.wikidata.org/wiki/Property:P856)
- Wikipedia content: [CC BY-SA 4.0](https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License)
- Wikidata: [CC0 (public domain)](https://www.wikidata.org/wiki/Wikidata:Licensing)
