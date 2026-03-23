# Slashtag Data (blekko)

Human-curated topic categories from [blekko](https://github.com/blekko/slashtag-data)'s slashtag filter system. 120K+ domains across 1,280 topic categories (12-step, accounting, aerospace, newspapers, etc.).

The annotations in this file are opinions held by blekko's editors, not Common Crawl. The data remains the property of the original source.

## Data

- **Source**: https://github.com/blekko/slashtag-data
- **Format**: JSON → parquet
- **Domains**: ~120K unique
- **Categories**: 1,280 human-curated topic tags

## Usage

```bash
make slashtag           # from project root

cd examples/slashtag
python annotate.py left_host_index.yaml join_slashtag.yaml action_star.yaml
python annotate.py left_host_index.yaml join_slashtag.yaml action_multi_category.yaml
```

