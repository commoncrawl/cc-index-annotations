# Curlie — Human-Edited Web Directory

The largest human-edited directory of the web. Started as the Open Directory Project (ODP), later became DMOZ, and in 2017 relaunched as [Curlie](https://curlie.org/) to continue the 100% free directory.

The annotations in this file are opinions held by Curlie's editors, not Common Crawl. Data licensed under the [Curlie Open Source License](https://curlie.org/docs/en/license.html).

## Data

| Column | Description |
|--------|-------------|
| `surt_host_name` | SURT-encoded host for joining with CC host index |
| `url_surtkey` | SURT-encoded full URL for joining with CC URL index |
| `domain` | Original domain |
| `lang` | ISO 639 language code (`en`, `de`, `fr`, `ja`, etc.) |
| `category` | Full Curlie category path (e.g. `Home/Cooking/Baking`) |

~3.3M rows covering ~1.9M unique domains across 90+ languages and 770K categories.

Each row is a unique (URL, category, lang) tuple. A domain can appear many times — once per URL and category it's listed under. Use `surt_host_name` for host-level joins, `url_surtkey` for URL-level joins.

## Usage

```bash
make curlie                    # download + convert (downloads ~170MB)

cd examples/curlie
python annotate.py left_host_index.yaml join_curlie.yaml action_star.yaml

# Find all cooking sites across all languages
python annotate.py left_host_index.yaml join_curlie.yaml action_cooking.yaml
```

## Querying tips

```sql
-- English cooking sites
WHERE category LIKE '%Cooking%' AND lang = 'en'

-- All science sites regardless of language
WHERE category LIKE '%Science%' OR category LIKE '%Wissenschaft%' OR category LIKE '%Sciences%'

-- Everything in Japanese
WHERE lang = 'ja'

-- Top-level topic browsing
WHERE category LIKE 'Arts/%'
WHERE category LIKE 'World/Deutsch/Computer/%'
```
