# Tranco Top Sites Ranking

Domain popularity ranking from the [Tranco](https://tranco-list.eu/) project — a research-oriented top sites list hardened against manipulation. Combines data from Crux, Farsight, Majestic, Cloudflare Radar, and Cisco Umbrella using Dowdall rule aggregation.

The annotations in this parquet are opinions held by the Tranco project, not by Common Crawl. The data remains the property of the respective ranking providers.

## Usage

```bash
make tranco-top1m          # from project root
```

This resolves the latest Tranco list URL and generates `join_tranco.yaml` with a `table.source` pointing directly at it. No local data download needed — DuckDB streams the CSV at query time.

## Citation

> Victor Le Pochat, Tom Van Goethem, Samaneh Tajalizadehkhoob, Maciej Korczyński, and Wouter Joosen. 2019. "Tranco: A Research-Oriented Top Sites Ranking Hardened Against Manipulation," *Proceedings of the 26th Annual Network and Distributed System Security Symposium (NDSS 2019)*. https://doi.org/10.14722/ndss.2019.23386

Reference the specific list in your work, e.g.:

> We use the Tranco list [1] generated on 18 March 2026, available at https://tranco-list.eu/list/L76X4.

## Columns

| Column | Type | Description |
|--------|------|-------------|
| `rank` | integer | Global popularity rank (1 = most popular) |
| `domain` | string | Registered domain name |

## Example output

```
$ python annotate.py left_host_index.yaml join_tranco.yaml action_top_domains.yaml

surt_host_name,crawl,rank
com,google,CC-MAIN-2024-51,1
net,gtld-servers,CC-MAIN-2024-51,2
com,googleapis,CC-MAIN-2024-51,3
com,microsoft,CC-MAIN-2024-51,4
com,facebook,CC-MAIN-2024-51,5
```

## Sources

- Tranco: https://tranco-list.eu/
- License: derived from Crux (CC BY-SA 4.0), Majestic (CC BY 3.0), Cloudflare Radar (CC BY-NC 4.0)
