# External Data Sources

Pre-built YAML files that join external datasets against Common Crawl's host index using `table.source` — no local data downloads needed. DuckDB streams each dataset directly at query time.

The annotations in these files are opinions held by their respective providers, not by Common Crawl. The data remains the property of the original sources.

## Available datasets

| YAML | Source | Domains | License |
|------|--------|---------|---------|
| `join_tranco.yaml` | [Tranco](https://tranco-list.eu/) top sites ranking | ~5.4M | CC BY-SA/BY-NC 4.0 (mixed) |
| `join_majestic_million.yaml` | [Majestic](https://majestic.com/) top 1M by referring subnets | 1M | CC BY 3.0 |
| `join_cisa_gov_domains.yaml` | [CISA](https://github.com/cisagov/dotgov-data) US .gov domain registry | ~12.6K | Public domain |
| `join_gsa_nongov_federal.yaml` | [GSA](https://github.com/GSA/govt-urls) US federal non-.gov domains | ~400 | Public domain |
| `join_ifcn_factcheckers.yaml` | [IFCN](https://github.com/IFCN/verified-signatories) verified fact-checkers | ~167 | Public |
| `join_misinfo_domains.yaml` | [Lasser et al.](https://github.com/JanaLasser/misinformation_domains) misinformation domains | ~4.8K | CC BY-SA 4.0 |

## Usage

```bash
make external-data            # from project root (fetches host-index-paths.gz)

cd examples/external-data

# Query with any single dataset
python annotate.py left_host_index.yaml join_tranco.yaml action_star.yaml

# Stack multiple datasets in one query
python annotate.py left_host_index.yaml join_*.yaml action_star.yaml
```

`action_star.yaml` returns all columns (`SELECT *`) with a default limit of 100 rows.

## Adding your own

Drop a YAML file here with a `table.source` block pointing at any public CSV, JSON, or parquet URL. See [docs/yaml-reference.md](../../docs/yaml-reference.md) for the full spec.

## Citations

**Tranco**: Le Pochat, Van Goethem, Tajalizadehkhoob, Korczyński, and Joosen. "Tranco: A Research-Oriented Top Sites Ranking Hardened Against Manipulation," NDSS 2019. https://doi.org/10.14722/ndss.2019.23386

**Majestic Million**: https://majestic.com/reports/majestic-million — CC BY 3.0

**CISA .gov Data**: https://github.com/cisagov/dotgov-data — US government public domain

**GSA Non-.gov URLs**: https://github.com/GSA/govt-urls — US government public domain

**IFCN Verified Signatories**: https://github.com/IFCN/verified-signatories — International Fact-Checking Network

**Misinformation Domains**: Lasser et al. https://github.com/JanaLasser/misinformation_domains — CC BY-SA 4.0
