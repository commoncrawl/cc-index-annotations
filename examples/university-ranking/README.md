This example identifies university domains in Common Crawl data using the Hipo community's university domain list (~10K domains worldwide).

Optionally, CWUR world rankings can be added for the top 2000 universities.

## Sources

| Source | URL | License | What it covers |
|--------|-----|---------|----------------|
| Hipo University Domains List | https://github.com/Hipo/university-domains-list | MIT | ~10,000 university domains worldwide with country info |
| CWUR World University Rankings 2025 | https://cwur.org/2025.php | copyrighted | Top 2000 universities ranked (optional, see below) |

## Default output (Hipo only)

By default, `university-ranking-fetch.py` fetches only the Hipo domain list. This is fast (a single JSON download) and produces a parquet with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `surt_host_name` | string | SURT-formatted hostname (join key) |
| `domain` | string | Original domain |
| `is_university` | bool | Always true |
| `in_hipo` | bool | Present in Hipo university domains list |
| `country` | string | Country |
| `university_name` | string | Institution name |

## With CWUR rankings (optional)

Pass `--include-cwur` to also scrape CWUR profiles (~1 hour with polite delays). This adds ranking columns to the parquet:

| Column | Type | Description |
|--------|------|-------------|
| `in_cwur` | bool | Present in CWUR 2025 rankings |
| `cwur_world_rank` | int | CWUR world rank (null if unranked) |
| `cwur_national_rank` | int | CWUR national rank (null if unranked) |
| `cwur_education_rank` | int | CWUR education rank (null if unranked) |
| `cwur_employability_rank` | int | CWUR employability rank (null if unranked) |
| `cwur_faculty_rank` | int | CWUR faculty rank (null if unranked) |
| `cwur_research_rank` | int | CWUR research rank (null if unranked) |
| `cwur_score` | float | CWUR overall score 0-100 (null if unranked) |

To use these columns in queries, uncomment them in `join_university_ranking.yaml` and the action YAMLs.

## Example output

`SELECT university_name, domain, country, cwur_world_rank, cwur_score FROM 'university-ranking.parquet' WHERE in_cwur = true ORDER BY cwur_world_rank LIMIT 10`

| university_name | domain | country | cwur_world_rank | cwur_score |
|---|---|---|---|---|
| Harvard University | harvard.edu | United States | 1 | 100.0 |
| Massachusetts Institute of Technology | mit.edu | United States | 2 | 96.8 |
| Stanford University | stanford.edu | United States | 3 | 95.4 |
| University of Cambridge | cam.ac.uk | United Kingdom | 4 | 94.1 |
| University of Oxford | ox.ac.uk | United Kingdom | 5 | 93.8 |

*(Only available after running with `--include-cwur`. Scores depend on fetch date.)*

## Usage

```bash
# Default: Hipo university domains only (fast)
python university-ranking-fetch.py

# With CWUR rankings (slow, ~1h with polite delays)
python university-ranking-fetch.py --include-cwur

# All university domains in a crawl
python annotate.py left_host_index.yaml join_university_ranking.yaml action_all_universities.yaml

# Top 25 ranked universities in a crawl (requires --include-cwur)
python annotate.py left_host_index.yaml join_university_ranking.yaml action_top_universities.yaml

# Look up a specific university
python annotate.py left_host_index.yaml join_university_ranking.yaml action_surt_host_name.yaml mit.edu
```

## Disclaimer

The rankings in this annotation are **opinions held by the Center for World University Rankings (CWUR)**, not by Common Crawl. The university domain data is maintained by the **Hipo community** and may not be exhaustive. Common Crawl does not endorse, verify, or take responsibility for the accuracy of these classifications or rankings. If you believe an institution has been incorrectly classified or ranked, please contact the relevant source directly.
