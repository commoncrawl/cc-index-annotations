This example combines a broad university domain list with CWUR world rankings into a single [Common Crawl Annotation](https://github.com/commoncrawl/cc-index-annotations) file, enabling identification of educational institutions and their relative standing in Common Crawl data.

## Sources

| Source | URL | License | What it covers |
|--------|-----|---------|----------------|
| Hipo University Domains List | https://github.com/Hipo/university-domains-list | MIT | ~10,000 university domains worldwide with country info |
| CWUR World University Rankings 2025 | https://cwur.org/2025.php | Fair use | Top 2000 universities ranked by education, employability, faculty, research |

## Output schema

The resulting `university-ranking.parquet` file has the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `surt_host_name` | string | SURT-formatted hostname (join key) |
| `domain` | string | Original domain |
| `is_university` | bool | True if the domain appears in either source |
| `in_hipo` | bool | Present in Hipo university domains list |
| `in_cwur` | bool | Present in CWUR 2025 rankings |
| `country` | string | Country (from Hipo, empty if CWUR-only) |
| `university_name` | string | Institution name |
| `cwur_world_rank` | int | CWUR world rank (null if unranked) |
| `cwur_national_rank` | int | CWUR national rank (null if unranked) |
| `cwur_education_rank` | int | CWUR education rank (null if unranked) |
| `cwur_employability_rank` | int | CWUR employability rank (null if unranked) |
| `cwur_faculty_rank` | int | CWUR faculty rank (null if unranked) |
| `cwur_research_rank` | int | CWUR research rank (null if unranked) |
| `cwur_score` | float | CWUR overall score 0-100 (null if unranked) |

## Example output

`SELECT university_name, domain, country, cwur_world_rank, cwur_score FROM 'university-ranking.parquet' WHERE in_cwur = true ORDER BY cwur_world_rank LIMIT 10`

| university_name | domain | country | cwur_world_rank | cwur_score |
|---|---|---|---|---|
| Harvard University | harvard.edu | United States | 1 | 100.0 |
| Massachusetts Institute of Technology | mit.edu | United States | 2 | 96.8 |
| Stanford University | stanford.edu | United States | 3 | 95.4 |
| University of Cambridge | cam.ac.uk | United Kingdom | 4 | 94.1 |
| University of Oxford | ox.ac.uk | United Kingdom | 5 | 93.8 |
| Princeton University | princeton.edu | United States | 6 | 93.2 |
| Columbia University | columbia.edu | United States | 7 | 92.5 |
| University of Pennsylvania | upenn.edu | United States | 8 | 92.1 |
| University of Chicago | uchicago.edu | United States | 9 | 91.7 |
| Yale University | yale.edu | United States | 10 | 90.6 |

*(Scores are illustrative — actual values depend on fetch date)*

## Usage

```bash
# Generate the parquet (fetches Hipo JSON + scrapes 2000 CWUR profiles, ~1h with polite delays)
python university-ranking-fetch.py

# Top 25 ranked universities in a crawl
python annotate.py left_host_index.yaml join_university_ranking.yaml action_top_universities.yaml

# Look up a specific university
python annotate.py left_host_index.yaml join_university_ranking.yaml action_surt_host_name.yaml mit.edu

# All university domains in a crawl
python annotate.py left_host_index.yaml join_university_ranking.yaml action_all_universities.yaml
```

## Disclaimer

The rankings in this annotation are **opinions held by the Center for World University Rankings (CWUR)**, not by Common Crawl. The university domain data is maintained by the **Hipo community** and may not be exhaustive. Common Crawl does not endorse, verify, or take responsibility for the accuracy of these classifications or rankings. If you believe an institution has been incorrectly classified or ranked, please contact the relevant source directly.
