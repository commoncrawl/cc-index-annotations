# NewsGuard — News Source Credibility Ratings

Host-level annotation with trust ratings, credibility criteria, and
misinformation flags from [NewsGuard](https://www.newsguardtech.com/),
which rates thousands of news and information sites on a 0–100 scale
across nine journalistic criteria.

## What's in this annotation

| Column | Type | Description |
|--------|------|-------------|
| `newsguard_rating` | string | Overall rating: T (trustworthy), N (not trustworthy), S (satire), P (platform), FL (flagged), C (baseline credible), L (low risk), A (academic), G (local government) |
| `newsguard_score` | float | Trust score 0–100 (null for S, P, FL, L, A, G categories) |
| `newsguard_orientation` | string | Political orientation: Left, Right, or N/A |
| `newsguard_criteria_*` | bool | Nine credibility and transparency criteria (see below) |
| `newsguard_flag_*` | bool | Misinformation and provenance flags (see below) |
| `newsguard_topics` | string | Comma-separated topic labels |
| `newsguard_targeted_audience` | string | Local, Regional, National, or International |

### Criteria columns

| Column | Points | Meaning (true = site passes) |
|--------|--------|------------------------------|
| `newsguard_criteria_no_false_content` | 22 | Does not repeatedly publish false content |
| `newsguard_criteria_responsible_reporting` | 18 | Gathers and presents information responsibly |
| `newsguard_criteria_error_correction` | 12.5 | Has effective practices for correcting errors |
| `newsguard_criteria_news_opinion_separation` | 12.5 | Handles the difference between news and opinion responsibly |
| `newsguard_criteria_no_deceptive_headlines` | 10 | Avoids deceptive headlines |
| `newsguard_criteria_ownership_disclosure` | 7.5 | Website discloses ownership and financing |
| `newsguard_criteria_ad_labeling` | 7.5 | Clearly labels advertising |
| `newsguard_criteria_management_disclosure` | 5 | Reveals who's in charge |
| `newsguard_criteria_content_creator_info` | 5 | Provides names of content creators with contact/bio info |

### Flag columns

| Column | Meaning |
|--------|---------|
| `newsguard_flag_covid` | Published COVID-19 misinformation |
| `newsguard_flag_hlth` | Published health misinformation |
| `newsguard_flag_vacc` | Published vaccine misinformation |
| `newsguard_flag_elec` | Published election/voting misinformation |
| `newsguard_flag_ukraine` | Published Russia-Ukraine misinformation |
| `newsguard_flag_climate` | Published climate misinformation |
| `newsguard_flag_israelhamas` | Published Israel-Hamas war misinformation |
| `newsguard_flag_politics` | Published political misinformation |
| `newsguard_flag_abortion` | Published abortion misinformation |
| `newsguard_flag_qannon` | Published QAnon conspiracy content |
| `newsguard_flag_state` | State-controlled news outlet |
| `newsguard_flag_stateinfluenced` | State-influenced |
| `newsguard_flag_russiastate` | Russia state-controlled or influenced |
| `newsguard_flag_chinastate` | China state-controlled or influenced |
| `newsguard_flag_iranstate` | Iran state-controlled or influenced |
| `newsguard_flag_hideparty` | Hidden partisan funding or ownership |
| `newsguard_flag_partisanlocal` | Secretly partisan local news |
| `newsguard_flag_plag` | Regularly publishes plagiarized content |
| `newsguard_flag_hoax` | Imposter site mimicking a credible brand |
| `newsguard_flag_dead` | Site no longer publishes or has been taken down |
| `newsguard_flag_uain` | Unreliable AI-generated news |

## Data source

NewsGuard's metadata feed is available under license from
[NewsGuard Technologies](https://www.newsguardtech.com/).
The sample files included here (`metadata_sample.csv` and
`label_sample.json`) are public examples provided by NewsGuard
for demonstration purposes.

The metadata CSV contains ratings, scores, criteria, flags, and
descriptive metadata for each site. The label JSON contains
"Nutrition Label" writeups — detailed editorial assessments
explaining the reasoning behind each rating.

## Preparing the annotation

Convert the metadata CSV to a parquet file sorted by SURT host name:

```bash
python newsguard-convert.py metadata_sample.csv newsguard.parquet
```

For production use with the full NewsGuard data feed:

```bash
python newsguard-convert.py /path/to/full/newsguard_metadata.csv newsguard.parquet
```

The script filters out `Country=ALL` rows (English-language duplicates)
and converts domains to SURT format for joining against the host index.

## Quick start

From the project root, fetch the host index paths file:

```bash
make newsguard
```

Then run a query — look up a specific site:

```bash
cd examples/newsguard
python annotate.py left_web_host_index.yaml join_newsguard.yaml \
    action_surt_host_name.yaml c-span.org
```

Find all sites flagged for misinformation:

```bash
python annotate.py left_web_host_index.yaml join_newsguard.yaml \
    action_flagged_misinfo.yaml
```

Find all trustworthy sites in the crawl:

```bash
python annotate.py left_web_host_index.yaml join_newsguard.yaml \
    action_trustworthy.yaml
```

Find state-controlled or state-influenced outlets:

```bash
python annotate.py left_web_host_index.yaml join_newsguard.yaml \
    action_state_media.yaml
```

Use S3 instead of HTTPS (faster, requires AWS credentials):

```bash
python annotate.py left_s3_host_index.yaml join_newsguard.yaml \
    action_surt_host_name.yaml bbc.co.uk
```

## Files in this directory

| File | Purpose |
|------|---------|
| `metadata_sample.csv` | Sample NewsGuard metadata feed (27 sites, July 2024) |
| `label_sample.json` | Sample NewsGuard Nutrition Label feed (8 sites, Dec 2022) |
| `newsguard-convert.py` | Converts the metadata CSV → parquet annotation |
| `newsguard.parquet` | The annotation parquet (built from the sample) |
| `join_newsguard.yaml` | Join config — all columns |
| `join_newsguard_lite.yaml` | Join config — rating, score, and key flags only |
| `left_web_host_index.yaml` | CC host index via HTTPS |
| `left_s3_host_index.yaml` | CC host index via S3 |
| `action_surt_host_name.yaml` | Look up a specific domain |
| `action_flagged_misinfo.yaml` | Find misinformation-flagged sites |
| `action_trustworthy.yaml` | Find trustworthy sites |
| `action_state_media.yaml` | Find state-controlled/influenced outlets |

## Multi-join example

Combine NewsGuard ratings with web graph metrics:

```bash
cd examples/newsguard
python annotate.py left_web_host_index.yaml \
    join_newsguard_lite.yaml \
    ../web-graph/join_web_outin.yaml \
    action_surt_host_name.yaml nytimes.com
```

## Attribution

The ratings and classifications are the assessments of
[NewsGuard Technologies, Inc.](https://www.newsguardtech.com/),
not of Common Crawl. Questions about ratings should be directed to
NewsGuard.
