# FineWeb-Edu Educational Quality Annotations

Domain-level educational quality scores derived from [FineWeb-Edu](https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu), a dataset of 500K web pages scored by Llama3-70B-Instruct on a 0-5 educational quality rubric.

The annotations in this file are opinions produced by HuggingFace's FineWeb team using Llama3-70B-Instruct, not by Common Crawl. The data remains the property of the original creators.

## Columns

| Column | Description |
|--------|-------------|
| `surt_host_name` | SURT-encoded hostname (join key) |
| `domain` | Original domain |
| `fineweb_edu_pages` | Number of pages scored from this domain |
| `fineweb_edu_avg_score` | Average educational score (0.0-5.0) |
| `fineweb_edu_max_score` | Highest score seen on this domain |
| `fineweb_edu_avg_lang_score` | Average language identification confidence |

## Score rubric (from HuggingFace)

| Score | Meaning |
|-------|---------|
| 0 | No educational value |
| 1 | Some basic info, may include ads/promo |
| 2 | Some educational elements, disorganized |
| 3 | Appropriate for educational use, key concepts |
| 4 | Highly relevant, clear writing, textbook-like |
| 5 | Outstanding, perfectly suited for teaching |

## Usage

```bash
# optional: set a HuggingFace token for higher rate limits
export HF_TOKEN=hf_your_token_here

make fineweb-edu               # fetches ~1.7GB from HuggingFace, aggregates to domain-level

cd examples/fineweb-edu

# Top educational domains
python annotate.py left_host_index.yaml join_fineweb_edu.yaml action_top_educational.yaml

# Lookup a specific domain
python annotate.py left_host_index.yaml join_fineweb_edu.yaml action_surt_host_name.yaml khanacademy.org
```

## Attribution

**FineWeb-Edu** by HuggingFace FineData team.

> Lozhkov, Ben Allal, von Werra et al. "FineWeb-Edu" (2024).
> https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu

**License**: Open Data Commons Attribution License (ODC-By) v1.0, subject to [Common Crawl Terms of Use](https://commoncrawl.org/terms-of-use).

**Scoring model**: Meta Llama3-70B-Instruct, used under the [Meta Llama 3 Community License](https://llama.meta.com/llama3/license/).

**Source data**: [FineWeb-Edu Llama3 Annotations](https://huggingface.co/datasets/HuggingFaceFW/fineweb-edu-llama3-annotations) on HuggingFace.
