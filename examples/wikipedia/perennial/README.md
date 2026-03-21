# Wikipedia Perennial Sources — international

Source reliability assessments from Wikipedia editors across multiple languages, extracted via MediaWiki API.

The ratings in this dataset are the opinions of Wikipedia editors in each language community, not Common Crawl.

## Coverage

| Language | Wiki | Domains | Key page |
|----------|------|---------|----------|
| French | fr.wikipedia.org | ~224 | Observatoire des sources |
| Chinese | zh.wikipedia.org | ~545 | 可靠来源/常见有争议来源列表 |
| Russian | ru.wikipedia.org | ~121 | Часто используемые источники + Нежелательные источники |
| Swedish | sv.wikipedia.org | ~95 | Trovärdiga källor/Bedömningar |
| Portuguese | pt.wikipedia.org | ~169 | Fontes não confiáveis/Lista + Lista de fontes confiáveis |
| Vietnamese | vi.wikipedia.org | ~29 | Danh sách nguồn đáng tin cậy |
| Indonesian | id.wikipedia.org | ~29 | Sumber tepercaya/Observatorium sumber/Daftar |
| Ukrainian | uk.wikipedia.org | ~25 | Список оцінених джерел |
| Turkish | tr.wikipedia.org | ~9 | Güvenilir kaynaklar/Mütemadi kaynaklar |
| Korean | ko.wikipedia.org | ~4 | 신뢰할 수 있는 출처 목록 |

English (en) is handled separately by the existing `wp_sources_scraper.py` in the parent `perennial/` directory (copied from the original `wikipedia-perennial` example).

## Usage

```bash
python wikipedia-perennial-fetch.py           # fetch all languages
python wikipedia-perennial-fetch.py --lang=fr  # fetch French only
python wikipedia-perennial-fetch.py -d         # also write CSV

python annotate.py left_host_index.yaml join_wikipedia_perennial.yaml action_star.yaml
```

## Configuration

All language-specific settings are in `languages.yaml`. To add a new language, add an entry with:
- `wiki`: the wiki hostname
- `pages`: list of page titles to fetch
- `parser`: `source_ods` (French template) or `wikitext_table` (generic)

## Data source

- Wikipedia content: [CC BY-SA 4.0](https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License)
