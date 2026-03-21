# Wikipedia Annotations

Annotations derived from Wikipedia and Wikidata covering source reliability, spam classification, and website categorization.

All data reflects the opinions of Wikipedia editors and contributors, not Common Crawl. Content licensed under [CC BY-SA 4.0](https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License) (Wikipedia) and [CC0](https://www.wikidata.org/wiki/Wikidata:Licensing) (Wikidata).

## Subdirectories

| Directory | Description | Domains |
|-----------|-------------|---------|
| `perennial/` | Source reliability ratings from English Wikipedia's Reliable Sources/Perennial list + international equivalents | ~1000+ |
| `spam/` | Spam and URL shortener flags from English Wikipedia's spam blacklist | ~30K+ |
| `categories/` | Website classification from English Wikipedia article categories (fact-checking, fake news, satirical, holocaust denial, alt-right, disinformation) | ~159 |
| `categories-intl/` | Same as categories but across all language Wikipedias via langlinks auto-discovery | ~500-2000 |

## International perennial sources

The perennial scraper supports multiple languages via `languages.yaml`. Wikis with structured source assessment lists:

| Language | Wiki | Key page(s) |
|----------|------|-------------|
| English (en) | en.wikipedia.org | Wikipedia:Reliable sources/Perennial sources |
| French (fr) | fr.wikipedia.org | Wikipedia:Observatoire des sources |
| Chinese (zh) | zh.wikipedia.org | Wikipedia:可靠来源/常见有争议来源列表 |
| Russian (ru) | ru.wikipedia.org | Wikipedia:Часто используемые источники |
| Vietnamese (vi) | vi.wikipedia.org | Wikipedia:Danh sách nguồn đáng tin cậy |
| Turkish (tr) | tr.wikipedia.org | Vikipedi:Güvenilir kaynaklar/Mütemadi kaynaklar |
| Ukrainian (uk) | uk.wikipedia.org | Вікіпедія:Список оцінених джерел |
| Indonesian (id) | id.wikipedia.org | Wikipedia:Sumber tepercaya/Observatorium sumber/Daftar |
| Korean (ko) | ko.wikipedia.org | 위키백과:신뢰할 수 있는 출처 목록 |
| Swedish (sv) | sv.wikipedia.org | Wikipedia:Trovärdiga källor/Bedömningar |
| Portuguese (pt) | pt.wikipedia.org | Wikipédia:Fontes não confiáveis/Lista |
