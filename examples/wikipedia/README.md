This example uses an interpreted version of the [wikipedia spam domain list](https://meta.wikimedia.org/wiki/Spam_blacklist) and [wikipedia perennial] to generate an [Common Crawl Annotation](https://github.com/commoncrawl/cc-index-annotations)

Please note that the entries are directly copied from wikipedia.com and are not under our control, please see [wikipedia:Perennial_sources](https://en.wikipedia.org/wiki/Wikipedia:Reliable_sources/Perennial_sources) and [wikipedia:Spam_blacklist](https://meta.wikimedia.org/wiki/Spam_blacklist) for more information.

The convert.py script takes the regexes from the blacklist and expands these to likely entries, 
for instance turning the regular expression `\bshortenlinks\.(?:com|org)\b` into both the domains `shortenlinks.org` and `shortenlinks.com`.

The resulting list of domains is therefore only an approximation of the possible list of domains the regexes could match,  

where possible we've tried to add sane defaults, but we cannot guarantee that no false positives or negatives, a regex with wildcards will always match more than these expansions


the resulting `wikipedia-domains.parquet` file consists of the following columns:

| surt_host_name |   domain   | wikipedia_deprecated | wikipedia_unreliable | wikipedia_reliable | wikipedia_spam |
|----------------|------------|----------------------|----------------------|--------------------|----------------|
[ com,example    | example.com| False                | True                 | True               | False          |
`[...]`




