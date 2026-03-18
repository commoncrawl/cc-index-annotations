This example combines multiple public spam, abuse, malware, and phishing domain lists into a single [Common Crawl Annotation](https://github.com/commoncrawl/cc-index-annotations) file.

## Sources

| Source | URL | License | What it covers |
|--------|-----|---------|----------------|
| URLhaus (abuse.ch) | https://urlhaus.abuse.ch/ | CC0 | Malware distribution URLs |
| PhishTank | https://www.phishtank.com/ | Free (API key optional) | Verified phishing URLs |
| UT1 Blacklists | https://github.com/olbat/ut1-blacklists | CC BY-SA | Categorized domain lists (malware, phishing, ddos, cryptojacking) |

Note: OpenPhish was considered but excluded because their robots.txt disallows automated access to the community feed. Feodo Tracker was excluded because their datasets are currently empty and IP-only (no domain data).

## Output schema

The resulting `spam-abuse.parquet` file has the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `surt_host_name` | string | SURT-formatted hostname (join key) |
| `domain` | string | Original domain |
| `abuse_urlhaus_malware` | bool | Domain seen distributing malware (URLhaus) |
| `abuse_phishtank_phishing` | bool | Domain seen in phishing (PhishTank) |
| `abuse_ut1_malware` | bool | Categorized as malware (UT1) |
| `abuse_ut1_phishing` | bool | Categorized as phishing (UT1) |
| `abuse_ut1_ddos` | bool | Categorized as DDoS (UT1) |
| `abuse_ut1_cryptojacking` | bool | Categorized as cryptojacking (UT1) |

## Example output

`SELECT * FROM 'spam-abuse.parquet' WHERE surt_host_name LIKE '%blogspot%' LIMIT 10`

| surt_host_name | domain | abuse_phishtank_phishing | abuse_urlhaus_malware | abuse_ut1_cryptojacking | abuse_ut1_ddos | abuse_ut1_malware | abuse_ut1_phishing |
|---|---|---|---|---|---|---|---|
| ae,blogspot,1p5mhny | 1p5mhny.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,7nhua | 7nhua.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,bgtews | bgtews.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,chhhpyilmazyildirir | chhhpyilmazyildirir.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,claimmvisitmybio | claimmvisitmybio.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,dasdase3224 | dasdase3224.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,dgwfdt | dgwfdt.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,edestryhgfhdfsfafrsyhftgjvghhndfgsg | edestryhgfhdfsfafrsyhftgjvghhndfgsg.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,fdjdhsgjfhgsdfjkhjasdm | fdjdhsgjfhgsdfjkhjasdm.blogspot.ae | False | False | False | False | True | True |
| ae,blogspot,fflink194xyz | fflink194xyz.blogspot.ae | False | False | False | False | True | True |

## Usage

```bash
# Generate the parquet (fetches and caches all sources)
python spam-abuse-fetch.py

# Query malware domains in a crawl
python annotate.py left_host_index.yaml join_spam_abuse.yaml action_malware.yaml

# Query phishing domains in a crawl
python annotate.py left_host_index.yaml join_spam_abuse.yaml action_phishing.yaml
```

## Disclaimer

The classifications in this annotation are **opinions held by the respective third-party sources**, not by Common Crawl. The data remains the property of its original providers and is subject to their respective licenses (see table above). Common Crawl does not endorse, verify, or take responsibility for the accuracy of these classifications. If you believe a domain has been incorrectly classified, please contact the relevant source directly.
