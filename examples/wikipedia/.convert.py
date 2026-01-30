#!/usr/bin/env python3
import json
import re
from urllib.parse import urlparse
from html.parser import HTMLParser
import urllib.request
import surt
import pandas as pd

debugging = False #when enabled, save .tsv files of intermediary and final stages

class SourceParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.sources = []
        self.in_table = False
        self.in_row = False
        self.in_first_td = False
        self.in_last_td = False
        self.current_row = {}
        self.current_col = 0
        self.cell_text = []
        
    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        
        if tag == "table" and "perennial-sources" in attrs_dict.get("class", ""):
            self.in_table = True
        elif tag == "tr" and self.in_table:
            row_class = attrs_dict.get("class", "")
            if row_class.startswith("s-"):
                self.in_row = True
                self.current_row = {"domains": [], "row_id": attrs_dict.get("id", "")}
                self.current_col = 0
                if "s-d" in row_class:
                    self.current_row["status"] = "deprecated"
                elif "s-gu" in row_class:
                    self.current_row["status"] = "generally_unreliable"
                elif "s-nr" in row_class:
                    self.current_row["status"] = "no_consensus"
                elif "s-gr" in row_class:
                    self.current_row["status"] = "generally_reliable"
        elif tag == "td" and self.in_row:
            if self.current_col == 0:
                self.in_first_td = True
                self.cell_text = []
            elif self.current_col == 5:
                self.in_last_td = True
        elif tag == "a":
            href = attrs_dict.get("href", "")
            if self.in_last_td and href.startswith("http") and "wikipedia.org" not in href and "wikimedia.org" not in href:
                domain = urlparse(href).netloc
                if domain:
                    self.current_row["domains"].append(domain)
            
    def handle_data(self, data):
        if self.in_first_td:
            text = data.strip()
            if text and text not in ["WP:", "📌"]:
                self.cell_text.append(text)
    
    def handle_endtag(self, tag):
        if tag == "table":
            self.in_table = False
        elif tag == "tr" and self.in_row:
            self.in_row = False
            if self.current_row.get("status"):
                self.sources.append(self.current_row)
        elif tag == "td":
            if self.in_first_td:
                self.in_first_td = False
                name = " ".join(self.cell_text)
                name = re.sub(r'\s*\([^)]*\)\s*$', '', name)
                self.current_row["name"] = name.strip()
            elif self.in_last_td:
                self.in_last_td = False
            self.current_col += 1

def extract_domains_from_linkchecker(html, sources):
    pattern = r'<a[^>]+href="https://spamcheck\.toolforge\.org/by-domain\?q=([^"]+)"'
    for source in sources:
        row_id = source.get("row_id", "")
        if row_id:
            start = html.find(f'id="{row_id}"')
            if start != -1:
                chunk = html[start:start + 5000]
                domains = re.findall(pattern, chunk)
                source["domains"].extend(domains)

def fetch_and_parse(filepath):
    #with open("wikipedia-perennial.json.txt", "r", encoding="utf-8") as f:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

    html = data["parse"]["text"]["*"]

    parser = SourceParser()
    parser.feed(html)

    extract_domains_from_linkchecker(html, parser.sources)

    for s in parser.sources:
        s["domains"] = [
            d for d in s["domains"]
            if "spamcheck.toolforge.org" not in d and "wikipedia.org" not in d
        ]

    return parser.sources

#def fetch_and_parse():
#    url = "https://en.wikipedia.org/w/api.php?action=parse&page=Wikipedia:Reliable_sources/Perennial_sources&format=json"
#    req = urllib.request.Request(url, headers={
#        "User-Agent": "cc-index-annotations/1.0 (your@email)"
#    })
#    with urllib.request.urlopen(req, timeout=30) as response:
#        data = json.loads(response.read())
#    
#    html = data["parse"]["text"]["*"]
#    
#    parser = SourceParser()
#    parser.feed(html)
#    
#    extract_domains_from_linkchecker(html, parser.sources)
#    
#    for source in parser.sources:
#        source["domains"] = [d for d in source["domains"] if "spamcheck.toolforge.org" not in d and "wikipedia.org" not in d]
#    
#    return parser.sources

def extract_perennials(filepath):
    sources = fetch_and_parse(filepath)
    
    rows = []
    for source in sources:
        for domain in source["domains"]:
            url = f"http://{domain}"
            full_surt = surt.surt(url)
            surt_host = full_surt.split(')/')[0]

            rows.append({
                "surt_host_name": surt_host,
                "domain": domain,
                "wikipedia_deprecated": source["status"] == "deprecated",
                "wikipedia_unreliable": source["status"] == "generally_unreliable",
                "wikipedia_reliable": source["status"] == "generally_reliable"
            })
    
    return rows


def expand_chars(s):
    pattern = r'(\w)\?'
    
    if not re.search(pattern, s):
        return [s]
    
    match = re.search(pattern, s)
    with_char = s[:match.start()] + match.group(1) + s[match.end():]
    without_char = s[:match.start()] + s[match.end():]
    
    return expand_chars(with_char) + expand_chars(without_char)

def expand_optionals(s):
    pattern = r'\(([^()]+)\)\?'
    
    if not re.search(pattern, s):
        return [s]
    
    match = re.search(pattern, s)
    with_opt = s[:match.start()] + match.group(1) + s[match.end():]
    without_opt = s[:match.start()] + s[match.end():]
    
    return expand_optionals(with_opt) + expand_optionals(without_opt)

def expand_alternations(s):
    pattern = r'\((?:\?:)?([^()]+\|[^()]+)\)'
    
    if not re.search(pattern, s):
        return [s]
    
    match = re.search(pattern, s)
    options = match.group(1).split('|')
    results = []
    for opt in options:
        results.append(s[:match.start()] + opt + s[match.end():])
    
    expanded = []
    for r in results:
        expanded.extend(expand_alternations(r))
    return expanded


def normalize_domain(regex):
    d = regex.strip()
    d = re.sub(r'^\\b', '', d)
    d = re.sub(r'\\b$', '', d)
    d = re.sub(r'\\.', '.', d)
    d = re.sub(r'^\^', '', d)
    d = re.sub(r'\$$', '', d)
    d = re.sub(r'\.\+', '', d)
    d = re.sub(r'\.\*', '', d)
    d = re.sub(r'\\d\{\d+,?\d*\}', '', d)
    d = re.sub(r'\(\.\{\d+,?\d*\}\)', '', d)
    d = re.sub(r'\{\d+,?\d*\}', '', d)
    d = re.sub(r'\[[\w\-]+\]\*', '', d)
    d = re.sub(r'\[[\w\-]+\]\+', '', d)
    d = re.sub(r'\[[\w\-]+\]', '', d)
    d = re.sub(r'\(\?![^)]+\)', '', d)  # remove negative lookaheads
    d = re.sub(r'\?', '', d)
    d = re.sub(r'\\d\+', '', d)
    d = re.sub(r'\\d', '', d)
    d = re.sub(r'\\w\+', '', d)
    d = re.sub(r'\\w\?', '', d)
    d = re.sub(r'-{2,}', '-', d)
    d = re.sub(r'\.{2,}', '.', d)
    #d = re.sub(r'\/.*$', '', d)
    d = re.sub(r'/.*$', '', d)
    d = re.sub(r'\.$', '', d) 
    return d

def extract_domains(file_path):
    domains = []
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            domain_regex = line.split('#')[0].strip()
            if not domain_regex:
                continue
            
            if "/" in domain_regex:
                continue

            for char_expanded in expand_chars(domain_regex):
                for opt_expanded in expand_optionals(char_expanded):
                    for expanded in expand_alternations(opt_expanded):
                        domain = normalize_domain(expanded)
                        if not domain or '(' in domain or '|' in domain or '{' in domain or '\\' in domain or '[' in domain:
                            continue
                        
                        
                        domains_to_process = []
                        # if '.' not in domain:  # If no TLD, add common ones
                        if domain.startswith('.') and domain.count('.') == 1: #skip tld matches
                            continue
                        if '.' not in domain or domain.startswith('.'):
                            for tld in ['com', 'net', 'org', 'info']:
                                domains_to_process.append(f"{domain}.{tld}")
                        else:
                            domains_to_process.append(domain)
                        
                        for d in domains_to_process:
                            #if d.startswith('-') or d.startswith('.-'): # we dont want nonsensical domains that start with hyphen
                            if d.startswith('-') or d.startswith('.-') or d.endswith('-') or '.-' in d or '-.' in d:
                                continue
                            try:
                                full_surt = surt.surt(f"http://{d}")
                                surt_host = full_surt.split(')/')[0]
                                domains.append({
                                    'surt_host_name': surt_host,
                                    'domain': d,
                                    'domain_regex': domain_regex,
                                    'wikipedia_spam': True
                                })
                            except:
                                pass
    return domains

if __name__ == "__main__":
    print("Extracting wikipedia perennials...")
    rows = extract_perennials("wikipedia-perennial.json.txt")
    per_df = pd.DataFrame(rows)
    per_df = per_df.drop_duplicates(subset=["domain"])
    per_df = per_df.sort_values("surt_host_name").reset_index(drop=True)
    
    #per_df.to_parquet("wikipedia-perennial.parquet", index=False)
    if debugging:
        per_df.to_csv("wikipedia-perennial.tsv", sep="\t", index=False)
    
    print("Expanding wikipedia-spam.txt domains...")

    domains = extract_domains('wikipedia-spam.txt')
    spam_df = pd.DataFrame(domains)
    #spam_df.to_parquet('wikipedia-spam.parquet', index=False)

    if debugging:
        spam_df.to_csv('wikipedia-spam.tsv', sep='\t', index=False)

    print("Writing to wikipedia-domains.parquet...")

    per_df = per_df.drop_duplicates(subset=["surt_host_name"])
    spam_df = spam_df.drop_duplicates(subset=["surt_host_name"])

    df = per_df.merge(
        spam_df[["surt_host_name", "domain", "wikipedia_spam"]],  # Include domain in merge
        on="surt_host_name",
        how="outer",
        suffixes=('', '_spam')
    )
    
    df["domain"] = df["domain"].combine_first(df["domain_spam"])
    df = df.drop(columns=["domain_spam"], errors='ignore')

    df = df[[
        "surt_host_name",
        "domain",
        "wikipedia_deprecated",
        "wikipedia_unreliable",
        "wikipedia_reliable",
        "wikipedia_spam",
    ]]

    bool_cols = ["wikipedia_deprecated", "wikipedia_unreliable", "wikipedia_reliable", "wikipedia_spam"]

    for col in bool_cols:
        df[col] = df[col].astype('boolean').fillna(False).astype(bool)

    df = df.sort_values("surt_host_name").reset_index(drop=True)

    df.to_parquet('wikipedia-domains.parquet', index=False)

    if debugging:
        df.to_csv('wikipedia-domains.tsv', sep='\t', index=False)
