import re
import surt
import pandas as pd

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
                                    'in_wikipedia_blacklist': 1
                                })
                            except:
                                pass
    return domains

print("Converting blacklist.txt to parquet...")
domains = extract_domains('blacklist.txt')
df = pd.DataFrame(domains)
df.to_parquet('blacklist.parquet', index=False)
df.to_csv('blacklist.tsv', sep='\t', index=False)
