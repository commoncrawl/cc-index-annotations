import re
import surt
import pandas as pd

debugging = True  #when enabled, save .tsv files of intermediary and final stages

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

def _process_domain_line(line, is_shortener=False):
    results = []
    domain_regex = line.split('#')[0].strip()
    if not domain_regex:
        return results

    if "/" in domain_regex:
        return results

    for char_expanded in expand_chars(domain_regex):
        for opt_expanded in expand_optionals(char_expanded):
            for expanded in expand_alternations(opt_expanded):
                domain = normalize_domain(expanded)
                if not domain or '(' in domain or '|' in domain or '{' in domain or '\\' in domain or '[' in domain:
                    continue

                domains_to_process = []
                if domain.startswith('.') and domain.count('.') == 1:
                    continue
                if '.' not in domain:
                    for tld in ['com', 'net', 'org', 'info']:
                        domains_to_process.append(f"{domain}.{tld}")
                elif domain.startswith('.'):
                    domains_to_process.append(domain.lstrip('.'))
                else:
                    domains_to_process.append(domain)

                for d in domains_to_process:
                    if d.startswith('-') or d.startswith('.-') or d.endswith('-') or '.-' in d or '-.' in d:
                        continue
                    try:
                        full_surt = surt.surt(f"http://{d}")
                        surt_host = full_surt.split(')/')[0]
                        entry = {
                            'surt_host_name': surt_host,
                            'domain': d,
                            'domain_regex': domain_regex,
                            'wikipedia_spam': True,
                            'wikipedia_shortener': is_shortener,
                        }
                        results.append(entry)
                    except:
                        pass
    return results

def extract_domains(file_path):
    domains = []
    shortener_section = False
    found_shortener_start = False
    found_shortener_end = False

    with open(file_path) as f:
        for line in f:
            line = line.strip()

            if line == "# URL shorteners":
                shortener_section = True
                found_shortener_start = True
                continue
            if line == "# end of URL shorteners":
                shortener_section = False
                found_shortener_end = True
                continue

            if not line or line.startswith('#'):
                continue
            
            domains.extend(_process_domain_line(line, is_shortener=shortener_section))

    if not found_shortener_start or not found_shortener_end:
        raise RuntimeError(
            "Could not find URL shortener section delimiters in "
            f"{file_path}. Expected '# URL shorteners' and '# end of URL shorteners'."
        )

    return domains

print("Converting wikipedia-spam.txt to parquet...")
domains = extract_domains('wikipedia-spam.txt')
df = pd.DataFrame(domains)

bool_cols = ["wikipedia_spam", "wikipedia_shortener"]
for col in bool_cols:
    df[col] = df[col].astype('boolean').fillna(False).astype(bool)

df = df.sort_values("surt_host_name").reset_index(drop=True)

df.to_parquet('wikipedia-spam.parquet', index=False)
if debugging:
    df.to_csv('wikipedia-spam.tsv', sep='\t', index=False)

