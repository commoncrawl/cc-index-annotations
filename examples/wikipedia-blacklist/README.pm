This example uses an interpreted version of the [wikipedia blacklist](https://meta.wikimedia.org/wiki/Spam_blacklist) 
This blacklist contains pattern matches for domains you are not allowed to link to from wikipedia. 

The convert.py script takes the regexes from the blacklist and expands these to likely entries, 
for instance turning the regular expression `\bshortenlinks\.(?:com|org)\b` into both the domains `shortenlinks.org` and `shortenlinks.com`

The resulting list of domains is therefore only an approximation of the possible list of domains the regexes could match. 

Where possible we've tried to add sane defaults, but we cannot guarantee that no false positives or negatives, a regex with wildcards will always match more than these expansions



