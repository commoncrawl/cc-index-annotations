#!/usr/bin/env python3
"""
Create a public version of the combined parquet by excluding columns
from proprietary/restricted sources.

Usage:
    python make_public.py combined.parquet combined_public.parquet
    python make_public.py combined.parquet combined_public.parquet --also-exclude majestic,ifcn
"""

import argparse
import os
import sys

import duckdb

# Columns matching these prefixes or substrings are excluded
EXCLUDED_PATTERNS = [
    "phishtank",     # PhishTank — restrictive feed terms
    "urlhaus",       # abuse.ch — Fair Use, no derivative works (§7.3)
    "cwur",          # CWUR — © all rights reserved
    "tranco",        # Tranco — contains CC BY-NC (Cloudflare Radar)
]


def main():
    p = argparse.ArgumentParser(
        description="Create a public parquet by excluding restricted columns.",
    )
    p.add_argument("input", help="Input combined parquet")
    p.add_argument("output", help="Output public parquet")
    p.add_argument(
        "--also-exclude",
        help="Additional comma-separated patterns to exclude "
             "(matched case-insensitively against column names)",
    )
    args = p.parse_args()

    patterns = list(EXCLUDED_PATTERNS)
    if args.also_exclude:
        patterns.extend(args.also_exclude.split(","))

    con = duckdb.connect()
    cols = [
        row[0]
        for row in con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{args.input}')"
        ).fetchall()
    ]

    kept = []
    dropped = []
    for c in cols:
        cl = c.lower()
        if any(p.lower() in cl for p in patterns):
            dropped.append(c)
        else:
            kept.append(c)

    print(f"Input:   {len(cols)} columns", file=sys.stderr)
    print(f"Dropped: {len(dropped)} columns", file=sys.stderr)
    for c in dropped:
        print(f"  - {c}", file=sys.stderr)
    print(f"Kept:    {len(kept)} columns", file=sys.stderr)

    select = ", ".join(f'"{c}"' for c in kept)
    con.execute(
        f"COPY (SELECT {select} FROM read_parquet('{args.input}')) "
        f"TO '{args.output}' (FORMAT PARQUET)"
    )

    n = con.execute(
        f"SELECT count(*) FROM read_parquet('{args.output}')"
    ).fetchone()[0]
    print(f"\nWrote {args.output}: {n:,} rows x {len(kept)} columns", file=sys.stderr)

    # TSV preview
    base = args.output.rsplit(".", 1)[0]
    tsv_path = f"{base}_preview.tsv"
    con.execute(
        f"COPY (SELECT {select} FROM read_parquet('{args.output}') LIMIT 10) "
        f"TO '{tsv_path}' (FORMAT CSV, DELIMITER '\t', HEADER true)"
    )
    print(f"Preview: {tsv_path}", file=sys.stderr)

    con.close()


if __name__ == "__main__":
    main()
