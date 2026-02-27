#!/usr/bin/env python3
import argparse
import glob
import os
import sys
import pyarrow.parquet as pq
import pandas as pd


def parse_input(spec: str) -> tuple[str, str | None, list[str] | None]:
    parts = spec.split(":")
    path = parts[0]
    prefix = parts[1] if len(parts) > 1 and parts[1] else None
    keys = parts[2].split(",") if len(parts) > 2 and parts[2] else None
    return path, prefix, keys


def resolve_path(path: str) -> list[str]:
    if os.path.isdir(path):
        files = sorted(glob.glob(os.path.join(path, "**/*.parquet"), recursive=True))
        if not files:
            sys.exit(f"No .parquet files found in {path}")
        return files
    matches = sorted(glob.glob(path))
    return matches if matches else [path]


def load(path: str, prefix: str | None, join_cols: list[str] | None) -> pd.DataFrame:
    files = resolve_path(path)
    df = pd.concat([pq.read_table(f).to_pandas() for f in files], ignore_index=True)
    if prefix:
        rename = {c: f"{prefix}{c}" for c in df.columns if not join_cols or c not in join_cols}
        df = df.rename(columns=rename)
    return df


def main():
    p = argparse.ArgumentParser(
        description="Join parquet files/dirs. Each input is path[:prefix[:join_cols]]",
        epilog="Example: %(prog)s -o out.parquet ./left_dir/:l_:id ./right_dir/:r_:id",
    )
    p.add_argument("inputs", nargs="+", help="path[:prefix[:col1,col2,...]] — path can be a file, glob, or directory")
    p.add_argument("-o", "--output", required=True)
    p.add_argument("-j", "--join-cols", help="default join columns (comma-sep)")
    p.add_argument("--how", default="inner", choices=["inner", "outer", "left", "right", "cross"])
    p.add_argument("--suffixes", help="comma-sep suffixes for duplicate non-join columns (default: _1,_2,...)")
    args = p.parse_args()

    default_keys = args.join_cols.split(",") if args.join_cols else None
    frames = []

    for spec in args.inputs:
        path, prefix, keys = parse_input(spec)
        keys = keys or default_keys
        df = load(path, prefix, keys)
        frames.append((df, keys))

    if len(frames) < 2:
        sys.exit("Need at least 2 input files")

    result = frames[0][0]
    for i, (df, keys) in enumerate(frames[1:], 1):
        on = keys or default_keys
        if not on:
            sys.exit("No join columns specified")
        sfx = (f"_left{i}", f"_right{i}")
        result = result.merge(df, on=on, how=args.how, suffixes=sfx)

    result.to_parquet(args.output, index=False)
    print(f"Wrote {args.output}: {result.shape[0]} rows x {result.shape[1]} cols")


if __name__ == "__main__":
    main()
