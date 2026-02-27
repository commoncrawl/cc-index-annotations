import argparse
import sys
from functools import reduce
import pyarrow.parquet as pq
import pandas as pd


def parse_input(spec: str) -> tuple[str, str | None, list[str] | None]:
    parts = spec.split(":")
    path = parts[0]
    prefix = parts[1] if len(parts) > 1 and parts[1] else None
    keys = parts[2].split(",") if len(parts) > 2 and parts[2] else None
    return path, prefix, keys


def load(path: str, prefix: str | None, join_cols: list[str] | None) -> pd.DataFrame:
    df = pq.read_table(path).to_pandas()
    if prefix:
        rename = {c: f"{prefix}{c}" for c in df.columns if not join_cols or c not in join_cols}
        df = df.rename(columns=rename)
    return df


def main():
    p = argparse.ArgumentParser(
        description="Join parquet files. Each input is path[:prefix[:join_cols]]",
        epilog="Example: %(prog)s -o out.parquet a.parquet:a_:id,date b.parquet:b_:id,date c.parquet::id",
    )
    p.add_argument("inputs", nargs="+", help="path[:prefix[:col1,col2,...]]")
    p.add_argument("-o", "--output", required=True)
    p.add_argument("-j", "--join-cols", help="default join columns (comma-sep)")
    p.add_argument("--how", default="inner", choices=["inner", "outer", "left", "right", "cross"])
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
    for df, keys in frames[1:]:
        on = keys or default_keys
        if not on:
            sys.exit("No join columns specified")
        result = result.merge(df, on=on, how=args.how)

    result.to_parquet(args.output, index=False)
    print(f"Wrote {args.output}: {result.shape[0]} rows x {result.shape[1]} cols")


if __name__ == "__main__":
    main()
