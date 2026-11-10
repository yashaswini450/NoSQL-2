#!/usr/bin/env bash
set -euo pipefail

exec python3 - "$@" <<'PY'
import argparse
import csv
import os
from pathlib import Path

parser = argparse.ArgumentParser(description="Build deterministic, nested Wikipedia subsets.")
parser.add_argument("articles", type=Path, help="flat directory of extracted .txt articles")
parser.add_argument("destination", type=Path, help="new directory for the five subsets")
args = parser.parse_args()

articles = sorted(
    (path for path in args.articles.glob("*.txt") if path.is_file() and not path.name.startswith((".", "_"))),
    key=lambda path: os.fsencode(path.name),
)
if len(articles) < 10:
    parser.error("expected at least 10 .txt articles in the input directory")
if args.destination.exists():
    parser.error("the subset destination already exists")

args.destination.mkdir()
shard_limit = 16 * 1024 * 1024
with (args.destination / "subsets.tsv").open("w", newline="", encoding="utf-8") as metadata:
    writer = csv.writer(metadata, delimiter="\t", lineterminator="\n")
    writer.writerow(("percent", "directory", "documents", "input_bytes", "source_bytes"))
    for percent in (10, 20, 40, 80, 100):
        subset = args.destination / f"pct-{percent:03d}"
        subset.mkdir()
        selected = articles[:len(articles) * percent // 100]
        input_bytes = 0
        source_bytes = 0
        shard_number = 0
        shard = bytearray()
        for article in selected:
            data = article.read_bytes()
            source_bytes += len(data)
            # Keep article boundaries as Hadoop text record boundaries.
            if data and not data.endswith(b"\n"):
                data += b"\n"
            if shard and len(shard) + len(data) > shard_limit:
                (subset / f"articles-{shard_number:04d}.txt").write_bytes(shard)
                shard_number += 1
                shard.clear()
            shard.extend(data)
            input_bytes += len(data)
        (subset / f"articles-{shard_number:04d}.txt").write_bytes(shard)
        writer.writerow((percent, subset.name, len(selected), input_bytes, source_bytes))
        print(f"{percent:3}%: {len(selected)} documents, {input_bytes} input bytes", flush=True)
PY
