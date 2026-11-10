#!/usr/bin/env bash
set -euo pipefail
export HADOOP_ROOT_LOGGER=INFO,console

exec python3 - "$@" <<'PY'
import argparse
import csv
from pathlib import Path
import re
import shutil
from statistics import median
import subprocess
from time import perf_counter

parser = argparse.ArgumentParser(description="Run and measure the Problem 1 Hadoop configurations in local mode.")
parser.add_argument("subsets", type=Path)
parser.add_argument("stopwords", type=Path)
parser.add_argument("top50", type=Path)
parser.add_argument("job_jar", type=Path, help="JAR containing both Pairs and Stripes classes")
parser.add_argument("destination", type=Path, help="new directory for output, logs and timing tables")
args = parser.parse_args()

configurations = (
    ("Pairs-NoAgg", "WordAssociationPairs", "false"),
    ("Pairs-Agg", "WordAssociationPairs", "true"),
    ("Stripes-NoAgg", "WordAssociationStripes", "false"),
    ("Stripes-Agg", "WordAssociationStripes", "true"),
)

for path in (args.stopwords, args.top50, args.job_jar):
    if not path.is_file():
        parser.error(f"input file does not exist: {path}")
with (args.subsets / "subsets.tsv").open(encoding="utf-8", newline="") as metadata:
    subsets = list(csv.DictReader(metadata, delimiter="\t"))
if [int(subset["percent"]) for subset in subsets] != [10, 20, 40, 80, 100]:
    parser.error("expected subset metadata for 10, 20, 40, 80, and 100 percent")

destination = args.destination.resolve()
destination.mkdir()
raw_fields = ("configuration", "percent", "documents", "input_bytes", "trial", "seconds",
              "map_output_records", "map_output_bytes")
summary_fields = ("configuration", "percent", "documents", "input_bytes", "trials", "median_seconds",
                  "median_map_output_records", "median_map_output_bytes")


def read_counters(log):
    text = log.read_text(encoding="utf-8", errors="replace")
    _, marker, block = text.rpartition("mapreduce.Job: Counters:")
    if not marker:
        raise SystemExit(f"No final Hadoop counter table in {log}")
    records = re.search(r"(?m)^\s*Map output records=(\d+)\s*$", block)
    size = re.search(r"(?m)^\s*Map output bytes=(\d+)\s*$", block)
    if records is None or size is None:
        raise SystemExit(f"Missing mapper counters in {log}")
    return int(records[1]), int(size[1])


with (destination / "scalability_runs.csv").open("w", newline="", encoding="utf-8") as raw_file, \
        (destination / "scalability_results.csv").open("w", newline="", encoding="utf-8") as summary_file:
    raw = csv.writer(raw_file, lineterminator="\n")
    summary = csv.writer(summary_file, lineterminator="\n")
    raw.writerow(raw_fields)
    summary.writerow(summary_fields)
    for subset in subsets:
        percent = int(subset["percent"])
        articles = (args.subsets / subset["directory"]).resolve()
        if not articles.is_dir():
            raise SystemExit(f"Missing subset directory: {articles}")
        repetitions = 3 if percent in (10, 40, 100) else 1
        measurements = {name: [] for name, _, _ in configurations}
        reference = None
        for trial in range(1, repetitions + 1):
            for name, main_class, enabled in configurations:
                run = f"{percent:03d}-{name}-{trial}"
                log = destination / f"{run}.log"
                output = destination / run
                command = ["hadoop", "jar", str(args.job_jar.resolve()), main_class, str(args.stopwords.resolve()),
                           str(args.top50.resolve()), str(articles), str(output), enabled]
                with log.open("w", encoding="utf-8") as stream:
                    start = perf_counter()
                    result = subprocess.run(command, cwd=destination, stdout=stream,
                                            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
                    seconds = perf_counter() - start
                if result.returncode:
                    raise SystemExit(f"{run} failed with exit code {result.returncode}. See {log}")
                parts = list(output.glob("part-r-*"))
                if len(parts) != 1:
                    raise SystemExit(f"{run} must produce one reducer output in {output}")
                rows = parts[0].read_bytes()
                if reference is None:
                    reference = rows
                elif rows != reference:
                    raise SystemExit(f"{run}: output differs from the first Pairs run at {percent}%")
                records, size = read_counters(log)
                measurements[name].append((seconds, records, size))
                raw.writerow((name, percent, subset["documents"], subset["input_bytes"], trial,
                              f"{seconds:.6f}", records, size))
                raw_file.flush()
                print(f"{run}: {seconds:.3f}s, {records} mapper records, {size} mapper bytes", flush=True)
        for name, trials in measurements.items():
            elapsed, records, size = (median(values) for values in zip(*trials))
            summary.writerow((name, percent, subset["documents"], subset["input_bytes"], repetitions,
                              f"{elapsed:.6f}", records, size))
            summary_file.flush()
shutil.copyfile(destination / "100-Stripes-Agg-1" / "part-r-00000", destination / "stripes_top5.tsv")
PY
