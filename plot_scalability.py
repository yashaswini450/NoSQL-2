#!/usr/bin/env python3
"""Plot measured Hadoop runtimes from run_scalability.sh."""

import argparse
from collections import defaultdict
import csv

import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("summary_csv")
    parser.add_argument("output_png")
    parser.add_argument("--title", default="Problem 1(d) Hadoop scalability")
    args = parser.parse_args()

    series = defaultdict(list)
    with open(args.summary_csv, newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            series[row["configuration"]].append((int(row["input_bytes"]) / 2**20, float(row["median_seconds"])))
    if not series:
        parser.error("the summary has no measurements")

    fig, ax = plt.subplots(figsize=(8, 4.5), layout="constrained")
    for name, measurements in series.items():
        size, seconds = zip(*sorted(measurements))
        marker = "o" if name.startswith("Pairs") else "s"
        linestyle = "--" if name.endswith("-Agg") else "-"
        ax.plot(size, seconds, marker=marker, linestyle=linestyle, label=name)
    ax.set(title=args.title, xlabel="Input size (MiB)", ylabel="Median wall-clock time (s)")
    ax.set_ylim(bottom=0)
    ax.grid(alpha=0.25)
    ax.legend()
    fig.savefig(args.output_png, dpi=180)


if __name__ == "__main__":
    main()
