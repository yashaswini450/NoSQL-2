#!/bin/bash
# Problem 2(b) runner.
# Usage: ./run_query.sh <results_dir> "<query>" [output_dir]
#   <results_dir> = Part 2(a) output folder with inverted_index.tsv, top100_df.tsv, doc_count.txt
#                   (e.g. results_full). N is read from doc_count.txt.
# Run from a folder that contains stopwords.txt and opennlp-tools-1.9.3.jar
# (needs `hadoop` on PATH, e.g. `source ~/hadoop-env/env.sh` or export HADOOP_HOME).
set -e
SRC=$(cd "$(dirname "$0")" && pwd)   # folder holding the .java sources
RES=${1:?results dir}; Q=${2:?query}; OUT=${3:-output_query}
N=$(tr -d '[:space:]' < "$RES/doc_count.txt")
mkdir -p classes_query
javac -cp "$(hadoop classpath):opennlp-tools-1.9.3.jar" -d classes_query "$SRC/TextPreprocessor.java" "$SRC/QueryRetrieval.java"
jar cf problem2_query.jar -C classes_query .
export HADOOP_CLASSPATH=$PWD/opennlp-tools-1.9.3.jar   # the driver also stems the query
rm -rf "$OUT"
hadoop jar problem2_query.jar QueryRetrieval -libjars opennlp-tools-1.9.3.jar \
    stopwords.txt "$RES/top100_df.tsv" "$RES/inverted_index.tsv" "$OUT" "$N" "$Q"
[ -f "$OUT/part-r-00000" ] && cat "$OUT/part-r-00000"
