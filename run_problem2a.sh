#!/bin/bash
# Problem 2(a): build and run DocumentFrequency (DF + Top-100) and InvertedIndex.
#
# Usage:   ./run_problem2a.sh <input_dir> <results_dir>
# Example: ./run_problem2a.sh Wikipedia-50-ARTICLES results_50
#          ./run_problem2a.sh Wikipedia-EN-20120601_ARTICLES results_full
#
# Expects JAVA_HOME / HADOOP_HOME on PATH (e.g. `source ~/hadoop-env/env.sh`) and, in the
# current directory: the .java sources, stopwords.txt and opennlp-tools-1.9.3.jar.
set -euo pipefail

INPUT=${1:?input_dir}
RESULTS=${2:?results_dir}
OPENNLP=opennlp-tools-1.9.3.jar
TAG=$(basename "$RESULTS")
WORK=hadoop_out_$TAG

export HADOOP_CLASSPATH=$PWD/$OPENNLP

mkdir -p "$RESULTS/logs"
rm -rf "$WORK" classes

echo "== Compile"
mkdir classes
javac -classpath "$(hadoop classpath):$OPENNLP" -d classes \
	TextPreprocessor.java WholeDocumentInputFormat.java DocumentFrequency.java InvertedIndex.java
jar cf problem2_index.jar -C classes .

now() { date +%s.%N; }

echo "== DocumentFrequency (DF + Top-100) on $INPUT"
t0=$(now)
hadoop jar problem2_index.jar DocumentFrequency -libjars "$OPENNLP" \
	stopwords.txt "$INPUT" "$WORK/df_out" 2>&1 | tee "$RESULTS/logs/document_frequency.log"
t1=$(now)

cp "$WORK/df_out/df/part-r-00000" "$RESULTS/df_all.tsv"
cp "$WORK/df_out/top100/part-r-00000" "$RESULTS/top100_df.tsv"
cp "$WORK/df_out/doc_count.txt" "$RESULTS/doc_count.txt"

echo "== InvertedIndex on $INPUT"
t2=$(now)
hadoop jar problem2_index.jar InvertedIndex -libjars "$OPENNLP" \
	stopwords.txt "$RESULTS/top100_df.tsv" "$INPUT" "$WORK/index_out" 2>&1 | tee "$RESULTS/logs/inverted_index.log"
t3=$(now)

cp "$WORK/index_out/part-r-00000" "$RESULTS/inverted_index.tsv"

# Representative records: terms ranked 1, 25, 50, 75 and 100 by DF, first 10 postings each.
{
	echo "# TERM<TAB>DOC_ID:TF,... (sample: terms ranked 1/25/50/75/100 by DF; at most the first 10 postings shown; full lines in inverted_index.tsv)"
	for rank in 1 25 50 75 100; do
		term=$(sed -n "${rank}p" "$RESULTS/top100_df.tsv" | cut -f1)
		[ -n "$term" ] || continue
		awk -F'\t' -v t="$term" '$1 == t {
			n = split($2, p, ",");
			line = p[1];
			for (i = 2; i <= n && i <= 10; i++) line = line "," p[i];
			print $1 "\t" line;
		}' "$RESULTS/inverted_index.tsv"
	done
} > "$RESULTS/inverted_index_sample.tsv"

{
	echo "input=$INPUT"
	echo "documents=$(cat "$RESULTS/doc_count.txt")"
	echo "input_bytes=$(du -sb "$INPUT" | cut -f1)"
	awk -v a="$t0" -v b="$t1" 'BEGIN { printf "document_frequency_seconds=%.2f\n", b - a }'
	awk -v a="$t2" -v b="$t3" 'BEGIN { printf "inverted_index_seconds=%.2f\n", b - a }'
	echo "timing=wall clock of each 'hadoop jar' command (JVM + job startup, input read, output write; standalone mode, no HDFS transfer)"
} | tee "$RESULTS/timings.txt"

echo "== Done: results in $RESULTS"
