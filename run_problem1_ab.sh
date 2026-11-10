#!/usr/bin/env bash
# ==============================================================================
# Problem 1(a) & 1(b) Build & Execution Script (Person 1 — Yashaswini)
# ==============================================================================
set -euo pipefail

INPUT_DIR="${1:-Wikipedia-EN-20120601_ARTICLES/}"
STOPWORDS="stopwords.txt"

echo "=== [1/4] Compiling WordCount2, Top50Words, and WordAssociationPairs ==="
javac -classpath "$(hadoop classpath)" WordCount2.java Top50Words.java WordAssociationPairs.java
jar cf problem1_pairs.jar WordCount2*.class Top50Words*.class WordAssociationPairs*.class

echo "=== [2/4] Running Problem 1(a): Top 50 Frequent Non-Stop Words ==="
rm -rf output_top50
hadoop jar problem1_pairs.jar Top50Words "${STOPWORDS}" "${INPUT_DIR}" output_top50 50
cp output_top50/part-r-00000 top50_words.tsv
echo "Saved top50_words.tsv ($(wc -l < top50_words.tsv) words)."

echo "=== [3/4] Running Problem 1(b): Word Association Pairs (No Local Aggregation) ==="
rm -rf output_pairs_noagg
hadoop jar problem1_pairs.jar WordAssociationPairs "${STOPWORDS}" top50_words.tsv "${INPUT_DIR}" output_pairs_noagg false 2

echo "=== [4/4] Running Problem 1(b): Word Association Pairs (With Local Aggregation) ==="
rm -rf output_pairs_agg
hadoop jar problem1_pairs.jar WordAssociationPairs "${STOPWORDS}" top50_words.tsv "${INPUT_DIR}" output_pairs_agg true 2
cp output_pairs_agg/part-r-00000 pairs_top5.tsv
echo "Saved pairs_top5.tsv ($(wc -l < pairs_top5.tsv) rows)."

echo "Verifying consistency between No-Aggregation and Local-Aggregation Pairs outputs..."
diff -u output_pairs_noagg/part-r-00000 output_pairs_agg/part-r-00000 && echo "SUCCESS: Outputs match identically!"

