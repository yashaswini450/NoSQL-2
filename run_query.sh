#!/bin/bash
# Problem 2(b) runner. Usage: ./run_query.sh <inverted_index_dir> <N> "<query>" [output_dir]
# Run from the folder holding QueryRetrieval.java, stopwords.txt, opennlp-tools-1.9.3.jar.
set -e
IDX=$1; N=$2; Q=$3; OUT=${4:-output_query}
javac -cp $(hadoop classpath):opennlp-tools-1.9.3.jar QueryRetrieval.java
jar cf problem2_query.jar QueryRetrieval*.class
export HADOOP_CLASSPATH=$PWD/opennlp-tools-1.9.3.jar   # driver needs the stemmer too
rm -rf "$OUT"
hadoop jar problem2_query.jar QueryRetrieval -libjars opennlp-tools-1.9.3.jar stopwords.txt "$IDX" "$OUT" "$N" "$Q"
cat "$OUT"/part-r-00000
