# DAS 839 NoSQL - Assignment 2 Report

## 1. Environment and setup
TODO: Java 11, Hadoop 3.4.3 (standalone, WSL2 Ubuntu), machine specs, dataset prep, nested subset method (Person 2).

## 2. Design and implementation
### Problem 2(b) - Query retrieval
- Driver: lowercase, split `[^a-z0-9]+`, stop-word removal, PorterStemmer, dedupe into set Q (repeated query terms count once, matching t in Q∩D).
- Mapper: for index line `TERM\tDOC:TF,...` with TERM in Q: DF = #postings, IDF = ln(N/DF + 1); emit `DOC -> TERM:TF*IDF`.
- Reducer (single): sum contributions per doc, collect sorted matched terms, sort by score desc then DOC_ID asc, emit top 10.
TODO: Problems 1(a)-(d), 2(a) (other members), pseudocode.

## 3. Correctness checks
- 2(b) Hadoop output verified identical to independent Python scorer (`verify_retrieval.py`) on a 50-article test index.
- TODO: worked score calculation for 3 retrieved documents (full dataset): TF, DF, IDF, sum.
- TODO: stop words absent, DF counts distinct docs, Pairs vs Stripes diff.

## 4. Performance evaluation
TODO (Person 2): runtime tables, median of 3 runs, mapper output records/bytes, plot.

## 5. Reproducibility
See `problem_1.txt`, `problem_2.txt` (TODO: paste contents).

## 6. Limitations
TODO.
