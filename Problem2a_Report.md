# Problem 2(a) — Document Frequency and Inverted Index

*DAS 839 Assignment 2, Problem 2(a) (5 marks). Author: Shrey.
Every number below comes from our own runs on 2026-11-09; the raw evidence is in `results_full/` and
`results_50/` (job logs, counters, timings, check output).*

---

## 1. Environment and setup

| Item | Value |
|---|---|
| OS | Windows 11 + WSL2 Ubuntu, kernel 6.18.54.1-microsoft-standard-WSL2, x86_64 |
| CPU / RAM | AMD Ryzen 9 6900HS, 16 logical CPUs; 7.4 GiB RAM visible to WSL |
| Java | OpenJDK 11.0.33 (Eclipse Temurin) |
| Hadoop | 3.4.3, standalone (local) mode — `LocalJobRunner`, local filesystem, no HDFS/YARN |
| Libraries | `opennlp-tools-1.9.3.jar` (`opennlp.tools.stemmer.PorterStemmer`) |
| Stop words | official `stopwords.txt` (CRLF line endings, blank lines between entries → 355 non-empty entries), unmodified |

Exact versions are recorded in `results_full/environment.txt`. Java and Hadoop were installed without
root by unpacking the Temurin JDK 11 and Hadoop 3.4.3 tarballs into `~/hadoop-env/`; `~/hadoop-env/env.sh`
exports `JAVA_HOME`, `HADOOP_HOME` and `PATH`.

**Dataset preparation.**

| Dataset | Preparation | Files | Bytes |
|---|---|---|---|
| Full corpus `Wikipedia-EN-20120601_ARTICLES.tar.gz` | `tar -xf` (despite the `.gz` name the archive is not gzip-compressed) | 10,000 articles `<id>.txt` | 166,222,646 |
| Test set `Wikipedia-50-ARTICLES.tar` | `tar -xf` | 50 articles | 717,789 |

Each article is a single file named by its numeric Wikipedia ID. All development was done on the
50-article set first; final results are from the full corpus.

---

## 2. Design and implementation

### 2.1 Term pipeline (`TextPreprocessor.java`)

The same class is used by every job, so a term means exactly the same thing in the DF table, the
top-100 list and the inverted index.

1. **Case normalisation:** lowercase the whole document.
2. **Tokenisation and punctuation removal:** split on the regular expression `[^a-z0-9]+`. Every
   character that is not an ASCII letter or digit (punctuation, apostrophes, non-ASCII letters) is a
   separator, so no token contains punctuation.
3. Skip empty tokens.
4. **Stop-word filtering** on the *raw* token, before stemming: `stopwords.contains(token)` on a
   `HashSet` loaded from the DistributedCache.
5. **Stemming:** `PorterStemmer.stem(token)` from `opennlp-tools-1.9.3.jar`.
6. Skip empty stems.

Stop words are removed before stemming because the list contains words as they appear in text
(`having`, `does`). Stemming first would produce forms such as `doe` (from `does`) that the list does
not contain, so stop words would slip through.

**Document identification.** DOC_ID is the article file name without `.txt`
(`563203.txt` → `563203`), a stable integer that uniquely identifies each article.

### 2.2 One record per document (`WholeDocumentInputFormat.java`)

A `CombineFileInputFormat<Text, Text>` whose reader returns each file as **one record**:
key = DOC_ID, value = full article text. Files are marked non-splittable.

Two reasons for this design:

- **Exact per-document semantics.** One `map()` call sees exactly one whole document, so removing
  duplicate terms (for DF) and counting occurrences (for TF) inside `map()` is exact. It does not
  depend on an article happening to be one line.
- **Small-files problem.** The default `TextInputFormat` creates one split, and so one map task, per
  file — 10,000 map tasks for 166 MB. `CombineFileInputFormat` packs files into splits of at most
  64 MB (`mapreduce.input.fileinputformat.split.maxsize`), giving **3 map tasks** for the full corpus.

The DOC_ID is set by the record reader rather than by casting `context.getInputSplit()` to `FileSplit`,
because a combined split holds many files.

### 2.3 Job 1 — Document frequency (`DocumentFrequency.java`)

```
setup():
    stopwords ← load cached stopwords.txt into a HashSet
map(docId, text):
    terms    ← preprocess(text)            # list, repeats kept
    distinct ← HashSet(terms)              # each term once per document
    for t in distinct: emit(t, 1)
combine(t, counts) = reduce(t, counts):
    emit(t, Σ counts)                      # = DF(t)
```

- **Why DF is correct:** every emitted `1` stands for one distinct (term, document) pair, because the
  `HashSet` removes repeats within a document. Summing them counts documents, not occurrences.
- **Why the combiner is safe:** the reduce function is an associative, commutative integer sum, and
  each input value already represents one distinct document, so summing early cannot double-count.
- **One reducer**, so all DF values land in the single TSV file the assignment asks for
  (`df_all.tsv`, sorted by TERM).
- The job counts the number of documents read (`DOCS` counter) and writes it to `doc_count.txt`;
  this is the total number of documents, **N = 10,000**.

### 2.4 Job 2 — Top 100 terms by DF (top-K pattern)

```
map(line "term<TAB>df"):
    add (term, df) to a TreeSet ordered by (DF desc, TERM asc); if size > 100, drop the last
cleanup():
    emit(null, entry) for each entry of this mapper's local top 100
reduce(null, entries):                     # single reducer
    merge into a TreeSet with the same order, trimmed to 100; emit in rank order
```

This is correct because each term occurs in exactly one mapper's input, so every term of the global
top 100 is also in its own mapper's local top 100. The mappers send at most 100 records each instead of
524,390.

**Ties:** ordered by DF descending, then TERM ascending (lexicographic, Java `String.compareTo`). This is
a total order, so the top 100 is deterministic. Within the list, `2011` and `world` (DF 5,639) and
`nation` and `present` (DF 5,378) are tied and ordered alphabetically. At the cut-off, see §3.

### 2.5 Job 3 — Inverted index (`InvertedIndex.java`)

`stopwords.txt` and `top100_df.tsv` are both shipped with the DistributedCache.

```
setup():
    stopwords ← cached stopwords.txt ; top ← cached top100_df.tsv as HashMap term → DF
map(docId, text):
    tf ← HashMap
    for t in preprocess(text):
        if t in top: tf[t] += 1            # aggregate inside the document
    for (t, c) in tf: emit(t, "docId:c")   # one record per (term, document)
reduce(t, postings):                       # single reducer
    collect into HashMap docId → tf        # counter DUPLICATE_DOC_IN_POSTINGS if a docId repeats
    sort docIds numerically ascending
    emit(t, "d1:tf1,d2:tf2,...")
    if #postings ≠ DF(t) from top100_df.tsv: counter DF_MISMATCH += 1
```

**Output schema** (`inverted_index.tsv`, one line per term, 100 lines, sorted by TERM):

```
TERM<TAB>DOC_ID:TF,DOC_ID:TF,...
```

Postings are ordered by DOC_ID ascending (numerically). The number of postings of a term equals its DF.

### 2.6 Data structures and aggregation strategy

| Where | Structure | What it aggregates | Effect (full corpus, measured) |
|---|---|---|---|
| DF map, per document | `HashSet<String>` | repeated terms in one document | 18,110,423 term occurrences → 7,153,978 records (−60.5%) |
| DF combiner, per map task | integer sum | the same term across documents of one split | 7,153,978 → 724,282 shuffled records (−89.9%) |
| Top-K map | bounded `TreeSet` (100) | local top 100 | 524,390 input lines → 100 records |
| Index map, per document | `HashMap<String,Integer>` | occurrences of a top-100 term in one document | 3,203,078 occurrences → 615,076 records (−80.8%) |
| Index reduce | `HashMap` docId → TF, then sort | all postings of one term | 100 output lines |

All jobs use `Tool`/`ToolRunner`, so the generic option `-libjars opennlp-tools-1.9.3.jar` ships the
stemmer to the tasks.

---

## 3. Correctness checks

### 3.1 Independent checker (`check_problem2a.py`)

The checker is plain Python. It does not use Hadoop, the Java code or a stemmer, so it is an
independent check of the outputs. Full output is in `results_full/checks.txt` and `results_50/checks.txt`;
**all checks passed on both datasets.**

| Check | Full corpus | 50 articles |
|---|---|---|
| `doc_count.txt` equals the number of article files | 10,000 = 10,000 ✅ | 50 = 50 ✅ |
| `df_all.tsv` has no duplicate terms | 524,390 rows ✅ | 12,776 rows ✅ |
| 1 ≤ DF ≤ N for every term | ✅ | ✅ |
| `top100_df.tsv` equals the top 100 of `df_all.tsv` under (DF desc, TERM asc) | ✅ | ✅ |
| Index contains exactly the 100 top terms | ✅ | ✅ |
| **#postings = DF for every term** (no duplicate document counts) | ✅ | ✅ |
| No DOC_ID appears twice in a posting list | ✅ | ✅ |
| Every TF ≥ 1; every DOC_ID is a real article; postings sorted | ✅ | ✅ |
| Raw-text cross-check (below) | 12 terms ✅ | 6 terms ✅ |
| No top-100 term is spelled like a stop word | ✅ | ✅ |

**Raw-text cross-check.** For index terms that also occur as raw tokens in the text (e.g. `american`,
`area`, `both`, `first`, `form`, `found`), the checker scans every article itself and confirms that each
article containing the raw token is in the posting list with TF ≥ the raw count. TF can be larger because
other words stem to the same term (`forms` → `form`). This check is one-directional; exact equality is
shown by the hand checks below.

**Reducer counters.** `InvertedIndex` compares each posting list with `top100_df.tsv`. The counters
`DF_MISMATCH` and `DUPLICATE_DOC_IN_POSTINGS` never fired (Hadoop does not print counters that stay at 0),
and `POSTINGS = 615,076` equals Σ DF over the 100 terms.

### 3.2 Hand checks with `grep` (50 articles, `results_50/handcheck.txt`)

All surface forms of a stem were counted with `grep` directly on the raw articles and compared with the
MapReduce output. These confirm exact equality, including stemming.

| Term | Raw forms counted | Quantity | MapReduce | `grep` |
|---|---|---|---|---|
| `includ` | include, includes, included, including | DF | 42 | 42 |
| `peopl` | people, peoples | DF | 36 | 36 |
| `includ` | include, includes, included, including | TF in doc 1338 | 12 | 12 |
| `peopl` | people, peoples, peopled | TF in doc 113147 | 51 | 51 |

### 3.3 Stop words

Stop words are removed before stemming, and the checker confirms that no top-100 term is spelled like a
stop word. Some frequent terms are not in the official list and therefore remain valid terms: `this`
(stem `thi`), `us` and `use` are absent from `stopwords.txt`.

### 3.4 Ties at the top-100 cut-off

| Dataset | DF at rank 100 | Terms with that DF | Kept (TERM asc) | Excluded |
|---|---|---|---|---|
| Full corpus | 4,832 | 1 (`major`) | 1 | 0 |
| 50 articles | 24 | 13 | 1 (`44`) | 12 |

On the full corpus the rank-100 DF is not shared, so the tie rule excludes no term. On the test set it
decides which 1 of 13 tied terms is kept.

---

## 4. Results and performance

### 4.1 Corpus statistics (full corpus, N = 10,000)

| Quantity | Value |
|---|---|
| Documents (N) | 10,000 |
| Term occurrences after stop-word removal | 18,110,423 |
| Distinct stemmed non-stop terms | 524,390 |
| Distinct (term, document) pairs = Σ DF | 7,153,978 |
| Postings in the top-100 index | 615,076 |
| Occurrences of top-100 terms (Σ TF) | 3,203,078 |
| Largest single TF | `apo` in document 509836: 6,188 |

### 4.2 Top 100 terms by DF (`top100_df.tsv`)

| # | Term | DF | # | Term | DF | # | Term | DF | # | Term | DF |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `s` | 9836 | 26 | `see` | 6761 | 51 | `2011` | 5639 | 76 | `found` | 5137 |
| 2 | `apo` | 9518 | 27 | `histori` | 6657 | 52 | `world` | 5639 | 77 | `citi` | 5133 |
| 3 | `categori` | 9381 | 28 | `10` | 6642 | 53 | `live` | 5627 | 78 | `2007` | 5127 |
| 4 | `refer` | 9340 | 29 | `peopl` | 6626 | 54 | `follow` | 5614 | 79 | `form` | 5121 |
| 5 | `quot` | 8674 | 30 | `0` | 6606 | 55 | `three` | 5599 | 80 | `25` | 5089 |
| 6 | `other` | 8415 | 31 | `6` | 6565 | 56 | `11` | 5503 | 81 | `18` | 5072 |
| 7 | `1` | 8350 | 32 | `part` | 6501 | 57 | `american` | 5501 | 82 | `ca` | 5071 |
| 8 | `link` | 8295 | 33 | `unit` | 6498 | 58 | `earli` | 5473 | 83 | `make` | 5067 |
| 9 | `thi` | 8190 | 34 | `pt` | 6372 | 59 | `m` | 5458 | 84 | `15` | 5050 |
| 10 | `2` | 8105 | 35 | `nl` | 6333 | 60 | `both` | 5454 | 85 | `end` | 5040 |
| 11 | `us` | 8059 | 36 | `made` | 6332 | 61 | `work` | 5427 | 86 | `area` | 5012 |
| 12 | `extern` | 8008 | 37 | `7` | 6322 | 62 | `origin` | 5400 | 87 | `30` | 4970 |
| 13 | `two` | 7949 | 38 | `8` | 6281 | 63 | `2008` | 5387 | 88 | `c` | 4968 |
| 14 | `includ` | 7885 | 39 | `n` | 6223 | 64 | `through` | 5381 | 89 | `dai` | 4944 |
| 15 | `year` | 7800 | 40 | `2010` | 6188 | 65 | `nation` | 5378 | 90 | `p` | 4927 |
| 16 | `first` | 7689 | 41 | `known` | 6085 | 66 | `present` | 5378 | 91 | `becam` | 4897 |
| 17 | `3` | 7529 | 42 | `9` | 6051 | 67 | `d` | 5347 | 92 | `base` | 4892 |
| 18 | `http` | 7418 | 43 | `pl` | 6034 | 68 | `sv` | 5334 | 93 | `24` | 4881 |
| 19 | `fr` | 7376 | 44 | `place` | 5973 | 69 | `ja` | 5280 | 94 | `t` | 4859 |
| 20 | `time` | 7349 | 45 | `gener` | 5827 | 70 | `2009` | 5265 | 95 | `2000` | 4858 |
| 21 | `state` | 7299 | 46 | `call` | 5804 | 71 | `e` | 5220 | 96 | `number` | 4854 |
| 22 | `4` | 7037 | 47 | `12` | 5785 | 72 | `20` | 5195 | 97 | `hous` | 4842 |
| 23 | `5` | 7025 | 48 | `ru` | 5752 | 73 | `univers` | 5194 | 98 | `second` | 4838 |
| 24 | `name` | 6838 | 49 | `i` | 5695 | 74 | `id` | 5188 | 99 | `high` | 4834 |
| 25 | `www` | 6780 | 50 | `well` | 5660 | 75 | `uk` | 5162 | 100 | `major` | 4832 |

Porter stems are not always dictionary words (`categori`, `histori`, `peopl`, `dai` from "day"). The
noise terms in this list (numbers, single letters, `quot`, `apo`, `http`) are discussed in §6.

### 4.3 Representative inverted-index records (`inverted_index_sample.tsv`)

Terms ranked 1, 25, 50, 75 and 100 by DF; the first 10 postings of each (full lines are in
`results_full/inverted_index.tsv`):

```
s      698:39,700:95,708:2,709:8,740:10,842:7,857:8,864:148,900:44,902:91
www    698:2,700:8,708:2,740:3,842:1,857:20,864:25,900:18,902:54,1016:14
well   698:2,700:4,740:1,842:1,857:1,864:9,900:9,902:6,1016:1,1178:6
uk     698:1,700:2,740:2,842:1,857:22,864:6,900:1,902:9,1016:3,1178:1
major  698:8,700:3,709:2,740:2,842:1,864:2,902:2,1178:1,1198:3,1214:4
```

Read `698:39` as "term `s` occurs 39 times in article 698". The line for `s` has 9,836 postings, equal
to its DF.

### 4.4 Job counters and run times

| Job | Map tasks | Map input records | Map output records | Map output bytes | Combine output records | Reduce output records |
|---|---|---|---|---|---|---|
| DF, full | 3 | 10,000 | 7,153,978 | 74,106,228 | 724,282 | 524,390 |
| Top-100, full | 1 | 524,390 | 100 | 944 | – | 100 |
| Inverted index, full | 3 | 10,000 | 615,076 | 8,134,949 | – | 100 |
| DF, 50 articles | 1 | 50 | 32,659 | 334,891 | 12,776 | 12,776 |
| Top-100, 50 articles | 1 | 12,776 | 100 | 745 | – | 100 |
| Inverted index, 50 articles | 1 | 50 | 3,221 | 42,481 | – | 100 |

For the full DF job the combiner reduced the materialised map output from 74.1 MB to 10.3 MB
(`Map output materialized bytes=10300285`).

| Dataset | DF + Top-100 (`DocumentFrequency`) | Inverted index | Input |
|---|---|---|---|
| Full corpus | 20.08 s | 11.97 s | 10,000 files, 166,222,646 bytes |
| 50 articles | 3.73 s | 2.41 s | 50 files, 717,789 bytes |

**Timing method.** Wall-clock time of each `hadoop jar` command, measured in `run_problem2a.sh` with
`date +%s.%N` before and after. It includes JVM and job start-up, reading the input and writing the
output. It does not include extracting the tar files. There is no HDFS transfer because the runs are in
standalone mode. Each run was made once: 2(a) is not part of the scalability study, so no repeated runs
or medians are reported for it.

### 4.5 Log excerpt (`results_full/logs/document_frequency.log`)

```
INFO mapreduce.Job: Job job_local340665873_0001 completed successfully
INFO mapreduce.Job: Counters: 33
	Map-Reduce Framework
		Map input records=10000
		Map output records=7153978
		Map output bytes=74106228
		Map output materialized bytes=10300285
		Combine input records=7153978
		Combine output records=724282
		Reduce input groups=524390
		Reduce input records=724282
		Reduce output records=524390
		Shuffled Maps =3
	DocumentFrequency$Counters
		DISTINCT_TERM_DOC_PAIRS=7153978
		DOCS=10000
		TERM_OCCURRENCES=18110423
...
Documents (N) = 10000
```

---

## 5. Reproducibility

**Source files:** `TextPreprocessor.java`, `WholeDocumentInputFormat.java`, `DocumentFrequency.java`,
`InvertedIndex.java`; scripts `run_problem2a.sh` (build and run) and `check_problem2a.py` (checks).

**One command per dataset** (compiles, runs DF → Top-100 → inverted index, copies outputs, records timings):

```bash
source ~/hadoop-env/env.sh                        # JAVA_HOME = JDK 11, HADOOP_HOME = Hadoop 3.4.3
tar -xf Wikipedia-50-ARTICLES.tar
tar -xf Wikipedia-EN-20120601_ARTICLES.tar.gz     # not gzip-compressed despite the name

./run_problem2a.sh Wikipedia-50-ARTICLES          results_50
./run_problem2a.sh Wikipedia-EN-20120601_ARTICLES results_full
python3 check_problem2a.py results_full Wikipedia-EN-20120601_ARTICLES stopwords.txt
```

**Equivalent manual commands:**

```bash
export HADOOP_CLASSPATH=$PWD/opennlp-tools-1.9.3.jar
mkdir classes
javac -classpath "$(hadoop classpath):opennlp-tools-1.9.3.jar" -d classes \
      TextPreprocessor.java WholeDocumentInputFormat.java DocumentFrequency.java InvertedIndex.java
jar cf problem2_index.jar -C classes .

hadoop jar problem2_index.jar DocumentFrequency -libjars opennlp-tools-1.9.3.jar \
      stopwords.txt Wikipedia-EN-20120601_ARTICLES out_df
hadoop jar problem2_index.jar InvertedIndex -libjars opennlp-tools-1.9.3.jar \
      stopwords.txt out_df/top100/part-r-00000 Wikipedia-EN-20120601_ARTICLES out_index
```

**Input and output paths**

| Output | Hadoop path | Copied to | Schema |
|---|---|---|---|
| All DF values | `out_df/df/part-r-00000` | `results_full/df_all.tsv` | `TERM<TAB>DF`, sorted by TERM |
| Top 100 | `out_df/top100/part-r-00000` | `results_full/top100_df.tsv` | `TERM<TAB>DF`, by (DF desc, TERM asc) |
| N | `out_df/doc_count.txt` | `results_full/doc_count.txt` | one integer |
| Inverted index | `out_index/part-r-00000` | `results_full/inverted_index.tsv` | `TERM<TAB>DOC_ID:TF,...` |

**Fixed configuration:** 1 reducer per job; maximum combined split size 64 MB
(`-D mapreduce.input.fileinputformat.split.maxsize=...` to change); K = 100 (`-D p2.topk=...` to change).

---

## 6. Limitations and observations

- **Noise terms in the top 100.** With the agreed tokeniser (`[^a-z0-9]+`, no filter on length or
  numbers), about 45 of the 100 terms are not content words:
  - 25 numbers (`1`, `2`, `10`, `2010`, …);
  - 9 single letters (`s`, `n`, `i`, `m`, `d`, `e`, `c`, `p`, `t`). Most occurrences of `s` come from
    the possessive written as the HTML entity `&apos;s`;
  - HTML-entity residue: `quot` and `apo` from `&quot;` and `&apos;`;
  - interlanguage-link prefixes such as `fr`, `pt`, `nl`, `pl`, `ru`, `sv`, `ja`, `id` (from text like
    `fr:Montrose`);
  - URL parts `http` and `www`.

  These follow directly from the agreed tokenisation, which was kept unchanged. A cleaner variant would
  decode HTML entities before tokenising and drop 1-character and purely numeric tokens.
- **Stop-word list gaps.** `stopwords.txt` does not contain `this`, `us` or `use`, so `thi` and `us` are
  valid terms. 47 of its 355 entries contain an apostrophe (`don't`, `it's`, …) and can never match a
  token after splitting on `[^a-z0-9]+`. The official list was used unchanged.
- **Non-ASCII letters** act as separators (`café` → `caf`).
- **Standalone mode.** All runs used Hadoop's local mode on one machine, so the times include no HDFS or
  YARN overhead and map tasks run one after another. They would differ on a cluster.
- **Single runs.** 2(a) was timed once per dataset because it is not part of the scalability experiment.
- **Reducer memory.** The index reducer holds all postings of one term in memory before sorting (at most
  9,836 here). This is fine for 10,000 articles; for a much larger corpus, a composite (term, DOC_ID) key
  with a partitioner on the term alone (value-to-key conversion) would let Hadoop sort the postings
  instead.
