# Problem 2(a) — Document Frequency & Inverted Index (Person 3: Shrey)

Handoff notes for the report (`REPORT.md`) and for Person 4 (Problem 2(b)). All numbers below come
from real runs on 2026-11-09/10 (logs in `results_full/logs/` and `results_50/logs/`).

---

## 0. What this part is about (plain language)

Problem 2 builds a tiny search engine with MapReduce. Part (a) builds the *index*; part (b) uses it to
rank documents for a query.

- **Document frequency (DF)** of a term = number of *distinct* articles containing it (an article that
  uses "film" 50 times still counts once). Rare terms get a high IDF in 2(b), common ones a low IDF.
- **Top 100 terms by DF** = the 100 terms that occur in the most articles. Only these are indexed.
- **Inverted index** = for each of those 100 terms, the list of articles containing it together with the
  term frequency (TF = how many times it occurs in that article). 2(b) reads this to score documents with
  `S(D,Q) = Σ TF(t,D) · ln(N/DF(t) + 1)`.

---

## 1. Environment

| Item | Value |
|---|---|
| OS | Windows 11 + WSL2 Ubuntu (kernel 6.18.54.1-microsoft-standard-WSL2, x86_64) |
| CPU / RAM | AMD Ryzen 9 6900HS, 16 logical CPUs; 7.4 GiB RAM visible to WSL |
| Java | OpenJDK 11.0.33 (Eclipse Temurin) |
| Hadoop | 3.4.3, **standalone (local) mode**, LocalJobRunner, local filesystem (no HDFS/YARN) |
| Libraries | `opennlp-tools-1.9.3.jar` (PorterStemmer) |
| Dataset | `Wikipedia-EN-20120601_ARTICLES.tar.gz` — actually an *uncompressed* tar (`tar -xf`), 10,000 files `<id>.txt`, 166,222,646 bytes; each article is one line. Test set: `Wikipedia-50-ARTICLES.tar` (50 files, 717,789 bytes) |
| Stop words | `stopwords.txt` (CRLF, blank lines between entries → 355 non-empty entries) |

Install (no root): Temurin JDK 11 and Hadoop 3.4.3 tarballs unpacked into `~/hadoop-env/`, with
`~/hadoop-env/env.sh` exporting `JAVA_HOME`, `HADOOP_HOME`, `PATH` and `HADOOP_CLASSPATH`.

---

## 2. Conventions (shared with 2(b) — must stay identical)

**Term pipeline** (`TextPreprocessor.terms`), applied to every document and to queries:
1. lowercase
2. split on `[^a-z0-9]+` — punctuation, apostrophes and non-ASCII characters are separators
3. skip empty tokens
4. skip stop words (checked on the **raw** token, before stemming)
5. `opennlp.tools.stemmer.PorterStemmer.stem(token)`
6. skip empty stems

This is exactly the pipeline in Person 4's 2(b) driver (lowercase, `[^a-z0-9]+`, stop words, PorterStemmer).

**Document ID:** file name without `.txt` (e.g. `563203.txt` → `563203`), a stable integer ID per article.
Set by the input format, not by `FileSplit` (see 3.1).

**Ordering / ties:** DF descending, then TERM ascending (lexicographic, Java `String.compareTo`).
Postings within a term are ordered by DOC_ID ascending (numeric).

**N (total documents):** counted by the DF job (`DOCS` counter) and written to `doc_count.txt` → **N = 10000**.

**Output schemas**

| File | Schema | Notes |
|---|---|---|
| `df_all.tsv` | `TERM<TAB>DF` | all 524,390 distinct stemmed non-stop terms, single file, sorted by TERM |
| `top100_df.tsv` | `TERM<TAB>DF` | 100 rows ranked by (DF desc, TERM asc) |
| `inverted_index.tsv` | `TERM<TAB>DOC_ID:TF,DOC_ID:TF,...` | 100 rows sorted by TERM; #postings = DF |
| `inverted_index_sample.tsv` | same, truncated | terms ranked 1/25/50/75/100, first 10 postings each |
| `doc_count.txt` | `N` | 10000 |

---

## 3. Design and implementation

### 3.1 `WholeDocumentInputFormat` — one record per article
A `CombineFileInputFormat<Text, Text>` whose per-file reader returns the whole file as one record:
key = DOC_ID, value = article text. Files are not splittable.

Why:
- **Exact per-document semantics.** One `map()` call sees exactly one complete document, so de-duplication
  for DF and counting for TF are exact. This does not rely on "one line = one document".
- **Small-files problem.** The default `TextInputFormat` would create one split, and so one map task, per
  file: 10,000 tasks. Combining files into splits of at most 64 MB gives **3 map tasks** for the full corpus.

It follows the structure of the starter `CustomFileInputFormat` / `CustomLineRecordReader`.

### 3.2 `DocumentFrequency` (two chained jobs, `Tool`/`ToolRunner` so `-libjars` works)

Stop words are shipped with the DistributedCache (`job.addCacheFile`) and read in `Mapper.setup()`
through the Hadoop `FileSystem` API, which works in local and cluster mode alike.

**Job 1 — DF**
```
setup():   stopwords ← cached stopwords.txt ; stemmer ← new PorterStemmer()
map(docId, text):
    terms ← preprocess(text)              # list, with repeats
    for t in set(terms):                  # HashSet → each term once per document
        emit(t, 1)
combine(t, ones) = reduce(t, ones):       # int sum; each 1 means "one distinct document"
    emit(t, Σ ones)                       # = DF(t)
```
One reducer → the single TSV file the assignment asks for. The combiner is safe because every value
already stands for one distinct (term, document) pair, so summing early cannot double-count.

**Job 2 — Top-100 (top-K pattern)**
```
map(line "term\tdf"):   keep a TreeSet ordered by (df desc, term asc), trimmed to 100 entries
cleanup():              emit (null, "term\tdf") for this mapper's local top 100
reduce(null, entries):  merge into a TreeSet with the same order, trimmed to 100 → emit in rank order
```
Correct because each term appears in exactly one mapper's input, so the global top 100 is contained in
the union of the local top-100 lists.

### 3.3 `InvertedIndex`
Ships `stopwords.txt` and `top100_df.tsv` with the DistributedCache.
```
map(docId, text):
    tf ← HashMap
    for t in preprocess(text): if t in top100: tf[t] += 1     # in-mapper aggregation per document
    for (t, c) in tf: emit(t, "docId:c")                       # one record per (term, doc), not per occurrence
reduce(t, postings):                                           # 1 reducer
    collect into HashMap docId → tf          (counter DUPLICATE_DOC_IN_POSTINGS if a docId repeats)
    sort docIds numerically ; emit(t, "d1:tf1,d2:tf2,...")
    if #postings ≠ DF(t) from top100_df.tsv: counter DF_MISMATCH += 1
```

### 3.4 `PosTag.java` fix
Model path changed to the relative `opennlp-en-ud-ewt-pos-1.0-1.9.3.bin`, and the `String line = ...`
declaration uncommented. Output is saved in `postag_output.txt`. For example, `anyone → anyon` and
`documentation → document`. The `'` / `s` tokens in that output show OpenNLP's SimpleTokenizer splitting
"OpenNLP's", the same `s` artefact discussed in §7.

---

## 4. Results (full dataset, N = 10,000)

- Distinct stemmed non-stop terms: **524,390**
- Term occurrences after stop-word removal: **18,110,423**
- Distinct (term, document) pairs = Σ DF: **7,153,978**
- Total postings in the top-100 index: **615,076**

**Top 100 by DF**:

| Rank | Term | DF | | Rank | Term | DF |
|---|---|---|---|---|---|---|
| 1 | s | 9836 | | 11 | us | 8059 |
| 2 | apo | 9518 | | 12 | extern | 8008 |
| 3 | categori | 9381 | | 13 | two | 7949 |
| 4 | refer | 9340 | | 14 | includ | 7885 |
| 5 | quot | 8674 | | 15 | year | 7800 |
| 6 | other | 8415 | | 16 | first | 7689 |
| 7 | 1 | 8350 | | 17 | 3 | 7529 |
| 8 | link | 8295 | | 18 | http | 7418 |
| 9 | thi | 8190 | | 19 | fr | 7376 |
| 10 | 2 | 8105 | | 20 | time | 7349 |

The full list is in `top100_df.tsv`. Rank 100 is `major` (DF 4832).

**Ties.**
- Within the list, ties are ordered by TERM ascending. For example, `2011` and `world` both have DF 5639,
  and `nation` and `present` both have DF 5378.
- At the cut-off, DF 4832 at rank 100 is held by a single term, so on the full corpus no term is excluded
  by the tie rule.
- On the 50-article set, DF 24 at rank 100 is shared by 13 terms. 1 is kept by TERM order and 12 are excluded.

**Representative index records** (`inverted_index_sample.tsv`, first 10 postings of each):
```
s      698:39,700:95,708:2,709:8,740:10,842:7,857:8,864:148,900:44,902:91
www    698:2,700:8,708:2,740:3,842:1,857:20,864:25,900:18,902:54,1016:14
well   698:2,700:4,740:1,842:1,857:1,864:9,900:9,902:6,1016:1,1178:6
uk     698:1,700:2,740:2,842:1,857:22,864:6,900:1,902:9,1016:3,1178:1
major  698:8,700:3,709:2,740:2,842:1,864:2,902:2,1178:1,1198:3,1214:4
```
Read `698:39` as "term `s` occurs 39 times in article 698".

### Job counters and timings

| Job | Map tasks | Map input recs | Map output recs | Map output bytes | Combine out | Reduce out | Wall clock |
|---|---|---|---|---|---|---|---|
| DF (full) | 3 | 10,000 | 7,153,978 | 74,106,228 | 724,282 | 524,390 | 20.08 s (DF + Top-100) |
| Top-100 (full) | 1 | 524,390 | 100 | 944 | – | 100 | (included above) |
| Inverted index (full) | 3 | 10,000 | 615,076 | 8,134,949 | – | 100 | 11.97 s |
| DF (50 docs) | 1 | 50 | 32,659 | – | 12,776 | 12,776 | 3.73 s (DF + Top-100) |
| Inverted index (50 docs) | 1 | 50 | 3,221 | – | – | 100 | 2.41 s |

- **Timing method.** Wall clock of each `hadoop jar` command, measured in `run_problem2a.sh` with `date +%s.%N`.
  It includes JVM and job startup, reading the input and writing the output. There is no HDFS transfer
  because this is standalone mode. Single runs only: 2(a) has no timing requirement.
- **Local aggregation.** The combiner cut DF shuffle records from 7.15 M to 0.72 M (−89.9%).
  In the index job, per-document in-mapper TF aggregation emits 615,076 records instead of one per occurrence.

---

## 5. Correctness checks (all passed)

`check_problem2a.py` is independent of the Java code: plain Python, with no Hadoop and no stemmer.
Output is in `results_full/checks.txt` and `results_50/checks.txt`.

- `doc_count.txt` N equals the number of article files (10000 / 50).
- `df_all.tsv` has no duplicate terms, and 1 ≤ DF ≤ N for every term.
- `top100_df.tsv` equals the true top 100 of `df_all.tsv` under (DF desc, TERM asc), with the same DF values.
- The index contains exactly the 100 terms.
- **#postings == DF for every term.** DF counts distinct documents, so there are no duplicate document counts.
- There is no duplicate DOC_ID inside any posting list. The reducer counters `DF_MISMATCH` and
  `DUPLICATE_DOC_IN_POSTINGS` never fired. Hadoop omits counters that stay at 0.
- Every TF ≥ 1, every DOC_ID is a real article, and postings are sorted.
- **Raw-text cross-check.** Some top terms are unchanged by stemming, e.g. `american`, `area`, `both`,
  `first`, `found` and `form` (12 terms on the full corpus). For these, every article containing the raw
  token is in the postings with TF ≥ the raw count. Other words can stem to the same term, so the index
  can only be larger or equal.
- **No stop words.** Stop words are removed before stemming, and no top-100 term is spelled like a stop word.
- **Hand checks with `grep` on the 50-article set** (`results_50/handcheck.txt`):

  | Term | Raw forms grepped | MapReduce | grep |
  |---|---|---|---|
  | `includ` DF | include/includes/included/including | 42 | 42 |
  | `peopl` DF | people/peoples | 36 | 36 |
  | `includ` TF in doc 1338 | include/includes/included/including | 12 | 12 |
  | `peopl` TF in doc 113147 | people/peoples/peopled | 51 | 51 |

---

## 6. Reproducibility (paste into `problem_2.txt`)

```bash
# one-time environment (WSL Ubuntu, no root)
source ~/hadoop-env/env.sh                       # JAVA_HOME=jdk-11, HADOOP_HOME=hadoop-3.4.3
tar -xf Wikipedia-50-ARTICLES.tar
tar -xf Wikipedia-EN-20120601_ARTICLES.tar.gz    # despite the name, it is not gzip-compressed

# build + run everything for 2(a) (compiles, runs DF -> Top-100 -> InvertedIndex, copies outputs)
./run_problem2a.sh Wikipedia-50-ARTICLES          results_50
./run_problem2a.sh Wikipedia-EN-20120601_ARTICLES results_full
python3 check_problem2a.py results_full Wikipedia-EN-20120601_ARTICLES stopwords.txt

# equivalent manual commands
export HADOOP_CLASSPATH=$PWD/opennlp-tools-1.9.3.jar
mkdir classes
javac -classpath "$(hadoop classpath):opennlp-tools-1.9.3.jar" -d classes \
      TextPreprocessor.java WholeDocumentInputFormat.java DocumentFrequency.java InvertedIndex.java
jar cf problem2_index.jar -C classes .
hadoop jar problem2_index.jar DocumentFrequency -libjars opennlp-tools-1.9.3.jar \
      stopwords.txt Wikipedia-EN-20120601_ARTICLES out_df
#   -> out_df/df/part-r-00000 (df_all.tsv), out_df/top100/part-r-00000 (top100_df.tsv), out_df/doc_count.txt
hadoop jar problem2_index.jar InvertedIndex -libjars opennlp-tools-1.9.3.jar \
      stopwords.txt top100_df.tsv Wikipedia-EN-20120601_ARTICLES out_index
#   -> out_index/part-r-00000 (inverted_index.tsv)

# PosTag demo
javac -cp opennlp-tools-1.9.3.jar PosTag.java && java -cp .:opennlp-tools-1.9.3.jar PosTag
```

Fixed configuration:
- 1 reducer per job
- `mapreduce.input.fileinputformat.split.maxsize` = 64 MB (it can be overridden with `-D`)
- Top-K = 100 (`-D p2.topk=...`)

---

## 7. Limitations and observations

- **Noise terms in the top 100.** This is the most important observation. With the agreed tokenizer
  (`[^a-z0-9]+`, no length or number filter), about 45 of the 100 most frequent-by-DF terms are not
  content words:
  - 25 bare numbers: `1`, `2`, `10`, `2010`, …
  - 9 single letters: `s`, `n`, `i`, `m`, `d`, `e`, `c`, `p`, `t`. Measured on the full corpus, `s`
    mostly comes from the possessive HTML entity `&apos;s` (136,666 occurrences). Smaller sources are
    curly `’s` (7,603) and link artefacts like "junction s" (about 3,800).
  - HTML-entity residue: `quot`, `apo` from `&quot;` and `&apos;`.
  - interlanguage-link prefixes: `fr`, `pt`, `nl`, `pl`, `ru`, `sv`, `ja`, `id`, from text like `fr:Montrose`.
  - URL parts: `http`, `www`.

  These are faithful to the agreed pipeline, and we kept them so that 2(b) uses exactly the same
  preprocessing. A cleaner variant would drop 1-character and numeric tokens and decode HTML entities.
  That would have to be applied to the index *and* the query side together.
- **Stop-word list gaps.** The official `stopwords.txt` has no `this`, `us` or `use`, so `thi` (stem of
  "this") and `us` are valid terms. 47 entries contain an apostrophe (`don't`, `it's`, …), and these can
  never match a token after splitting on `[^a-z0-9]+`. We did not edit the official list.
- **Porter stems are not words**, e.g. `categori`, `histori`, `peopl`, `dai` (from "day"/"days"). This is expected.
- **Non-ASCII letters** (é, ü, CJK, …) act as separators, so `café` becomes `caf`. This is consistent
  across index and queries.
- Runs were made in **standalone mode** on one machine, so no HDFS or YARN overhead is measured.
- 2(a) was timed with single runs only, because it is not part of the scalability study.
