# DAS 839 NoSQL Systems — Assignment 2 Report

> Draft status: Problem 2 sections are written from real runs. Parts marked **TODO (P1/P2)** wait for Person 1/2 results.
> No instructor-supplied query had been announced when this was written, so we used our own multi-term query "history united states". The pipeline accepts any query; rerun `./run_query.sh results_full "<query>"` if one is announced.

## 1. Environment and setup

| Item | Value |
|---|---|
| OS | Windows 11 + WSL2 Ubuntu |
| Java / Hadoop | OpenJDK 11 / Hadoop 3.4.3, standalone (local) mode, LocalJobRunner, local filesystem (no HDFS/YARN) |
| Library | `opennlp-tools-1.9.3.jar` (`PorterStemmer`) |
| Dataset | `Wikipedia-EN-20120601_ARTICLES.tar.gz` (a plain tar despite the name): **10,000 articles**, 166,222,646 bytes, file `<id>.txt`. Test set: `Wikipedia-50-ARTICLES.tar` (50 articles) |
| Stop words | `stopwords.txt` (355 non-empty entries) |
| Machine | TODO (fill from `results_full/environment.txt` of Person 3, and Person 2's machine if different) |
| Nested subsets (Problem 1d) | TODO (P2) |

Dataset preparation: `tar -xf` both archives; DOC_ID = file name without `.txt`.
Full commands: see `problem_1.txt` and `problem_2.txt`.

## 2. Design and implementation

### 2.1 Shared preprocessing (all of Problem 2)
`TextPreprocessor.terms()`: lowercase → split on `[^a-z0-9]+` → drop empty tokens → drop stop words
(checked on the raw token) → `PorterStemmer.stem` → drop empty stems. The same class is used by 2(a)
(documents) and 2(b) (the query), so the two sides cannot diverge. Stop words are loaded from the Hadoop
DistributedCache in 2(a). Non-ASCII letters act as separators.

### 2.2 Problem 2(a) (Person 3 — summary)
`DocumentFrequency` (job 1: emit `(term,1)` once per distinct term per document, combiner/reducer sum → DF;
job 2: top-100 with a TreeSet ordered by DF desc, TERM asc) and `InvertedIndex` (per-document in-mapper TF for
the top-100 terms, reducer writes `TERM<TAB>DOC_ID:TF,...` sorted numerically by DOC_ID). Input uses a
`WholeDocumentInputFormat` (one record per article, files combined into ≤64 MB splits → 3 map tasks).
Details, counters and timings: `Problem2a_Report.md` (TODO: merge into this report).
Tie handling: DF desc, then TERM ascending.

### 2.3 Problem 2(b) — Query-based retrieval (`QueryRetrieval.java`)

**Inputs.** `inverted_index.tsv` (MapReduce input), `top100_df.tsv` (DistributedCache, source of DF),
`N` (command line; read from `doc_count.txt`), `stopwords.txt`, query string.

**Query handling (driver).**
1. Preprocess with the shared pipeline (§2.1).
2. **Repeated terms:** the processed terms are put in a *set* `Q`, so a term repeated in the query counts once
   (matches the formula's `t ∈ Q ∩ D`). The score is **not** multiplied by query-term multiplicity.
3. **Stop words** disappear in step 1. **Terms absent from the index** (not one of the top-100 DF terms) are
   logged (`Ignored (not in the top-100 index)`) and skipped; they contribute nothing.
4. **No usable term** (e.g. a query of only stop words, or only unindexed words): the driver prints
   "no documents can match", runs no job and writes no output. If usable terms exist but no document
   contains them, the output file is empty.

**Pseudocode.**
```
driver:  Q ← set(preprocess(query)) ∩ indexedTerms(top100_df.tsv);  if Q = ∅: report and exit
         conf.query.terms ← Q;  conf.N ← N;  cacheFile ← top100_df.tsv;  1 reducer

setup():  dfByTerm ← read cached top100_df.tsv
map(line "TERM \t d1:tf1,d2:tf2,..."):
    if TERM ∉ Q: return
    idf ← ln(N / dfByTerm[TERM] + 1)
    for each posting d:tf:  emit(d, TERM ":" tf*idf)
    if #postings ≠ dfByTerm[TERM]: counter DF_MISMATCH++          # sanity check against 2(a)
reduce(DOC_ID, values):
    perTerm ← TreeMap TERM → contribution                          # summed in term order (deterministic)
    hit(doc) ← (Σ contributions, sorted matched terms)
cleanup():                                                          # single reducer ⇒ global ranking
    sort hits by score desc, then DOC_ID asc (numeric)
    write the first 10 as  RANK \t DOC_ID \t MATCHED_TERMS \t SCORE(%.6f)
```

**Design decisions.**
- *Formula unchanged:* `IDF(t)=ln(N/DF(t)+1)` (natural log), `S(D,Q)=Σ TF·IDF`; no length normalisation.
- *DF source:* DF(t) comes from Part 2(a)'s `top100_df.tsv` (as the assignment says); the mapper also checks it equals
  the number of postings in the index (counter `DF_MISMATCH` stayed 0).
- *Tie-breaking:* equal scores ordered by DOC_ID **ascending, compared numerically** (IDs are integers, and the 2(a)
  postings are numerically ordered too; "1000" ranks after "999"). Contributions are summed in sorted-term order so
  floating-point sums do not depend on shuffle order and exactly-equal scores stay exactly equal.
- *Single reducer:* required for a global top-10; the candidate set is at most N documents, so it fits in memory.
- *Output:* score printed with 6 decimals.

### 2.4 Problems 1(a)–(d) (`Top50Words.java`, `WordAssociationPairs.java`)

**Problem 1(a) — Frequent-word extraction (`Top50Words.java`):**
- **Preprocessing & DistributedCache:** `stopwords.txt` is distributed via Hadoop's `DistributedCache` (`job.addCacheFile(...)`). Each line is lowercased and split on `[^a-z0-9]+`, dropping empty tokens and stop words (`stopwords.contains(token)`), identical to the pre-stemming step of `TextPreprocessor.java` in Problem 2.
- **Aggregation & Tie-breaking:** `LongSumCombiner` aggregates word counts per map split. A single reducer (`Top50Reducer`) accumulates corpus totals and in `cleanup()` sorts all non-stop words by `(frequency DESC, word ASC)` (lexicographical tie-breaker), emitting the top 50 words as `WORD<TAB>FREQUENCY` (`top50_words.tsv`).

```
setup():   stopwords ← load cached stopwords.txt into HashSet
map(k, line):
    for token in line.toLowerCase().split("[^a-z0-9]+"):
        if token != "" and token ∉ stopwords: emit(token, 1)
combine(w, counts): emit(w, Σ counts)
reduce(w, counts):  wordCounts[w] ← Σ counts
cleanup():
    sort wordCounts by (count DESC, w ASC)
    for first 50 entries (w, c): emit(w, c)
```

**Problem 1(b) — Word association using the Pairs approach (`WordAssociationPairs.java`):**
- **Vocabulary & Window ($d = 2$):** Both `stopwords.txt` and `top50_words.tsv` are loaded via `DistributedCache`. For each record's filtered non-stop token stream $t_0, t_1, \dots, t_{n-1}$, whenever $u = t_i \in V_{50}$, we inspect neighbours $v = t_j$ within window distance $d = 2$ ($1 \le |j - i| \le 2$).
- **Pair & Denominator Convention:** Pairs $(u, v)$ are treated as **ordered** pairs where $u \in V_{50}$, $v \in V_{50}$, and $u \neq v$ (excluding self-pairs). For each focal word $u$, the denominator $\sum_{w \in V_{50}, w \neq u} C(u, w)$ is the marginal sum of co-occurrence counts of $u$ with all other Top-50 vocabulary words within distance $d = 2$. The conditional score is $P(v \mid u) = \frac{C(u, v)}{\sum_w C(u, w)}$.
- **Local Aggregation Modes (Problem 1d i–ii):**
  - *Without local aggregation (`false`/`none`):* Emits `(u\tv, 1)` inside the inner loop for every co-occurrence (`1,540,824` intermediate records on full corpus).
  - *Map-function level (`function`):* Aggregates pair counts inside a per-record `HashMap<String, Long>` and flushes at the end of `map()` (`674,998` intermediate records, $-56.2\%$).
  - *Map-class level (`true`/`class`):* Maintains an in-mapper `HashMap<String, Long>` across the split, flushes once in `Mapper.cleanup()` (then clears state to prevent double-counting), and enables `PairsCombiner` (`2,428` unique pairs per split, $-99.84\%$).
- **Problems 1(c)–(d) (Stripes & Scalability):** TODO (P2).

## 3. Correctness checks

### 3.1 Problem 2(b)
Query used (chosen by us; no instructor query announced) **Q = "history united states"** → stems `{histori, state, unit}` ("united states" is not a
stop-word phrase; none of the three words is in `stopwords.txt`). N = 10,000.

| Term | DF (`top100_df.tsv`) | IDF = ln(10000/DF + 1) |
|---|---|---|
| histori | 6657 | 0.917162 |
| state | 7299 | 0.862911 |
| unit | 6498 | 0.931745 |

9,018 documents contain at least one query term. **Top-10 (Hadoop output, `query_top10.tsv`):**

| RANK | DOC_ID | MATCHED_TERMS | SCORE |
|---|---|---|---|
| 1 | 334701 | state,unit | 580.808168 |
| 2 | 462443 | histori,state,unit | 384.904727 |
| 3 | 317144 | state,unit | 298.119407 |
| 4 | 31655 | histori,state,unit | 241.365556 |
| 5 | 18858 | histori,state,unit | 236.636569 |
| 6 | 201936 | histori,state,unit | 236.551865 |
| 7 | 62513 | histori,state,unit | 234.387576 |
| 8 | 21650 | histori,state,unit | 230.939936 |
| 9 | 560898 | state,unit | 212.701161 |
| 10 | 225102 | histori,state,unit | 208.959599 |

**Worked score calculations** (TF from `inverted_index.tsv`, IDF from the table above;
`results_2b/worked_scores_full.txt`):

- Doc **334701**: state TF 672, unit TF 1 → 672 × 0.862911 + 1 × 0.931745 = 579.876423 + 0.931745 = **580.808168**
- Doc **462443**: histori 1, state 215, unit 213 → 1 × 0.917162 + 215 × 0.862911 + 213 × 0.931745 = 0.917162 + 185.525939 + 198.461626 = **384.904727**
- Doc **317144**: state 163, unit 169 → 163 × 0.862911 + 169 × 0.931745 = 140.654549 + 157.464858 = **298.119407**

(Products are shown rounded; the totals use full double precision.)

**Validation.**
- `verify_retrieval.py` — a Hadoop-free re-implementation that reads DF from `top100_df.tsv` — produces output
  **identical** to the Hadoop run (full corpus, `diff` clean), and also on the 50-article set (N=50; top doc 1338, 46.733929).
- Counter `DF_MISMATCH` absent (= 0): postings count equals DF for every queried term.
- Edge cases run: stop-word-only query (`the of and`) → no terms, message, no job; `zzzqqq history history History` →
  `zzzqqq` ignored as absent, repeated `history` collapsed, ranking uses `histori` only; tie between docs 7174 and
  45633 (both 72.455768) is ordered numerically.
- Stop words never reach the scoring stage (removed before stemming in the shared pipeline).

**Why this ranking is reasonable (and its limits).**
The formula is applied as specified: raw TF × IDF, no length normalisation. All three terms are very common
(DF 65–73 % of the corpus), so their IDFs are low and nearly equal (0.86–0.93) and **TF dominates** the score.
That is why the #1 document (334701, a long list of numbered New York *state* highways) does not contain
"histori" at all: it uses "state" 672 times. Documents 462443 (a US-Congress table: 215 "state", 213 "unit",
1 "histori") and 317144 (an airport list with many "United States" entries) are similarly repetition-driven.
Results that match all three terms still dominate positions 4–8 and 10. This behaviour is a property of the
specified scoring function (long, repetitive documents win; common terms discriminate poorly), not of the
implementation; we did not change the formula.

### 3.2 Problem 2(a) (Person 3)
Checks from `check_problem2a.py` (independent Python): N equals file count; DF within [1,N] with no duplicate
terms; `top100_df.tsv` equals the true top-100 of `df_all.tsv`; #postings = DF for all 100 terms; no duplicate
DOC_ID in a posting list; every TF ≥ 1; raw-text cross-check; `grep` hand checks (`includ` DF 42, `peopl` DF 36 on
the 50-article set). Details in `Problem2a_Report.md`.

### 3.3 Problem 1
- **Stop-word verification (1a):** Checked all 50 terms in `top50_words.tsv` against `stopwords.txt` (355 entries); 0 stop words appear in `top50_words.tsv`. Top 5 corpus tokens: `apos` (273,057), `quot` (267,968), `s` (207,334), `http` (79,108), `1` (77,032).
- **Pairs co-occurrence & local-aggregation consistency (1b):** Verified that `WordAssociationPairs` produces identical output (`diff -u` clean across all 250 rows in `pairs_top5.tsv`) across `none`, `function`, and `class` local-aggregation modes, and that raw co-occurrence counts are symmetric ($C(u, v) = C(v, u)$, e.g. $C(\text{united}, \text{states}) = C(\text{states}, \text{united}) = 21,734$, while $P(\text{states} \mid \text{united}) = 0.664059$ and $P(\text{united} \mid \text{states}) = 0.685507$ due to different marginal denominators).
- **Pairs vs Stripes `diff` (1c):** TODO (P2).

## 4. Performance evaluation
- Problem 1(d): TODO (P2) — runtime table, median of 3 runs for 10/40/100 %, mapper output records/bytes, plot, trend analysis.
- Problem 2(a), single runs, standalone mode (Person 3): DF + Top-100 20.08 s, inverted index 11.97 s (full corpus);
  3.73 s / 2.41 s (50 articles). The combiner cut DF shuffle records from 7.15 M to 0.72 M.
- Problem 2(b): TODO — fill the wall-clock of the query job on the full corpus (single run, 5.4 MB index input);
  not part of the required scalability study.

## 5. Reproducibility
Commands, paths and parameters: `problem_1.txt` (TODO P2) and `problem_2.txt` (included in this repo).
TODO at the end: paste both files verbatim here.

## 6. Limitations
- 2(b) only searches the **100 highest-DF terms** that 2(a) indexes: any other query term is ignored, so a query
  made only of rarer terms returns nothing. This follows the assignment's definition of the index.
- The top-100 DF list contains many non-content tokens (bare numbers, single letters, HTML residue such as
  `quot`/`apo`, language prefixes, `http`/`www`) because the agreed tokenizer does not filter them (see
  `Problem2a_Report.md` §7); a query containing such a token would match them. Cleaning would have to be applied to
  index and query together.
- Score uses raw TF with no length normalisation, so long documents dominate (§3.1).
- All runs are standalone-mode on one machine; no HDFS/YARN overheads were measured.
- No instructor-supplied query was available; results use our own query "history united states". Another query needs only a rerun of `run_query.sh`.
- TODO: limitations from Problem 1 runs.
