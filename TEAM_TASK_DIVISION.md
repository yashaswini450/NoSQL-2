# DAS 839 — NoSQL Systems Assignment 2: Team Task Division & GitHub Guide

**GitHub Repository:** `https://github.com/yashaswini450/NoSQL-2`  
**Course Reference:** `NoSQL26_Assignment-2.pdf` (Total: **26 Marks**)

---

## 1. Assignment Overview (From `NoSQL26_Assignment-2.pdf`)

We use **Apache Hadoop MapReduce** on the Wikipedia corpus (`Wikipedia-EN-20120601_ARTICLES.tar.gz`, using `Wikipedia-50-ARTICLES.tar` for local testing).

### Breakdown of Problems & Marks
- **Problem 1: Word Association Mining and Scalability (14 Marks)**
  - **Part 1(a) — Frequent-word extraction (2 marks):** Find the top 50 most frequent words in the dataset, excluding all stop words (`stopwords.txt` via Hadoop DistributedCache). Output schema: `WORD<TAB>FREQUENCY`.
  - **Part 1(b) — Word association using the Pairs approach (4 marks):** Calculate co-occurrence counts $C(u, v)$ among the top 50 words within window distance $d = 2$. Report top 5 associated words for each word $u$ with raw count $C(u, v)$ and conditional probability $P(v \mid u) = \frac{C(u, v)}{\sum_w C(u, w)}$.
  - **Part 1(c) — Word association using the Stripes approach (3 marks):** Reimplement co-occurrence counting using the stripes approach (`MapWritable`). Produce the exact same output format and verify counts agree with Part 1(b).
  - **Part 1(d) — Local aggregation and scalability study (5 marks):** Compare 4 configurations (Pairs without local aggregation, Pairs with local aggregation, Stripes without local aggregation, Stripes with local aggregation) across nested deterministic subsets (**10%, 20%, 40%, 80%, 100%** of the corpus) with $d = 2$. Repeat 10%, 40%, and 100% at least 3 times (report median time + mapper output records/bytes) and plot input size vs. runtime.
- **Problem 2: Document Indexing and Query-Based Retrieval (12 Marks)**
  - Uses `PorterStemmer` from `opennlp-tools-1.9.3.jar` across all steps.
  - **Part 2(a) — Document frequency and inverted index (5 marks):**
    1. Calculate Document Frequency (DF = number of distinct documents containing the term) for each distinct non-stop stemmed term (`TERM<TAB>DF`).
    2. Identify the **top 100 terms** with highest DF (with deterministic tie-breaking).
    3. Build an **Inverted Index** for these 100 terms storing `(doc_id, TF)` pairs for every document containing the term.
  - **Part 2(b) — Query-based document retrieval (7 marks):**
    - MapReduce pipeline that accepts a multi-term query $Q$, preprocesses/stems query terms, and ranks documents using:
      $$\text{IDF}(t) = \log\left(\frac{N}{\text{DF}(t)} + 1\right), \quad S(D, Q) = \sum_{t \in Q \cap D} \text{TF}(t, D) \times \text{IDF}(t)$$
      (natural logarithm $\ln$, where $N$ is total documents in corpus).
    - Output top 10 documents in descending score order: `RANK<TAB>DOC_ID<TAB>MATCHED_TERMS<TAB>SCORE`.
- **Required Reproducibility & Submission Files:**
  - `problem_1.txt` — Environment, commands, configuration, and steps to run Problem 1.
  - `problem_2.txt` — Environment, commands, configuration, and steps to run Problem 2.
  - `REPORT.md` (and final PDF report) — Covering all 6 required report sections.

---

## 2. Shared Preprocessing Standard (CRITICAL — Everyone Must Follow!)

The assignment states: *"Use consistent tokenization, case normalization, stop-word filtering, and document identification throughout the assignment."*

To ensure everyone's code produces compatible outputs:
1. **Case Normalization:** Convert every line to lowercase first (`line = line.toLowerCase()`).
2. **Tokenization & Punctuation Removal:** Split on non-word characters: `line.split("[^a-z0-9]+")` (or `line.split("[^\\w']+")` stripped of leading/trailing apostrophes; ignore empty strings `token.isEmpty()`).
3. **Stop-Word Filtering:** Load `stopwords.txt` into a `HashSet<String>` in `Mapper.setup()` via Hadoop DistributedCache (`job.addCacheFile(...)`). Check `if (stopwords.contains(token)) continue;`.
   - ⚠️ **Note on `WordCount2.java` bug:** The starter `WordCount2.java` does `line.replaceAll(pattern, "")` which corrupts substrings inside words! Replace that with `HashSet.contains(token)` after splitting into tokens.
4. **Pair-Counting & Tie-Breaking Conventions:**
   - **Window distance $d = 2$:** For token at index $i$ in a line/document, examine neighbours at indices $j \in [i - d, i + d], j \neq i$ (ordered pairs $(w_i, w_j)$ where both $w_i, w_j \in \text{Top50}$). Looking both left and right within distance $d=2$ makes ordered pairs symmetric and easy to implement in both Pairs and Stripes.
   - **Tie-Breaking:** Whenever sorting by frequency or score (Top 50 words, Top 5 associations, Top 100 DF terms, Top 10 documents), break ties deterministically in **ascending lexicographical order** of `WORD` / `TERM` / `DOC_ID`.
5. **Document ID:** Use the article filename (e.g., `563203.txt` or `563203`) obtained from `((FileSplit) context.getInputSplit()).getPath().getName()`.

---

## 3. Starter Files Provided in the Workspace

| Starter File | How We Use It |
|---|---|
| `WordCount.java` | Reference baseline for MapReduce mapper/reducer/combiner structure |
| `WordCount2.java` | Reference for `DistributedCache` (`addCacheFile` / `getCacheFiles`) with `stopwords.txt` → basis for **Problem 1(a)** |
| `CustomFileInputFormat.java` & `CustomLineRecordReader.java` | Reference for custom `InputFormat` / `FileSplit` handling |
| `PosTag.java` | Reference for `PorterStemmer` from `opennlp-tools-1.9.3.jar` → basis for **Problem 2(a) & 2(b)** |
| `stopwords.txt` | Official stop-word list (must be committed to GitHub) |
| `opennlp-tools-1.9.3.jar` | OpenNLP library containing `PorterStemmer` (keep locally, ignored by `.gitignore`) |
| `Wikipedia-50-ARTICLES.tar` | Small 50-article dataset for fast testing (keep locally, ignored by `.gitignore`) |
| `Wikipedia-EN-20120601_ARTICLES.tar.gz` | Full Wikipedia dataset for final runs (keep locally, ignored by `.gitignore`) |

---

## 4. Work Division Across the 4 Teammates

| Teammate | Branch Name | Assigned Problems & Marks | Deliverables to Commit |
|---|---|---|---|
| **Person 1 (Yashaswini)** | `feature/problem1-top50-and-pairs` | **Problem 1(a) & 1(b)** (6 Marks) | `Top50Words.java`, `WordAssociationPairs.java`, `WordCount2.java` (fixed), `top50_words.tsv`, `pairs_top5.tsv` |
| **Person 2 (Teammate 2)** | `feature/problem1-stripes-and-scalability` | **Problem 1(c) & 1(d)** (8 Marks) | `WordAssociationStripes.java`, `prepare_subsets.sh`, `run_scalability.sh`, `stripes_top5.tsv`, `scalability_results.csv`, `problem_1.txt` |
| **Person 3 (Teammate 3)** | `feature/problem2-df-and-inverted-index` | **Problem 2(a)** (5 Marks) | `PosTag.java` (fixed), `DocumentFrequency.java`, `InvertedIndex.java`, `top100_df.tsv`, `inverted_index_sample.tsv` |
| **Person 4 (Teammate 4)** | `feature/problem2-query-retrieval-and-report` | **Problem 2(b) + Report** (7 Marks + Report) | `QueryRetrieval.java`, `query_top10.tsv`, `problem_2.txt`, `REPORT.md` |

---

## 5. Detailed Instructions for Each Teammate

---

### 👤 Person 1 — Yashaswini
**Branch:** `feature/problem1-top50-and-pairs`  
**Tasks:** Problem 1(a) Frequent-Word Extraction (2 marks) + Problem 1(b) Pairs Co-occurrence & Local Aggregation (4 marks)

#### What to build:
1. **Fix `WordCount2.java` & Create `Top50Words.java` (Problem 1a):**
   - Load `stopwords.txt` via Hadoop DistributedCache (`job.addCacheFile(new Path(stopwordsPath).toUri())`).
   - In `Mapper.setup()`, read all lines from the cached `stopwords.txt` into a `HashSet<String>` (trimmed, lowercase).
   - Tokenize lowercase lines using `line.toLowerCase().split("[^a-z0-9]+")`. Skip empty tokens and tokens in `stopwords`.
   - Aggregate total word counts across the corpus, then sort by `(frequency DESC, word ASC)` and output the **top 50 words** in schema:
     ```
     WORD<TAB>FREQUENCY
     ```
   - Save the output as `top50_words.tsv` and share it with **Person 2** immediately so Person 2 uses the exact same 50-word vocabulary.
2. **Create `WordAssociationPairs.java` (Problem 1b & 1d-i/ii):**
   - Load both `stopwords.txt` and `top50_words.tsv` via DistributedCache.
   - Filter each line/document down to non-stop tokens (Wait: check whether window distance $d=2$ is measured over all non-stop tokens or raw tokens — document this clearly and keep it identical in `WordAssociationStripes.java`!).
   - For each token $w_i$ at index $i$ that is in the Top-50 vocabulary, look at neighbours $w_j$ within window distance $d = 2$ ($1 \le |j - i| \le 2$). If $w_j$ is also in the Top-50 vocabulary, emit pair key `"u\tv"` and count `1`.
   - Support a CLI flag `--local-agg` (or `-combiner` + in-mapper combining `HashMap<String, Integer>` flushed in `cleanup()`) so **Problem 1(d)** can run both **Pairs without local aggregation** and **Pairs with local aggregation**.
   - In the Reducer (or post-processing step), compute:
     - Raw co-occurrence count $C(u, v)$
     - Marginal sum $\sum_w C(u, w)$
     - Conditional score $P(v \mid u) = \frac{C(u, v)}{\sum_w C(u, w)}$
   - Output the 5 strongest associated words $v$ for each of the 50 words $u$ to `pairs_top5.tsv`.

#### Commands to compile & test:
```bash
javac -classpath $(hadoop classpath) Top50Words.java WordAssociationPairs.java
jar cf problem1_pairs.jar Top50Words*.class WordAssociationPairs*.class

# Run 1(a) on small dataset first, then full dataset
hadoop jar problem1_pairs.jar Top50Words stopwords.txt Wikipedia-50-ARTICLES/ output_top50

# Run 1(b) Pairs (d=2) without and with local aggregation
hadoop jar problem1_pairs.jar WordAssociationPairs stopwords.txt top50_words.tsv Wikipedia-50-ARTICLES/ output_pairs_noagg false
hadoop jar problem1_pairs.jar WordAssociationPairs stopwords.txt top50_words.tsv Wikipedia-50-ARTICLES/ output_pairs_agg true
```

#### What Person 1 commits to GitHub:
```bash
git checkout -b feature/problem1-top50-and-pairs
git add WordCount2.java Top50Words.java WordAssociationPairs.java top50_words.tsv pairs_top5.tsv
git commit -m "feat(problem1): implement Top-50 frequent word extraction and Pairs co-occurrence (1a, 1b)"
git push -u origin feature/problem1-top50-and-pairs
```

---

### 👤 Person 2 — Teammate 2
**Branch:** `feature/problem1-stripes-and-scalability`  
**Tasks:** Problem 1(c) Stripes Co-occurrence (3 marks) + Problem 1(d) Local Aggregation & Scalability Study (5 marks) + `problem_1.txt`

#### What to build:
1. **Create `WordAssociationStripes.java` (Problem 1c & 1d-iii/iv):**
   - Use the **exact same tokenization, window $d=2$, and `top50_words.tsv`** as Person 1.
   - Instead of emitting individual pairs $(u, v)$, for each focal word $u \in \text{Top50}$, build an associative array (`MapWritable`) mapping each co-occurring neighbour $v \in \text{Top50}$ within distance $d=2$ to its count.
   - Support two modes via a flag (`false` / `true`):
     - **Stripes without local aggregation (1d-iii):** Emit `(u, MapWritable)` per token occurrence (map-function level).
     - **Stripes with local aggregation (1d-iv):** Maintain an in-mapper `Map<String, Map<String, Integer>>` across the entire split and flush once in `Mapper.cleanup()` + enable Combiner.
   - In the Reducer, sum all stripes for key $u$, compute $C(u, v)$ and $P(v \mid u) = \frac{C(u, v)}{\sum_w C(u, w)}$, and output the top 5 associated words per $u$ to `stripes_top5.tsv`.
   - Verify with `diff pairs_top5.tsv stripes_top5.tsv` that the counts and scores match Part 1(b)!
2. **Create `prepare_subsets.sh` & `run_scalability.sh` (Problem 1d):**
   - **Nested deterministic subsets:** Sort all article filenames in `Wikipedia-EN-20120601_ARTICLES/` deterministically (`ls | sort`) and take the first **10%, 20%, 40%, 80%, and 100%** of files so that $S_{10\%} \subset S_{20\%} \subset S_{40\%} \subset S_{80\%} \subset S_{100\%}$. Record the exact number of documents and bytes in each subset.
   - Run all 4 configurations (`Pairs-NoAgg`, `Pairs-Agg`, `Stripes-NoAgg`, `Stripes-Agg`) across all 5 subsets with $d=2$.
   - Repeat the **10%, 40%, and 100%** runs **3 times** and record the median wall-clock execution time, plus Hadoop job counter metrics (`Map output records` and `Map output bytes`) in `scalability_results.csv`.
   - Write `problem_1.txt` documenting the exact environment, commands, configuration, and steps to run Problem 1(a)–(d).

#### What Person 2 commits to GitHub:
```bash
git checkout -b feature/problem1-stripes-and-scalability
git add WordAssociationStripes.java prepare_subsets.sh run_scalability.sh stripes_top5.tsv scalability_results.csv problem_1.txt
git commit -m "feat(problem1): implement Stripes co-occurrence and scalability benchmark suite (1c, 1d)"
git push -u origin feature/problem1-stripes-and-scalability
```

---

### 👤 Person 3 — Teammate 3
**Branch:** `feature/problem2-df-and-inverted-index`  
**Tasks:** Problem 2(a) Document Frequency & Inverted Index (5 marks) + Fix `PosTag.java`

#### What to build:
1. **Fix `PosTag.java`:**
   - Uncomment line 18 (`String line = ...`) and change the hardcoded path on line 15 to `"opennlp-en-ud-ewt-pos-1.0-1.9.3.bin"`.
2. **Create `DocumentFrequency.java` (Problem 2a — Part 1):**
   - Use `stopwords.txt` via DistributedCache and `PorterStemmer` from `opennlp-tools-1.9.3.jar`:
     ```java
     import opennlp.tools.stemmer.PorterStemmer;
     PorterStemmer stemmer = new PorterStemmer();
     String stemmed = stemmer.stem(token);
     ```
   - Obtain the document ID in the Mapper using:
     ```java
     FileSplit split = (FileSplit) context.getInputSplit();
     String docId = split.getPath().getName();
     ```
   - **Important (DF counts distinct documents, NOT total occurrences):** Within each document, keep a `Set<String>` of unique non-stop stemmed terms seen in that document (or emit `(term, docId)` and deduplicate in Reducer).
   - Output all terms and their DF in a single TSV file (`TERM<TAB>DF`), and extract the **Top 100 terms with highest DF** (ties broken alphabetically by `TERM`) into `top100_df.tsv`.
3. **Create `InvertedIndex.java` (Problem 2a — Part 2):**
   - Load `stopwords.txt` and `top100_df.tsv` via DistributedCache.
   - For each document (`docId`), count the Term Frequency $\text{TF}(t, D)$ of any stemmed term $t$ belonging to the Top 100 terms.
   - Reducer outputs the inverted index for each of the 100 terms in a clean, parseable TSV schema (share this schema with **Person 4**!), e.g.:
     ```
     TERM<TAB>DOC_ID1:TF1,DOC_ID2:TF2,...
     ```
   - Save representative records in `inverted_index_sample.tsv` for the report and share the full `inverted_index.tsv` and `top100_df.tsv` with Person 4.

#### Commands to compile & run:
```bash
javac -classpath $(hadoop classpath):opennlp-tools-1.9.3.jar PosTag.java DocumentFrequency.java InvertedIndex.java
jar cf problem2_index.jar DocumentFrequency*.class InvertedIndex*.class

hadoop jar problem2_index.jar DocumentFrequency \
  -libjars opennlp-tools-1.9.3.jar \
  stopwords.txt Wikipedia-50-ARTICLES/ output_df

hadoop jar problem2_index.jar InvertedIndex \
  -libjars opennlp-tools-1.9.3.jar \
  stopwords.txt top100_df.tsv Wikipedia-50-ARTICLES/ output_inverted_index
```

#### What Person 3 commits to GitHub:
```bash
git checkout -b feature/problem2-df-and-inverted-index
git add PosTag.java DocumentFrequency.java InvertedIndex.java top100_df.tsv inverted_index_sample.tsv
git commit -m "feat(problem2): implement stemmed Document Frequency and Top-100 Inverted Index (2a)"
git push -u origin feature/problem2-df-and-inverted-index
```

---

### 👤 Person 4 — Teammate 4
**Branch:** `feature/problem2-query-retrieval-and-report`  
**Tasks:** Problem 2(b) Query-Based Document Retrieval (7 marks) + `problem_2.txt` + Final Report (`REPORT.md`)

#### What to build:
1. **Create `QueryRetrieval.java` (Problem 2b):**
   - Accepts a multi-term query string $Q$ from the command line, the total number of documents $N$ in the dataset ($N = 50$ for small dataset, or total file count in full dataset), `stopwords.txt`, and `top100_df.tsv` via DistributedCache, taking the Inverted Index directory from Part 2(a) as MapReduce input.
   - Preprocess the query $Q$ using the **exact same pipeline** as Part 2(a): lowercase → split on non-alphanumeric → remove stop words → stem each token with `PorterStemmer` → deduplicate into a set of unique query terms $Q$ (explicitly document in the report that repeated query terms are deduplicated into set $Q$ as defined by $t \in Q \cap D$).
   - Ignore query terms that are stop words or absent from the Top-100 index (and log/handle gracefully if no query terms match).
   - **Mapper:** Reads lines of the Inverted Index (`TERM<TAB>DOC_ID1:TF1,DOC_ID2:TF2,...`). If `TERM` $\in Q$, compute:
     $$\text{IDF}(t) = \ln\left(\frac{N}{\text{DF}(t)} + 1\right)$$
     using `Math.log((double) N / df + 1.0)`. For each posting `DOC_ID:TF`, emit key `DOC_ID` and value `"TERM:SCORE_CONTRIBUTION"` where contribution is $\text{TF}(t, D) \times \text{IDF}(t)$.
   - **Reducer:** For each `DOC_ID`, sum the contributions across all matched query terms to get $S(D, Q)$, collect the sorted list of matched terms `MATCHED_TERMS` (e.g., comma-separated), and sort all matching documents by:
     1. `SCORE` descending
     2. `DOC_ID` ascending (deterministic tie-breaking rule)
   - Output the **top 10 documents** (or all matching documents if fewer than 10 match) in TSV format:
     ```
     RANK<TAB>DOC_ID<TAB>MATCHED_TERMS<TAB>SCORE
     ```
2. **Create `problem_2.txt`:**
   - Document the environment, compilation & execution commands, configuration parameters, and steps to run Problem 2(a) and 2(b).
3. **Write `REPORT.md` (and export final PDF):**
   - Include all **6 Required Report Sections** from page 4 of `NoSQL26_Assignment-2.pdf`:
     1. **Environment and setup:** Java & Hadoop versions, machine specs, configuration, dataset preparation, and nested subset selection method.
     2. **Design and implementation:** Mapper/reducer logic, pseudocode, data structures, tokenization, stop-word filtering, stemming, local aggregation strategy, and pair/denominator conventions.
     3. **Correctness checks:** Representative outputs, verification that stop words do not appear, check that DF counts distinct documents (no duplicate doc counts), consistency verification between Pairs and Stripes, and manual worked score calculation for at least 3 retrieved documents in Problem 2(b).
     4. **Performance evaluation:** Runtime tables, scalability plot (input size vs. runtime for the 4 configurations across 10%, 20%, 40%, 80%, 100%), mapper output records/bytes, median over 3 runs (10%, 40%, 100%), and analysis of trends.
     5. **Reproducibility:** Exact commands, input/output paths, and contents of `problem_1.txt` and `problem_2.txt`.
     6. **Limitations:** Any incomplete runs, hardware/storage constraints, or unexpected observations.

#### What Person 4 commits to GitHub:
```bash
git checkout -b feature/problem2-query-retrieval-and-report
git add QueryRetrieval.java query_top10.tsv problem_2.txt REPORT.md
git commit -m "feat(problem2): implement TF-IDF query retrieval pipeline and add comprehensive report (2b)"
git push -u origin feature/problem2-query-retrieval-and-report
```

---

## 6. Step-by-Step GitHub Workflow (How & What to Commit)

### Rule 1: What NEVER to Commit to GitHub
The `.gitignore` file blocks large binaries and temporary directories automatically, but **always verify with `git status`** before committing!
- ❌ **DO NOT commit:**
  - `Wikipedia-50-ARTICLES.tar` or `Wikipedia-EN-20120601_ARTICLES.tar.gz` (174 MB — will exceed GitHub's 100 MB file limit!)
  - Extracted article folders (`Wikipedia-50-ARTICLES/`, `Wikipedia-EN-20120601_ARTICLES/`, `subsets/`)
  - `.jar` files (`opennlp-tools-1.9.3.jar`, compiled job jars)
  - `.bin` files (`opennlp-en-ud-ewt-pos-1.0-1.9.3.bin`)
  - `.class` files or Hadoop `output*/` directories
- ✅ **DO commit:**
  - `.java` source files
  - `.sh` or `.py` automation/plotting scripts
  - `stopwords.txt`
  - Small result TSV/CSV files (`top50_words.tsv`, `pairs_top5.tsv`, `stripes_top5.tsv`, `scalability_results.csv`, `top100_df.tsv`, `inverted_index_sample.tsv`, `query_top10.tsv`)
  - `problem_1.txt`, `problem_2.txt`, `README.md`, `TEAM_TASK_DIVISION.md`, `REPORT.md`, and plot images (`.png`)

---

### Step A: Initial Repo Setup (Person 1 — Yashaswini does this ONCE)
```bash
cd /Users/yashaswini/NoSQL-1/assignment-2

git init
git remote add origin https://github.com/yashaswini450/NoSQL-2.git
git add .gitignore README.md TEAM_TASK_DIVISION.md *.java stopwords.txt
git status   # Verify NO .tar, .tar.gz, .jar, or .bin files are staged!
git commit -m "chore: initial commit with starter files, stopwords, and assignment task division"
git branch -M main
git push -u origin main
```

### Step B: Cloning the Repo (Teammates 2, 3, and 4)
```bash
git clone https://github.com/yashaswini450/NoSQL-2.git
cd NoSQL-2
```
*(After cloning, copy `Wikipedia-50-ARTICLES.tar`, `Wikipedia-EN-20120601_ARTICLES.tar.gz`, `opennlp-tools-1.9.3.jar`, and `opennlp-en-ud-ewt-pos-1.0-1.9.3.bin` into your local `NoSQL-2/` folder from LMS.)*

### Step C: Daily Branch, Commit, Push & Pull Request Workflow (Everyone)
```bash
# 1. Make sure your main branch is up to date
git checkout main
git pull origin main

# 2. Create and switch to your assigned feature branch
git checkout -b feature/<your-assigned-branch>

# 3. Write and test your code, then check what files changed
git status

# 4. Stage ONLY your source code, scripts, and small result files
git add <file1> <file2>

# 5. Commit with a clear, descriptive message
git commit -m "feat: <short description of what you implemented>"

# 6. Push your branch to GitHub
git push -u origin feature/<your-assigned-branch>

# 7. Go to https://github.com/yashaswini450/NoSQL-2/pulls and open a Pull Request into main!
```

---

## 7. Final LMS Submission Checklist (Before Zipping)

Before one team member zips the final submission (`<RollNumber>.zip`), verify:
- [ ] All 4 Pull Requests are merged into `main`.
- [ ] `problem_1.txt` and `problem_2.txt` are present and list all environment details, commands, and execution steps.
- [ ] All `.java` source files compile cleanly and produce reproducible results.
- [ ] Output TSV files for 1(a), 1(b), 1(c), 1(d), 2(a), and 2(b) are included.
- [ ] Final report (`REPORT.pdf`) includes program logic, pseudocode, design decisions, worked score calculations for 3 documents, runtime tables, scalability plot, and screenshots/logs.
