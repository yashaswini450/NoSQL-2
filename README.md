# DAS 839 — NoSQL Systems Assignment 2
**MapReduce, Word Association, Document Indexing, and Scalability**

**GitHub Repository:** [https://github.com/yashaswini450/NoSQL-2](https://github.com/yashaswini450/NoSQL-2)

---

## 📋 Overview
This repository contains our group's Apache Hadoop MapReduce solutions for **DAS 839 Assignment II** (`NoSQL26_Assignment-2.pdf`) on the Wikipedia dataset (`Wikipedia-EN-20120601_ARTICLES.tar.gz`):

1. **Problem 1: Word Association Mining and Scalability (14 Marks)**
   - **1(a) Frequent-Word Extraction (2 marks):** Top 50 most frequent non-stop words using Hadoop `DistributedCache` (`Top50Words.java`).
   - **1(b) Pairs Co-occurrence Approach (4 marks):** Co-occurrence counts $C(u, v)$ and conditional probabilities $P(v \mid u)$ within window distance $d = 2$, outputting top-5 associated words per vocabulary term (`WordAssociationPairs.java`).
   - **1(c) Stripes Co-occurrence Approach (3 marks):** Stripes implementation using `MapWritable`, verified for exact consistency against the Pairs approach (`WordAssociationStripes.java`).
   - **1(d) Local Aggregation & Scalability Study (5 marks):** Evaluation of 4 configurations (Pairs w/o local agg, Pairs w/ local agg, Stripes w/o local agg, Stripes w/ local agg) across nested deterministic dataset subsets (10%, 20%, 40%, 80%, 100%).
2. **Problem 2: Document Indexing and Query-Based Retrieval (12 Marks)**
   - **2(a) Document Frequency & Inverted Index (5 marks):** Stemmed corpus Document Frequency (`TERM<TAB>DF`), extraction of Top 100 DF terms, and construction of a `(DOC_ID, TF)` Inverted Index using OpenNLP `PorterStemmer` (`DocumentFrequency.java`, `InvertedIndex.java`).
   - **2(b) Query-Based Document Retrieval (7 marks):** MapReduce TF–IDF scoring and ranking pipeline outputting the top 10 matching documents (`QueryRetrieval.java`).

See [`TEAM_TASK_DIVISION.md`](./TEAM_TASK_DIVISION.md) for detailed per-teammate instructions, design specifications, and GitHub commit workflows.

---

## 👥 Team Responsibilities & Branches

| Teammate | GitHub Branch | Assigned Tasks | Key Deliverables |
|---|---|---|---|
| **Person 1 (Yashaswini)** | `feature/problem1-top50-and-pairs` | **Problem 1(a) & 1(b)** (6 Marks) | `Top50Words.java`, `WordAssociationPairs.java`, `WordCount2.java`, `top50_words.tsv`, `pairs_top5.tsv` |
| **Person 2 (Teammate 2)** | `feature/problem1-stripes-and-scalability` | **Problem 1(c) & 1(d)** (8 Marks) | `WordAssociationStripes.java`, `prepare_subsets.sh`, `run_scalability.sh`, `stripes_top5.tsv`, `scalability_results.csv`, `problem_1.txt` |
| **Person 3 (Teammate 3)** | `feature/problem2-df-and-inverted-index` | **Problem 2(a)** (5 Marks) | `PosTag.java`, `DocumentFrequency.java`, `InvertedIndex.java`, `top100_df.tsv`, `inverted_index_sample.tsv` |
| **Person 4 (Teammate 4)** | `feature/problem2-query-retrieval-and-report` | **Problem 2(b) & Report** (7 Marks + Report) | `QueryRetrieval.java`, `query_top10.tsv`, `problem_2.txt`, `REPORT.tex` / `REPORT.pdf` |

---

## 📂 Repository Structure

```text
assignment-2/
├── .gitignore                        # Excludes datasets (.tar/.tar.gz), .jar, .bin, .class, and output dirs
├── README.md                         # Project overview and quick-start guide
├── TEAM_TASK_DIVISION.md             # Detailed teammate instructions and GitHub workflow
├── stopwords.txt                     # Provided stop-word list distributed via Hadoop cache
├── WordCount.java                    # Starter baseline WordCount
├── WordCount2.java                   # Starter DistributedCache WordCount
├── CustomFileInputFormat.java        # Starter custom FileInputFormat
├── CustomLineRecordReader.java       # Starter custom LineRecordReader
├── CustomWordCount.java              # Starter WordCount with custom InputFormat
├── PosTag.java                       # Starter OpenNLP POS tagger & PorterStemmer demo
├── Top50Words.java                   # [Problem 1a] Top-50 non-stop frequent word extraction
├── WordAssociationPairs.java         # [Problem 1b/1d] Co-occurrence mining (Pairs approach)
├── WordAssociationStripes.java       # [Problem 1c/1d] Co-occurrence mining (Stripes approach)
├── DocumentFrequency.java            # [Problem 2a] Corpus Document Frequency & Top-100 terms
├── InvertedIndex.java                # [Problem 2a] Inverted Index construction for Top-100 terms
├── QueryRetrieval.java               # [Problem 2b] TF-IDF query retrieval and Top-10 ranking
├── problem_1.txt                     # Reproducibility & execution steps for Problem 1
├── problem_2.txt                     # Reproducibility & execution steps for Problem 2
├── REPORT.tex / REPORT.pdf           # Comprehensive final report (LaTeX source + PDF)
```

---

## 🚀 Quick Git Workflow

1. **Clone the repository and place LMS datasets/jars locally:**
   ```bash
   git clone https://github.com/yashaswini450/NoSQL-2.git
   cd NoSQL-2
   ```
2. **Create your assigned feature branch:**
   ```bash
   git checkout -b feature/<your-branch-name>
   ```
3. **Stage and commit only source code, scripts, and small summary TSV/docs:**
   ```bash
   git status   # Ensure no .tar, .tar.gz, .jar, .bin, or output/ directories are staged
   git add <your-files>
   git commit -m "feat: <clear summary of your changes>"
   git push -u origin feature/<your-branch-name>
   ```
4. **Open a Pull Request** on GitHub into `main`.
