#!/usr/bin/env python3
"""Independent correctness checks for the Problem 2(a) outputs (no Hadoop, no stemmer needed).

Usage: python3 check_problem2a.py <results_dir> <input_dir> <stopwords.txt>

Checks
  1. doc_count.txt equals the number of article files in <input_dir>.
  2. df_all.tsv: unique terms, 1 <= DF <= N.
  3. top100_df.tsv: equals the true top 100 of df_all.tsv under (DF desc, TERM asc), same DF values.
  4. inverted_index.tsv: same 100 terms; #postings == DF; no duplicate DOC_ID per term; TF >= 1;
     every DOC_ID is a real article; postings sorted by DOC_ID.
  5. Raw-text cross-check: for top-100 terms that the stemmer leaves unchanged (e.g. "time"), every
     article containing that raw token must appear in the postings with TF >= the raw token count
     (other words may stem to the same term, so the index can only be larger or equal).
  6. Stop-word report: top-100 terms that are spelled like a stop word (only possible as the stem of
     a non-stop word, since stop words are removed before stemming).
"""
import os
import re
import sys
from collections import Counter

results, input_dir, stopwords_path = sys.argv[1:4]
failures = []


def check(cond, msg):
    print(("PASS  " if cond else "FAIL  ") + msg)
    if not cond:
        failures.append(msg)


def read_tsv(name):
    with open(os.path.join(results, name), encoding="utf-8") as f:
        return [l.rstrip("\n").split("\t") for l in f if l.strip() and not l.startswith("#")]


stopwords = {l.strip().lower() for l in open(stopwords_path, encoding="utf-8") if l.strip()}
docs = {f[:-4] for f in os.listdir(input_dir) if f.endswith(".txt")}
n = int(open(os.path.join(results, "doc_count.txt")).read().strip())

# 1
check(n == len(docs), f"doc_count.txt N={n} equals number of article files ({len(docs)})")

# 2
df_rows = read_tsv("df_all.tsv")
df = {t: int(d) for t, d in df_rows}
check(len(df) == len(df_rows), f"df_all.tsv has {len(df_rows)} rows and no duplicate terms")
check(all(1 <= d <= n for d in df.values()), "every DF is between 1 and N")

# 3
top = [(t, int(d)) for t, d in read_tsv("top100_df.tsv")]
expected_top = sorted(df.items(), key=lambda kv: (-kv[1], kv[0]))[:100]
check(top == expected_top, f"top100_df.tsv ({len(top)} rows) equals top 100 of df_all.tsv by (DF desc, TERM asc)")
if top:
    cutoff = top[-1][1]
    tied = sorted(t for t, d in df.items() if d == cutoff)
    inside = [t for t, d in top if d == cutoff]
    print(f"INFO  boundary tie: DF={cutoff} at rank {len(top)} is shared by {len(tied)} term(s); "
          f"{len(inside)} included by TERM asc, {len(tied) - len(inside)} excluded")

# 4
index = {}
for term, postings in read_tsv("inverted_index.tsv"):
    index[term] = [(p.rsplit(":", 1)[0], int(p.rsplit(":", 1)[1])) for p in postings.split(",")]
top_df = dict(top)
check(set(index) == set(top_df), f"inverted_index.tsv has exactly the {len(top_df)} top terms")
check(all(len(index[t]) == top_df[t] for t in index), "#postings == DF for every term")
check(all(len({d for d, _ in p}) == len(p) for p in index.values()), "no duplicate DOC_ID within a posting list")
check(all(tf >= 1 for p in index.values() for _, tf in p), "every TF >= 1")
check(all(d in docs for p in index.values() for d, _ in p), "every DOC_ID is a real article")
check(all([int(d) for d, _ in p] == sorted(int(d) for d, _ in p) for p in index.values()),
      "postings sorted by DOC_ID ascending")

# 5
fixed_point_terms = [t for t in index if t.isalpha() and len(t) >= 4][:15]
raw_counts = {t: Counter() for t in fixed_point_terms}
for doc in docs:
    with open(os.path.join(input_dir, doc + ".txt"), encoding="utf-8", errors="replace") as f:
        tokens = Counter(tok for tok in re.split(r"[^a-z0-9]+", f.read().lower()) if tok)
    for t in fixed_point_terms:
        if tokens[t]:
            raw_counts[t][doc] = tokens[t]
checked = 0
for t in fixed_point_terms:
    tf = dict(index[t])
    # Only meaningful if t is unchanged by stemming, i.e. the raw token appears in the index's documents.
    if not raw_counts[t] or not any(d in tf for d in raw_counts[t]):
        continue
    checked += 1
    ok = all(d in tf and tf[d] >= c for d, c in raw_counts[t].items())
    check(ok, f"raw token '{t}': all {len(raw_counts[t])} docs containing it are indexed with TF >= raw count")
print(f"INFO  raw-text cross-check run for {checked} term(s)")

# 6
lookalikes = [t for t, _ in top if t in stopwords]
print(f"INFO  top-100 terms spelled like a stop word (stems of non-stop words): {lookalikes or 'none'}")

print("\nRESULT: " + ("ALL CHECKS PASSED" if not failures else f"{len(failures)} CHECK(S) FAILED"))
sys.exit(1 if failures else 0)
