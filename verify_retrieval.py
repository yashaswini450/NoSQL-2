"""Independent (non-Hadoop) scorer: cross-checks QueryRetrieval and prints worked score calculations.

Usage: python verify_retrieval.py <results_dir> "<stem1,stem2,...>" [--explain K]
  <results_dir> holds inverted_index.tsv, top100_df.tsv, doc_count.txt (Part 2(a) outputs).
  Pass the already-stemmed, stop-word-free query terms (QueryRetrieval prints them as "Terms used for scoring").
stdout = RANK DOC_ID MATCHED_TERMS SCORE (same as the Hadoop output); --explain K prints, on stderr,
the per-term TF x IDF breakdown of the top K documents (for the report's worked examples).
"""
import math, sys, os
from collections import defaultdict

d, terms = sys.argv[1], set(sys.argv[2].split(","))
explain = int(sys.argv[sys.argv.index("--explain") + 1]) if "--explain" in sys.argv else 0
n = int(open(os.path.join(d, "doc_count.txt")).read().strip())
df = {l.split("\t")[0]: int(l.split("\t")[1]) for l in open(os.path.join(d, "top100_df.tsv"), encoding="utf-8") if l.strip()}
idf = {t: math.log(n / df[t] + 1) for t in terms if t in df}
tf = defaultdict(dict)          # doc -> term -> tf
for line in open(os.path.join(d, "inverted_index.tsv"), encoding="utf-8"):
    term, _, post = line.rstrip("\n").partition("\t")
    if term not in idf:
        continue
    posts = [p for p in post.split(",") if p]
    assert len(posts) == df[term], f"DF mismatch for {term}: {len(posts)} postings vs DF {df[term]}"
    for p in posts:
        doc, c = p.rsplit(":", 1)
        tf[doc][term] = int(c)
for t in sorted(idf):
    print(f"# N={n}  {t}: DF={df[t]}  IDF=ln({n}/{df[t]}+1)={idf[t]:.6f}", file=sys.stderr)
score = {doc: sum(c * idf[t] for t, c in sorted(m.items())) for doc, m in tf.items()}
key = lambda x: (-x[1], int(x[0]) if x[0].isdigit() else float("inf"), x[0])
ranked = sorted(score.items(), key=key)
print(f"# matching documents: {len(ranked)}", file=sys.stderr)
for r, (doc, s) in enumerate(ranked[:10], 1):
    print(f"{r}\t{doc}\t{','.join(sorted(tf[doc]))}\t{s:.6f}")
    if r <= explain:
        parts = " + ".join(f"{c}x{idf[t]:.6f}[{t}]" for t, c in sorted(tf[doc].items()))
        print(f"#   doc {doc}: {parts} = {s:.6f}", file=sys.stderr)
