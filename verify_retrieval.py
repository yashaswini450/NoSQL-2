"""Independent (non-Hadoop) scorer to cross-check QueryRetrieval output and to
produce worked score calculations for the report.

Usage: python verify_retrieval.py inverted_index.tsv N "stemmed terms comma-separated"
(pass already-stemmed query terms, e.g. "state,unit")
"""
import math, sys
from collections import defaultdict

idx_path, n, terms = sys.argv[1], int(sys.argv[2]), set(sys.argv[3].split(","))
score, matched = defaultdict(float), defaultdict(list)
for line in open(idx_path, encoding="utf-8"):
    term, _, post = line.rstrip("\n").partition("\t")
    if term not in terms:
        continue
    posts = [p for p in post.split(",") if p]
    idf = math.log(n / len(posts) + 1)
    print(f"# {term}: DF={len(posts)} IDF={idf:.6f}", file=sys.stderr)
    for p in posts:
        doc, tf = p.rsplit(":", 1)
        score[doc] += int(tf) * idf
        matched[doc].append(term)
for r, (d, s) in enumerate(sorted(score.items(), key=lambda x: (-x[1], x[0]))[:10], 1):
    print(f"{r}\t{d}\t{','.join(sorted(matched[d]))}\t{s:.6f}")
