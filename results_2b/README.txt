Problem 2(b) outputs for the PROVISIONAL query "history united states" (replace when the instructor's query is confirmed).
  query_top10_full.tsv      Hadoop output on the full corpus (N=10000)  == ../query_top10.tsv
  worked_scores_full.txt    independent Python re-computation (verify_retrieval.py): DF, IDF, TF x IDF for the top 3 docs
  worked_scores_50.txt      same for the 50-article test set (N=50)
Hadoop output and the Python scorer were diffed and are identical (full corpus).
