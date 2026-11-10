# Problem 1(c) and 1(d) results

## Correctness

Running `Top50Words` on the prepared full corpus reproduced the 50 published words and frequencies. Each of the four configurations produced identical output for each subset and trial. The full Stripes TSV also matched Yashaswini’s published Pairs TSV. It contains 250 rows with five associations for each of the 50 focal words. Same-word associations are excluded from the counts and denominators.

An independent recount from the original article files reproduced the vocabulary and the ranked counts and scores at all five subset sizes. The mapper counters in all 44 Hadoop logs matched the trial CSV.

## Wall-clock runtime

Hadoop 3.4.3 ran in standalone mode. See `problem_1.txt` for setup and commands. The subset sizes are the actual Hadoop input bytes, expressed in MiB. The 10%, 40%, and 100% cells are medians of three runs. The 20% and 80% cells each come from one run.

| Subset | Articles | MiB | Pairs without aggregation (s) | Pairs with aggregation (s) | Stripes without aggregation (s) | Stripes with aggregation (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10% | 1,000 | 10.51 | 7.64 | 7.70 | 8.70 | 7.77 |
| 20% | 2,000 | 26.37 | 11.70 | 10.84 | 12.95 | 10.94 |
| 40% | 4,000 | 61.72 | 17.88 | 16.33 | 19.77 | 16.17 |
| 80% | 8,000 | 124.91 | 35.58 | 28.51 | 33.71 | 27.30 |
| 100% | 10,000 | 158.53 | 34.27 | 30.49 | 38.44 | 30.24 |

## Mapper output on the full corpus

| Configuration | Median records | Median bytes |
| --- | ---: | ---: |
| Pairs-NoAgg | 1,540,824 | 24,567,766 |
| Pairs-Agg | 23,526 | 392,606 |
| Stripes-NoAgg | 1,169,590 | 31,272,291 |
| Stripes-Agg | 500 | 342,149 |

## Interpretation

Input grew by 15.1× from the smallest subset to the full corpus. Across the four configurations, runtime grew by 3.9× to 4.5×, so it did not rise in direct proportion to input bytes. Startup is included in every job and takes up more of the smallest runs. At 80%, Pairs without aggregation took 35.58 s, compared with the 100% median of 34.27 s. The 80% value comes from a single run.

Stripes with aggregation had the fastest full-corpus median at 30.24 s, close to Pairs with aggregation at 30.49 s. Aggregation cut the Stripes median by 21.3% and mapper output bytes by 98.9%. The mapper emitted 500 stripes in aggregation mode, compared with 1,169,590 without it. Without aggregation, Stripes emitted fewer records than Pairs but used more bytes because each record stored a map of neighbor counts.

The runs used one machine and a local filesystem. Each measured runtime includes Hadoop startup and writing the result. Preparation and compilation are outside the timer. Mapper bytes count serialized emissions, so use the shuffle counter in `problem1_hadoop_counters.txt` for the separate amount shuffled locally.

`scalability_runs.csv` contains all 44 measurements. `scalability_results.csv` contains the 20 summary rows and the mapper counters for every input size. `scalability.png` plots those runtime summaries. `subsets.tsv` gives the exact document and byte counts.
