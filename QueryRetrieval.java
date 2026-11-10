import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Problem 2(b): TF-IDF query retrieval over the Part 2(a) inverted index.
 *
 * Usage:
 *   hadoop jar problem2_query.jar QueryRetrieval -libjars opennlp-tools-1.9.3.jar \
 *       <stopwords.txt> <top100_df.tsv> <inverted_index.tsv> <output_dir> <N> "<query string>"
 *
 * Inputs (all from Part 2(a)):
 *   inverted_index.tsv : TERM \t DOC_ID1:TF1,DOC_ID2:TF2,...   (MapReduce input)
 *   top100_df.tsv      : TERM \t DF                            (DistributedCache; DF source for IDF)
 *
 * Scoring (natural log):  IDF(t) = ln(N/DF(t) + 1),  S(D,Q) = sum_{t in Q∩D} TF(t,D) * IDF(t).
 * Query preprocessing is the SAME pipeline as 2(a) (TextPreprocessor.terms): lowercase,
 * split [^a-z0-9]+, drop stop words, Porter-stem. Repeated query terms collapse into the SET Q.
 * Ranking: score descending; ties broken by DOC_ID ascending, compared numerically
 * (IDs are integers; a non-numeric ID falls back to string order).
 */
public class QueryRetrieval {

    static final int TOP_K = 10;

    /** Numeric-aware DOC_ID comparison (the 2(a) postings are ordered numerically too). */
    static int compareDocIds(String a, String b) {
        try {
            return Long.compare(Long.parseLong(a), Long.parseLong(b));
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }

    // ---------- Mapper: index line -> (DOC_ID, "TERM:contribution") ----------
    public static class QMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Set<String> queryTerms = new HashSet<>();
        private final Map<String, Integer> dfByTerm = new HashMap<>();
        private double n;
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) throws IOException {
            Configuration conf = ctx.getConfiguration();
            n = Double.parseDouble(conf.get("total.docs"));
            for (String t : conf.get("query.terms", "").split(",")) {
                if (!t.isEmpty()) queryTerms.add(t);
            }
            URI dfUri = TextPreprocessor.findCacheFile(ctx.getCacheFiles(), "top100_df.tsv");
            for (String l : TextPreprocessor.readCachedLines(conf, dfUri)) {
                String[] f = l.split("\t");
                if (f.length >= 2) dfByTerm.put(f[0], Integer.parseInt(f[1].trim()));
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {
            String line = value.toString();
            int tab = line.indexOf('\t');
            if (tab < 0) return;
            String term = line.substring(0, tab).trim();
            if (!queryTerms.contains(term)) return;

            Integer df = dfByTerm.get(term);
            if (df == null || df <= 0) {
                ctx.getCounter("QueryRetrieval", "TERM_WITHOUT_DF").increment(1);
                return;
            }
            double idf = Math.log(n / df + 1.0);

            String[] postings = line.substring(tab + 1).trim().split(",");
            int count = 0;
            for (String p : postings) {
                if (p.isEmpty()) continue;
                count++;
                int c = p.lastIndexOf(':');
                String doc = p.substring(0, c);
                int tf = Integer.parseInt(p.substring(c + 1).trim());
                outKey.set(doc);
                outVal.set(term + ":" + (tf * idf));
                ctx.write(outKey, outVal);
            }
            // sanity check: index and DF file from Part 2(a) must agree
            if (count != df) ctx.getCounter("QueryRetrieval", "DF_MISMATCH").increment(1);
        }
    }

    // ---------- Reducer (single): sum per doc, keep global top-K in cleanup ----------
    public static class QReducer extends Reducer<Text, Text, NullWritable, Text> {
        private static class Hit {
            String doc;
            double score;
            TreeSet<String> terms = new TreeSet<>();
        }
        private final List<Hit> hits = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) {
            // TreeMap => contributions are summed in term order, so the score does not depend on
            // shuffle order (keeps exact-equal scores exactly equal and tie-breaking deterministic).
            TreeMap<String, Double> perTerm = new TreeMap<>();
            for (Text v : values) {
                String s = v.toString();
                int c = s.lastIndexOf(':');
                perTerm.merge(s.substring(0, c), Double.parseDouble(s.substring(c + 1)), Double::sum);
            }
            Hit h = new Hit();
            h.doc = key.toString();
            for (Map.Entry<String, Double> e : perTerm.entrySet()) {
                h.terms.add(e.getKey());
                h.score += e.getValue();
            }
            hits.add(h);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            hits.sort((a, b) -> {
                int cmp = Double.compare(b.score, a.score);
                return cmp != 0 ? cmp : compareDocIds(a.doc, b.doc);
            });
            for (int i = 0; i < Math.min(TOP_K, hits.size()); i++) {
                Hit h = hits.get(i);
                ctx.write(NullWritable.get(), new Text((i + 1) + "\t" + h.doc + "\t"
                        + String.join(",", h.terms) + "\t" + String.format("%.6f", h.score)));
            }
            ctx.getCounter("QueryRetrieval", "MATCHING_DOCS").increment(hits.size());
        }
    }

    // ---------- Driver ----------
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] a = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (a.length < 6) {
            System.err.println("Usage: QueryRetrieval <stopwords.txt> <top100_df.tsv> "
                    + "<inverted_index.tsv> <output_dir> <N> \"<query>\"");
            System.exit(2);
        }
        String stopFile = a[0], dfFile = a[1], indexFile = a[2], outDir = a[3];
        StringBuilder qs = new StringBuilder();
        for (int i = 5; i < a.length; i++) qs.append(a[i]).append(' ');

        // 1. Preprocess the query with the shared 2(a) pipeline; Q is a SET (repeats collapse).
        Set<String> stop = TextPreprocessor.loadStopwords(conf, new Path(stopFile).toUri());
        TreeSet<String> q = new TreeSet<>(new TextPreprocessor(stop).terms(qs.toString()));
        System.err.println("Query: \"" + qs.toString().trim() + "\"");
        System.err.println("Processed query terms Q (stemmed, stop words removed, deduplicated): " + q);

        // 2. Drop terms that are not in the indexed top-100 (nothing to look up for them).
        Set<String> indexed = new HashSet<>();
        for (String l : TextPreprocessor.readCachedLines(conf, new Path(dfFile).toUri())) {
            indexed.add(l.split("\t")[0]);
        }
        TreeSet<String> absent = new TreeSet<>(q);
        absent.removeAll(indexed);
        q.retainAll(indexed);
        if (!absent.isEmpty()) {
            System.err.println("Ignored (not in the top-100 index): " + absent);
        }
        if (q.isEmpty()) {
            System.err.println("No query term is a non-stop term present in the index -> "
                    + "no documents can match; nothing written, no job run.");
            System.exit(0);
        }
        System.err.println("Terms used for scoring: " + q);

        conf.set("total.docs", a[4]);
        conf.set("query.terms", String.join(",", q));

        Job job = Job.getInstance(conf, "query retrieval");
        job.setJarByClass(QueryRetrieval.class);
        job.setMapperClass(QMapper.class);
        job.setReducerClass(QReducer.class);
        job.setNumReduceTasks(1); // global top-10 needs a single reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.addCacheFile(new Path(dfFile).toUri());
        FileInputFormat.addInputPath(job, new Path(indexFile));
        FileOutputFormat.setOutputPath(job, new Path(outDir));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
