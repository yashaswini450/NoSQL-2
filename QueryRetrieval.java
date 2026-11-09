import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import opennlp.tools.stemmer.PorterStemmer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 *   hadoop jar problem2_query.jar QueryRetrieval [-libjars opennlp-tools-1.9.3.jar] \
 *       <stopwords.txt> <inverted_index_dir> <output_dir> <N> "<query string>"
 *
 * Index line format (from Part 2(a)):  TERM \t DOC_ID1:TF1,DOC_ID2:TF2,...
 *
 * DF(t) is the number of postings on the term's line (index lists every doc containing t),
 * so no separate DF file is needed. IDF(t) = ln(N/DF(t) + 1); S(D,Q) = sum TF*IDF over t in Q∩D.
 * Query is lowercased, split on [^a-z0-9]+, stop-word filtered, Porter-stemmed, de-duplicated.
 */
public class QueryRetrieval {

    static final int TOP_K = 10;

    // ---------- Mapper: index line -> (DOC_ID, "TERM:contribution") ----------
    public static class QMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Set<String> queryTerms = new HashSet<>();
        private double n;
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context ctx) {
            Configuration conf = ctx.getConfiguration();
            n = Double.parseDouble(conf.get("total.docs"));
            for (String t : conf.get("query.terms", "").split(",")) {
                if (!t.isEmpty()) queryTerms.add(t);
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

            String[] postings = line.substring(tab + 1).trim().split(",");
            int df = 0;
            for (String p : postings) if (!p.isEmpty()) df++;
            if (df == 0) return;
            double idf = Math.log(n / df + 1.0);

            for (String p : postings) {
                if (p.isEmpty()) continue;
                int c = p.lastIndexOf(':');
                String doc = p.substring(0, c);
                int tf = Integer.parseInt(p.substring(c + 1).trim());
                outKey.set(doc);
                outVal.set(term + ":" + (tf * idf));
                ctx.write(outKey, outVal);
            }
        }
    }

    // ---------- Reducer (single): sum per doc, keep global top-K in cleanup ----------
    public static class QReducer extends Reducer<Text, Text, NullWritable, Text> {
        private static class Hit {
            String doc; double score; TreeSet<String> terms;
        }
        private final List<Hit> hits = new ArrayList<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) {
            Hit h = new Hit();
            h.doc = key.toString();
            h.terms = new TreeSet<>();
            for (Text v : values) {
                String s = v.toString();
                int c = s.lastIndexOf(':');
                h.terms.add(s.substring(0, c));
                h.score += Double.parseDouble(s.substring(c + 1));
            }
            hits.add(h);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            hits.sort((a, b) -> {
                int cmp = Double.compare(b.score, a.score);
                return cmp != 0 ? cmp : a.doc.compareTo(b.doc);
            });
            for (int i = 0; i < Math.min(TOP_K, hits.size()); i++) {
                Hit h = hits.get(i);
                ctx.write(NullWritable.get(), new Text((i + 1) + "\t" + h.doc + "\t"
                        + String.join(",", h.terms) + "\t" + String.format("%.6f", h.score)));
            }
        }
    }

    // ---------- Driver ----------
    static Set<String> loadStopwords(Configuration conf, Path p) throws IOException {
        Set<String> sw = new HashSet<>();
        FileSystem fs = p.getFileSystem(conf);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)))) {
            String l;
            while ((l = br.readLine()) != null) {
                l = l.trim().toLowerCase();
                if (!l.isEmpty()) sw.add(l);
            }
        }
        return sw;
    }

    /** Same pipeline as Part 2(a): lowercase, split non-alnum, drop stopwords, Porter stem, dedupe. */
    static TreeSet<String> preprocessQuery(String q, Set<String> stop) {
        PorterStemmer st = new PorterStemmer();
        TreeSet<String> out = new TreeSet<>();
        for (String tok : q.toLowerCase().split("[^a-z0-9]+")) {
            if (tok.isEmpty() || stop.contains(tok)) continue;
            out.add(st.stem(tok));
        }
        return out;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] a = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (a.length < 5) {
            System.err.println("Usage: QueryRetrieval <stopwords.txt> <inverted_index_dir> <output_dir> <N> \"<query>\"");
            System.exit(2);
        }
        Set<String> stop = loadStopwords(conf, new Path(a[0]));
        StringBuilder qs = new StringBuilder();
        for (int i = 4; i < a.length; i++) qs.append(a[i]).append(' ');
        TreeSet<String> terms = preprocessQuery(qs.toString(), stop);
        System.err.println("Processed query terms: " + terms);
        if (terms.isEmpty()) {
            System.err.println("No usable query terms after stop-word removal; nothing to do.");
            System.exit(0);
        }
        conf.set("total.docs", a[3]);
        conf.set("query.terms", String.join(",", terms));

        Job job = Job.getInstance(conf, "query retrieval");
        job.setJarByClass(QueryRetrieval.class);
        job.setMapperClass(QMapper.class);
        job.setReducerClass(QReducer.class);
        job.setNumReduceTasks(1); // global top-10 needs a single reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(a[1]));
        FileOutputFormat.setOutputPath(job, new Path(a[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
