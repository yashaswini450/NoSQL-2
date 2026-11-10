import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordAssociationStripes extends Configured implements Tool {
    private static final String LOCAL_AGG = "stripes.local.aggregation";
    private static final String STOPWORDS = "stripes-stopwords";
    private static final String VOCABULARY = "stripes-vocabulary";
    private static final Pattern SEPARATOR = Pattern.compile("[^a-z0-9]+");
    private static final int DISTANCE = 2;

    public static class StripesMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        private final Map<String, Map<String, Long>> splitStripes = new HashMap<>();
        private Set<String> stopwords;
        private Set<String> vocabulary;
        private boolean localAgg;

        @Override
        protected void setup(Context context) throws IOException {
            stopwords = readCachedTerms(STOPWORDS, false);
            vocabulary = readCachedTerms(VOCABULARY, true);
            localAgg = context.getConfiguration().getBoolean(LOCAL_AGG, false);
        }

        @Override
        protected void map(LongWritable offset, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = new ArrayList<>();
            for (String token : SEPARATOR.split(value.toString().toLowerCase(Locale.ROOT))) {
                if (!token.isEmpty() && !stopwords.contains(token)) {
                    tokens.add(token);
                }
            }

            for (int i = 0; i < tokens.size(); i++) {
                String word = tokens.get(i);
                if (!vocabulary.contains(word)) {
                    continue;
                }
                Map<String, Long> stripe;
                if (localAgg) {
                    stripe = splitStripes.computeIfAbsent(word, ignored -> new HashMap<>());
                } else {
                    stripe = new HashMap<>();
                }

                int first = Math.max(0, i - DISTANCE);
                int last = Math.min(tokens.size() - 1, i + DISTANCE);
                for (int j = first; j <= last; j++) {
                    String neighbor = tokens.get(j);
                    // Match Pairs: repeated occurrences of the same word are excluded.
                    if (!word.equals(neighbor) && vocabulary.contains(neighbor)) {
                        stripe.merge(neighbor, 1L, Long::sum);
                    }
                }
                if (!localAgg) {
                    writeStripe(context, word, stripe);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Map<String, Long>> entry : splitStripes.entrySet()) {
                writeStripe(context, entry.getKey(), entry.getValue());
            }
        }

        private void writeStripe(Context context, String word, Map<String, Long> stripe)
                throws IOException, InterruptedException {
            if (stripe.isEmpty()) {
                return;
            }
            MapWritable output = new MapWritable();
            for (Map.Entry<String, Long> neighbor : stripe.entrySet()) {
                output.put(new Text(neighbor.getKey()), new LongWritable(neighbor.getValue()));
            }
            context.write(new Text(word), output);
        }

        private static Set<String> readCachedTerms(String alias, boolean tsv) throws IOException {
            Set<String> terms = new HashSet<>();
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(alias), StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String term = tsv ? line.split("\t", 2)[0] : line;
                    term = term.trim().toLowerCase(Locale.ROOT);
                    if (!term.isEmpty()) {
                        terms.add(term);
                    }
                }
            }
            if (terms.isEmpty() || (tsv && terms.size() > 50)) {
                throw new IOException("Unexpected number of terms in " + alias + ": " + terms.size());
            }
            return terms;
        }
    }

    public static class StripesReducer extends Reducer<Text, MapWritable, Text, Text> {
        private final Text outputValue = new Text();

        @Override
        protected void reduce(Text word, Iterable<MapWritable> stripes, Context context)
                throws IOException, InterruptedException {
            Map<String, Long> counts = new HashMap<>();
            long total = 0;
            for (MapWritable stripe : stripes) {
                for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                    long count = ((LongWritable) entry.getValue()).get();
                    counts.merge(entry.getKey().toString(), count, Long::sum);
                    total += count;
                }
            }

            List<Map.Entry<String, Long>> ranked = new ArrayList<>(counts.entrySet());
            ranked.sort(Comparator.comparingLong((Map.Entry<String, Long> neighbor) -> neighbor.getValue())
                    .reversed().thenComparing(Map.Entry::getKey));
            for (int i = 0; i < Math.min(5, ranked.size()); i++) {
                Map.Entry<String, Long> neighbor = ranked.get(i);
                outputValue.set(String.format(Locale.ROOT, "%s\t%d\t%.6f",
                        neighbor.getKey(), neighbor.getValue(), (double) neighbor.getValue() / total));
                context.write(word, outputValue);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 5 || !(args[4].equals("true") || args[4].equals("false"))) {
            System.err.println("Usage: WordAssociationStripes <stopwords> <top50.tsv> <input> <output> <true|false>");
            return 2;
        }
        Job job = Job.getInstance(getConf(), "Word Association Stripes (local aggregation " + args[4] + ")");
        job.getConfiguration().setBoolean(LOCAL_AGG, Boolean.parseBoolean(args[4]));
        job.setJarByClass(WordAssociationStripes.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(StripesMapper.class);
        job.setReducerClass(StripesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        addCacheFile(job, args[0], STOPWORDS);
        addCacheFile(job, args[1], VOCABULARY);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static void addCacheFile(Job job, String filename, String alias) throws Exception {
        Path path = new Path(filename);
        URI uri = path.getFileSystem(job.getConfiguration()).makeQualified(path).toUri();
        job.addCacheFile(new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), alias));
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordAssociationStripes(), args));
    }
}
