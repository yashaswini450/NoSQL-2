import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * Problem 1(b) & Problem 1(d)(i-ii): Word association using the Pairs approach.
 *
 * Conventions:
 * - Tokenization: lowercase, apostrophe-aware stopword filtering, non-alphanumeric stripped.
 * - Window definition: Over the sequence of filtered non-stop tokens in each record,
 *   for token u at index i (where u is in Top-50 vocabulary), we consider all neighbouring
 *   tokens v at index j within window distance d (1 <= |j - i| <= d).
 * - Pair convention: Ordered pairs (u, v) with u in Top-50, v in Top-50, and u != v.
 * - Denominator convention: Sum_{w in Top-50, w != u} C(u, w), i.e. total co-occurrence
 *   count of focal word u with all other Top-50 vocabulary words within window d.
 * - Conditional association score: P(v | u) = C(u, v) / Sum_w C(u, w).
 * - Local aggregation modes:
 *     "false" / "none"     -> No local aggregation (emits (u\tv, 1) per occurrence)
 *     "function"           -> Map-function level aggregation (aggregates per map() call)
 *     "true" / "class"     -> Map-class level in-mapper combining + Hadoop Combiner
 *
 * Output schema (TSV):
 *   U<TAB>V<TAB>COUNT<TAB>SCORE
 */
public class WordAssociationPairs {

	public static class PairsMapper extends Mapper<Object, Text, Text, LongWritable> {

		private static final LongWritable ONE = new LongWritable(1L);
		private final Text pairKey = new Text();
		private final LongWritable countWritable = new LongWritable();

		private final Set<String> stopwords = new HashSet<String>();
		private final Set<String> vocab = new HashSet<String>();
		private int windowSize = 2;
		private String aggMode = "none";

		// Map-class level state for in-mapper combining
		private Map<String, Long> classLevelCounts;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			windowSize = conf.getInt("cooccurrence.window.distance", 2);
			aggMode = conf.get("cooccurrence.local.aggregation", "none").toLowerCase(Locale.ROOT);
			classLevelCounts = new HashMap<String, Long>();

			String stopwordsFileName = conf.get("stopwords.file.name", "stopwords.txt");
			String vocabFileName = conf.get("vocab.file.name", "top50_words.tsv");

			URI[] cacheFiles = context.getCacheFiles();
			if (cacheFiles != null) {
				for (URI uri : cacheFiles) {
					Path path = new Path(uri.getPath());
					String name = path.getName();
					File file = new File(name);
					if (!file.exists()) {
						file = new File(uri.getPath());
					}
					if (name.equals(stopwordsFileName) || name.contains("stopword")) {
						loadStopwords(file);
					} else if (name.equals(vocabFileName) || name.contains("top50")) {
						loadVocab(file);
					}
				}
			}
		}

		private void loadStopwords(File file) {
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				String line;
				while ((line = reader.readLine()) != null) {
					String sw = line.trim().toLowerCase(Locale.ROOT);
					if (!sw.isEmpty()) {
						stopwords.add(sw);
					}
				}
			} catch (IOException ioe) {
				System.err.println("Error loading stopwords: " + StringUtils.stringifyException(ioe));
			}
		}

		private void loadVocab(File file) {
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				String line;
				while ((line = reader.readLine()) != null) {
					String trimmed = line.trim();
					if (!trimmed.isEmpty()) {
						String[] parts = trimmed.split("\\s+");
						if (parts.length > 0 && !parts[0].isEmpty()) {
							vocab.add(parts[0].toLowerCase(Locale.ROOT));
						}
					}
				}
			} catch (IOException ioe) {
				System.err.println("Error loading Top-50 vocabulary: " + StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase(Locale.ROOT);
			String[] rawTokens = line.split("[^a-z0-9]+");

			List<String> tokens = new ArrayList<String>(rawTokens.length);
			for (String token : rawTokens) {
				if (token.isEmpty() || stopwords.contains(token)) {
					continue;
				}
				tokens.add(token);
			}

			int n = tokens.size();
			if ("function".equals(aggMode)) {
				// Map-function level local aggregation (per document/record)
				Map<String, Long> funcCounts = new HashMap<String, Long>();
				for (int i = 0; i < n; i++) {
					String u = tokens.get(i);
					if (!vocab.contains(u)) {
						continue;
					}
					int start = Math.max(0, i - windowSize);
					int end = Math.min(n - 1, i + windowSize);
					for (int j = start; j <= end; j++) {
						if (i == j) {
							continue;
						}
						String v = tokens.get(j);
						if (!u.equals(v) && vocab.contains(v)) {
							String k = u + "\t" + v;
							Long prev = funcCounts.get(k);
							funcCounts.put(k, (prev == null) ? 1L : prev + 1L);
						}
					}
				}
				for (Map.Entry<String, Long> entry : funcCounts.entrySet()) {
					pairKey.set(entry.getKey());
					countWritable.set(entry.getValue());
					context.write(pairKey, countWritable);
				}
			} else if ("true".equals(aggMode) || "class".equals(aggMode)) {
				// Map-class level local aggregation (in-mapper combining across entire split)
				for (int i = 0; i < n; i++) {
					String u = tokens.get(i);
					if (!vocab.contains(u)) {
						continue;
					}
					int start = Math.max(0, i - windowSize);
					int end = Math.min(n - 1, i + windowSize);
					for (int j = start; j <= end; j++) {
						if (i == j) {
							continue;
						}
						String v = tokens.get(j);
						if (!u.equals(v) && vocab.contains(v)) {
							String k = u + "\t" + v;
							Long prev = classLevelCounts.get(k);
							classLevelCounts.put(k, (prev == null) ? 1L : prev + 1L);
						}
					}
				}
			} else {
				// No local aggregation: emit (u\tv, 1) immediately on every co-occurrence
				for (int i = 0; i < n; i++) {
					String u = tokens.get(i);
					if (!vocab.contains(u)) {
						continue;
					}
					int start = Math.max(0, i - windowSize);
					int end = Math.min(n - 1, i + windowSize);
					for (int j = start; j <= end; j++) {
						if (i == j) {
							continue;
						}
						String v = tokens.get(j);
						if (!u.equals(v) && vocab.contains(v)) {
							pairKey.set(u + "\t" + v);
							context.write(pairKey, ONE);
						}
					}
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (("true".equals(aggMode) || "class".equals(aggMode)) && classLevelCounts != null) {
				for (Map.Entry<String, Long> entry : classLevelCounts.entrySet()) {
					pairKey.set(entry.getKey());
					countWritable.set(entry.getValue());
					context.write(pairKey, countWritable);
				}
				// Clear state after flushing to prevent any double-counting
				classLevelCounts.clear();
			}
		}
	}

	public static class PairsCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
		private final LongWritable sumWritable = new LongWritable();

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
			for (LongWritable val : values) {
				sum += val.get();
			}
			sumWritable.set(sum);
			context.write(key, sumWritable);
		}
	}

	/**
	 * Routes all pairs (u, v) with the same focal word u to the same Reducer.
	 */
	public static class FirstWordPartitioner extends Partitioner<Text, LongWritable> {
		@Override
		public int getPartition(Text key, LongWritable value, int numPartitions) {
			String k = key.toString();
			int tabIdx = k.indexOf('\t');
			String u = (tabIdx >= 0) ? k.substring(0, tabIdx) : k;
			return (u.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class PairsTop5Reducer extends Reducer<Text, LongWritable, Text, Text> {

		private int topAssociations = 5;
		private final Map<String, Map<String, Long>> matrix = new TreeMap<String, Map<String, Long>>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			topAssociations = context.getConfiguration().getInt("cooccurrence.top.k", 5);
		}

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			String k = key.toString();
			int tabIdx = k.indexOf('\t');
			if (tabIdx < 0) {
				return;
			}
			String u = k.substring(0, tabIdx);
			String v = k.substring(tabIdx + 1);

			long sum = 0L;
			for (LongWritable val : values) {
				sum += val.get();
			}

			Map<String, Long> neighbors = matrix.get(u);
			if (neighbors == null) {
				neighbors = new HashMap<String, Long>();
				matrix.put(u, neighbors);
			}
			Long prev = neighbors.get(v);
			neighbors.put(v, (prev == null) ? sum : prev + sum);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Text outKey = new Text();
			Text outVal = new Text();

			for (Map.Entry<String, Map<String, Long>> uEntry : matrix.entrySet()) {
				String u = uEntry.getKey();
				Map<String, Long> neighbors = uEntry.getValue();

				long marginalSum = 0L;
				for (Long count : neighbors.values()) {
					marginalSum += count;
				}
				if (marginalSum == 0L) {
					continue;
				}

				List<Map.Entry<String, Long>> sortedNeighbors = new ArrayList<Map.Entry<String, Long>>(
						neighbors.entrySet());
				Collections.sort(sortedNeighbors, new Comparator<Map.Entry<String, Long>>() {
					@Override
					public int compare(Map.Entry<String, Long> a, Map.Entry<String, Long> b) {
						int cmp = Long.compare(b.getValue(), a.getValue()); // Descending by C(u, v)
						if (cmp != 0) {
							return cmp;
						}
						return a.getKey().compareTo(b.getKey()); // Ascending lexicographical tie-breaker
					}
				});

				int limit = Math.min(topAssociations, sortedNeighbors.size());
				for (int i = 0; i < limit; i++) {
					Map.Entry<String, Long> vEntry = sortedNeighbors.get(i);
					String v = vEntry.getKey();
					long count = vEntry.getValue();
					double conditionalProb = (double) count / (double) marginalSum;

					outKey.set(u + "\t" + v);
					outVal.set(count + "\t" + String.format(Locale.US, "%.6f", conditionalProb));
					context.write(outKey, outVal);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println(
					"Usage: WordAssociationPairs <stopwords.txt> <top50_words.tsv> <input_path> <output_path> [localAgg:false|function|true] [windowDistance:2]");
			System.exit(2);
		}

		String stopwordsPath = args[0];
		String vocabPath = args[1];
		String inputPath = args[2];
		String outputPath = args[3];
		String localAgg = (args.length >= 5) ? args[4].toLowerCase(Locale.ROOT) : "false";
		int windowDistance = (args.length >= 6) ? Integer.parseInt(args[5]) : 2;

		Configuration conf = new Configuration();
		conf.set("stopwords.file.name", new Path(stopwordsPath).getName());
		conf.set("vocab.file.name", new Path(vocabPath).getName());
		conf.set("cooccurrence.local.aggregation", localAgg);
		conf.setInt("cooccurrence.window.distance", windowDistance);
		conf.setInt("cooccurrence.top.k", 5);

		String jobTitle = "Word Association Pairs (d=" + windowDistance + ", localAgg=" + localAgg + ")";
		Job job = Job.getInstance(conf, jobTitle);
		job.setJarByClass(WordAssociationPairs.class);

		job.addCacheFile(new Path(stopwordsPath).toUri());
		job.addCacheFile(new Path(vocabPath).toUri());

		job.setMapperClass(PairsMapper.class);
		if ("true".equals(localAgg) || "class".equals(localAgg) || "function".equals(localAgg)) {
			job.setCombinerClass(PairsCombiner.class);
		}
		job.setPartitionerClass(FirstWordPartitioner.class);
		job.setReducerClass(PairsTop5Reducer.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

