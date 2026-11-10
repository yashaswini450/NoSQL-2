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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * Problem 1(a): Frequent-word extraction.
 *
 * Identifies the top 50 most frequent words in the Wikipedia dataset,
 * excluding all stop words loaded via Hadoop's DistributedCache.
 *
 * Output schema (TSV):
 *   WORD<TAB>FREQUENCY
 *
 * Tie-breaking rule:
 *   Sorted by FREQUENCY descending, then lexicographically by WORD ascending.
 */
public class Top50Words {

	public static class StopwordFilteredMapper extends Mapper<Object, Text, Text, LongWritable> {

		private static final LongWritable ONE = new LongWritable(1L);
		private final Text word = new Text();
		private final Set<String> stopwords = new HashSet<String>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			URI[] cacheFiles = context.getCacheFiles();
			if (cacheFiles != null) {
				for (URI uri : cacheFiles) {
					Path path = new Path(uri.getPath());
					File localFile = new File(path.getName());
					if (localFile.exists()) {
						loadStopwords(localFile);
					} else {
						loadStopwords(new File(uri.getPath()));
					}
				}
			}
		}

		private void loadStopwords(File file) {
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				String line;
				while ((line = reader.readLine()) != null) {
					String sw = line.trim().toLowerCase();
					if (!sw.isEmpty()) {
						stopwords.add(sw);
					}
				}
			} catch (IOException ioe) {
				System.err.println("Error reading stopwords file: " + StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			for (String token : line.split("[^a-z0-9]+")) {
				if (token.isEmpty() || stopwords.contains(token)) {
					continue;
				}
				word.set(token);
				context.write(word, ONE);
			}
		}
	}

	public static class LongSumCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
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

	public static class Top50Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private int topK = 50;
		private final Map<String, Long> wordCounts = new HashMap<String, Long>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			topK = context.getConfiguration().getInt("top.k.words", 50);
		}

		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0L;
			for (LongWritable val : values) {
				sum += val.get();
			}
			wordCounts.put(key.toString(), sum);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			List<Map.Entry<String, Long>> entries = new ArrayList<Map.Entry<String, Long>>(wordCounts.entrySet());
			Collections.sort(entries, new Comparator<Map.Entry<String, Long>>() {
				@Override
				public int compare(Map.Entry<String, Long> a, Map.Entry<String, Long> b) {
					int cmp = Long.compare(b.getValue(), a.getValue()); // Descending by frequency
					if (cmp != 0) {
						return cmp;
					}
					return a.getKey().compareTo(b.getKey()); // Ascending lexicographical tie-breaker
				}
			});

			Text outWord = new Text();
			LongWritable outFreq = new LongWritable();
			int limit = Math.min(topK, entries.size());
			for (int i = 0; i < limit; i++) {
				Map.Entry<String, Long> entry = entries.get(i);
				outWord.set(entry.getKey());
				outFreq.set(entry.getValue());
				context.write(outWord, outFreq);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("Usage: Top50Words <stopwords.txt> <input_path> <output_path> [topK]");
			System.exit(2);
		}

		String stopwordsPath = args[0];
		String inputPath = args[1];
		String outputPath = args[2];
		int topK = (args.length >= 4) ? Integer.parseInt(args[3]) : 50;

		Configuration conf = new Configuration();
		conf.setInt("top.k.words", topK);

		Job job = Job.getInstance(conf, "Top 50 Frequent Non-Stop Words - Problem 1(a)");
		job.setJarByClass(Top50Words.class);

		// Add stop-word file to Hadoop DistributedCache
		job.addCacheFile(new Path(stopwordsPath).toUri());

		job.setMapperClass(StopwordFilteredMapper.class);
		job.setCombinerClass(LongSumCombiner.class);
		job.setReducerClass(Top50Reducer.class);

		// Single reducer to guarantee global Top-50 sorting
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

