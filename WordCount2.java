import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class WordCount2 {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		private boolean caseSensitive = false;
		private Set<String> patternsToSkip = new HashSet<String>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
			if (conf.getBoolean("wordcount.skip.patterns", false)) {
				URI[] patternsURIs = context.getCacheFiles();
				if (patternsURIs != null) {
					for (URI patternsURI : patternsURIs) {
						Path patternsPath = new Path(patternsURI.getPath());
						String patternsFileName = patternsPath.getName();
						parseSkipFile(patternsFileName);
					}
				}
			}
		}

		private void parseSkipFile(String fileName) {
			try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
				String pattern;
				while ((pattern = reader.readLine()) != null) {
					String trimmed = caseSensitive ? pattern.trim() : pattern.trim().toLowerCase();
					if (!trimmed.isEmpty()) {
						patternsToSkip.add(trimmed);
					}
				}
			} catch (IOException ioe) {
				System.err.println(
						"Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = caseSensitive ? value.toString() : value.toString().toLowerCase();
			String[] tokens = line.split("[^\\w']+");
			for (String rawToken : tokens) {
				if (rawToken.isEmpty() || patternsToSkip.contains(rawToken)) {
					continue;
				}
				String cleanToken = rawToken.replaceAll("[^a-zA-Z0-9]", "");
				if (cleanToken.isEmpty() || patternsToSkip.contains(cleanToken)) {
					continue;
				}
				word.set(cleanToken);
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "wordcount2");
		job.setJarByClass(WordCount2.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		List<String> otherArgs = new ArrayList<String>();
		for (int i = 0; i < args.length; ++i) {
			if ("-skippatterns".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				job.addCacheFile(new Path(args[++i]).toUri());
			} else if ("-casesensitive".equals(args[i])) {
				job.getConfiguration().setBoolean("wordcount.case.sensitive", true);
			} else if ("-combiner".equals(args[i])) {
				job.setCombinerClass(IntSumReducer.class);
			} else {
				otherArgs.add(args[i]);
			}
		}

		if (otherArgs.size() < 2) {
			System.err.println("Usage: WordCount2 [-skippatterns <stopwords.txt>] [-casesensitive] [-combiner] <in> <out>");
			System.exit(2);
		}

		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
