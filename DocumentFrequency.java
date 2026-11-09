import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Problem 2(a), part 1: document frequency (DF) of every stemmed non-stop term, and the top-K
 * (default 100) terms by DF.
 *
 * Job 1 (DF):     (docId, text) -> distinct terms of the document -> (term, 1) -> sum = DF
 * Job 2 (Top-K):  (term, DF) lines -> local top-K per mapper -> single reducer -> global top-K
 *
 * Ordering / ties: DF descending, then TERM ascending (lexicographic).
 *
 * Usage: DocumentFrequency [generic options] <stopwords.txt> <input_dir> <output_base>
 * Output: <output_base>/df/part-r-00000      TERM<TAB>DF  (all terms, single file)
 *         <output_base>/top100/part-r-00000  TERM<TAB>DF  (top-K terms, ranked)
 *         <output_base>/doc_count.txt        N (number of documents read)
 */
public class DocumentFrequency extends Configured implements Tool {

	public static final String STOPWORDS_NAME = "p2.stopwords.name";
	public static final String TOP_K = "p2.topk";

	public enum Counters {
		DOCS, TERM_OCCURRENCES, DISTINCT_TERM_DOC_PAIRS
	}

	/** Ranks (term, df) entries: DF descending, then TERM ascending. */
	public static final Comparator<TermDf> RANK_ORDER = new Comparator<TermDf>() {
		@Override
		public int compare(TermDf a, TermDf b) {
			if (a.df != b.df) {
				return Integer.compare(b.df, a.df);
			}
			return a.term.compareTo(b.term);
		}
	};

	public static class TermDf {
		final String term;
		final int df;

		TermDf(String term, int df) {
			this.term = term;
			this.df = df;
		}

		static TermDf parse(String line) {
			int tab = line.indexOf('\t');
			return new TermDf(line.substring(0, tab), Integer.parseInt(line.substring(tab + 1).trim()));
		}

		@Override
		public String toString() {
			return term + "\t" + df;
		}
	}

	/** Adds an entry to a bounded ranked set, dropping the lowest-ranked entry beyond k. */
	static void offer(TreeSet<TermDf> top, TermDf entry, int k) {
		top.add(entry);
		if (top.size() > k) {
			top.pollLast();
		}
	}

	// ---------------------------------------------------------------- Job 1: DF

	public static class DfMapper extends Mapper<Text, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final Text term = new Text();
		private TextPreprocessor preprocessor;

		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			URI stopwordsUri = TextPreprocessor.findCacheFile(context.getCacheFiles(), conf.get(STOPWORDS_NAME));
			preprocessor = new TextPreprocessor(TextPreprocessor.loadStopwords(conf, stopwordsUri));
		}

		@Override
		public void map(Text docId, Text text, Context context) throws IOException, InterruptedException {
			context.getCounter(Counters.DOCS).increment(1);
			// One record = one whole document, so this set de-duplicates terms within the document.
			List<String> terms = preprocessor.terms(text.toString());
			Set<String> distinct = new HashSet<String>(terms);
			context.getCounter(Counters.TERM_OCCURRENCES).increment(terms.size());
			for (String t : distinct) {
				term.set(t);
				context.write(term, one);
			}
			context.getCounter(Counters.DISTINCT_TERM_DOC_PAIRS).increment(distinct.size());
		}
	}

	/** Used as combiner and reducer: every incoming 1 represents one distinct document. */
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private final IntWritable result = new IntWritable();

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

	// ---------------------------------------------------------------- Job 2: Top-K

	public static class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		private final TreeSet<TermDf> top = new TreeSet<TermDf>(RANK_ORDER);
		private int k;

		@Override
		protected void setup(Context context) {
			k = context.getConfiguration().getInt(TOP_K, 100);
		}

		@Override
		public void map(LongWritable offset, Text line, Context context) {
			if (line.getLength() > 0) {
				offer(top, TermDf.parse(line.toString()), k);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Emit this mapper's local top-K once, after all its input has been seen.
			for (TermDf entry : top) {
				context.write(NullWritable.get(), new Text(entry.toString()));
			}
		}
	}

	public static class TopKReducer extends Reducer<NullWritable, Text, Text, IntWritable> {

		@Override
		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int k = context.getConfiguration().getInt(TOP_K, 100);
			TreeSet<TermDf> top = new TreeSet<TermDf>(RANK_ORDER);
			for (Text value : values) {
				offer(top, TermDf.parse(value.toString()), k);
			}
			for (TermDf entry : top) {
				context.write(new Text(entry.term), new IntWritable(entry.df));
			}
		}
	}

	// ---------------------------------------------------------------- Driver

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: DocumentFrequency [generic options] <stopwords.txt> <input_dir> <output_base>");
			return 2;
		}
		Path stopwords = new Path(args[0]);
		Path input = new Path(args[1]);
		Path outputBase = new Path(args[2]);
		Path dfOut = new Path(outputBase, "df");
		Path topOut = new Path(outputBase, "top100");

		Configuration conf = getConf();
		conf.set(STOPWORDS_NAME, stopwords.getName());
		if (conf.get("mapreduce.input.fileinputformat.split.maxsize") == null) {
			conf.setLong("mapreduce.input.fileinputformat.split.maxsize", WholeDocumentInputFormat.DEFAULT_MAX_SPLIT_SIZE);
		}

		// Job 1: document frequency of every term
		Job dfJob = Job.getInstance(conf, "p2a-document-frequency");
		dfJob.setJarByClass(DocumentFrequency.class);
		dfJob.addCacheFile(stopwords.getFileSystem(conf).makeQualified(stopwords).toUri());
		dfJob.setInputFormatClass(WholeDocumentInputFormat.class);
		dfJob.setMapperClass(DfMapper.class);
		dfJob.setCombinerClass(IntSumReducer.class);
		dfJob.setReducerClass(IntSumReducer.class);
		dfJob.setNumReduceTasks(1); // single TSV file with all terms
		dfJob.setOutputKeyClass(Text.class);
		dfJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(dfJob, input);
		FileOutputFormat.setOutputPath(dfJob, dfOut);
		if (!dfJob.waitForCompletion(true)) {
			return 1;
		}
		long docs = dfJob.getCounters().findCounter(Counters.DOCS).getValue();

		// Job 2: top-K terms by DF
		Job topJob = Job.getInstance(conf, "p2a-top-df-terms");
		topJob.setJarByClass(DocumentFrequency.class);
		topJob.setInputFormatClass(TextInputFormat.class);
		topJob.setMapperClass(TopKMapper.class);
		topJob.setReducerClass(TopKReducer.class);
		topJob.setNumReduceTasks(1);
		topJob.setMapOutputKeyClass(NullWritable.class);
		topJob.setMapOutputValueClass(Text.class);
		topJob.setOutputKeyClass(Text.class);
		topJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(topJob, dfOut);
		FileOutputFormat.setOutputPath(topJob, topOut);
		if (!topJob.waitForCompletion(true)) {
			return 1;
		}

		// N for IDF in Problem 2(b)
		FileSystem fs = outputBase.getFileSystem(conf);
		try (Writer w = new OutputStreamWriter(fs.create(new Path(outputBase, "doc_count.txt")), StandardCharsets.UTF_8)) {
			w.write(docs + "\n");
		}
		System.out.println("Documents (N) = " + docs);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new DocumentFrequency(), args));
	}
}
