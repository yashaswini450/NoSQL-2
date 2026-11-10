import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Problem 2(a), part 2: inverted index for the top-100 DF terms.
 *
 * Map:    (docId, text) -> TF of each top-100 term in the document -> (term, "docId:tf")
 * Reduce: term -> postings sorted by DOC_ID (numeric ascending)
 *
 * Output schema (one line per term, single file):
 *   TERM<TAB>DOC_ID:TF,DOC_ID:TF,...
 * The number of postings of a term equals its DF; the reducer verifies this against top100_df.tsv.
 *
 * Usage: InvertedIndex [generic options] <stopwords.txt> <top100_df.tsv> <input_dir> <output_dir>
 */
public class InvertedIndex extends Configured implements Tool {

	public static final String TOP_TERMS_NAME = "p2.topterms.name";

	public enum Counters {
		DOCS, POSTINGS, DF_MISMATCH, DUPLICATE_DOC_IN_POSTINGS
	}

	/** DOC_IDs are integers: order by length, then lexicographically (= numeric order without leading zeros). */
	static final Comparator<String> DOC_ID_ORDER = new Comparator<String>() {
		@Override
		public int compare(String a, String b) {
			if (a.length() != b.length()) {
				return Integer.compare(a.length(), b.length());
			}
			return a.compareTo(b);
		}
	};

	static Map<String, Integer> loadTopTerms(Configuration conf, URI[] cacheFiles) throws IOException {
		URI uri = TextPreprocessor.findCacheFile(cacheFiles, conf.get(TOP_TERMS_NAME));
		Map<String, Integer> dfByTerm = new HashMap<String, Integer>();
		for (String line : TextPreprocessor.readCachedLines(conf, uri)) {
			String[] parts = line.split("\t");
			dfByTerm.put(parts[0], Integer.parseInt(parts[1].trim()));
		}
		return dfByTerm;
	}

	public static class IndexMapper extends Mapper<Text, Text, Text, Text> {

		private TextPreprocessor preprocessor;
		private Map<String, Integer> topTerms;
		private final Text term = new Text();
		private final Text posting = new Text();

		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			URI stopwordsUri = TextPreprocessor.findCacheFile(context.getCacheFiles(),
					conf.get(DocumentFrequency.STOPWORDS_NAME));
			preprocessor = new TextPreprocessor(TextPreprocessor.loadStopwords(conf, stopwordsUri));
			topTerms = loadTopTerms(conf, context.getCacheFiles());
		}

		@Override
		public void map(Text docId, Text text, Context context) throws IOException, InterruptedException {
			context.getCounter(Counters.DOCS).increment(1);
			// One record = one whole document, so these counts are the exact TF(t, D).
			Map<String, Integer> tf = new HashMap<String, Integer>();
			for (String t : preprocessor.terms(text.toString())) {
				if (topTerms.containsKey(t)) {
					Integer count = tf.get(t);
					tf.put(t, count == null ? 1 : count + 1);
				}
			}
			for (Map.Entry<String, Integer> entry : tf.entrySet()) {
				term.set(entry.getKey());
				posting.set(docId.toString() + ":" + entry.getValue());
				context.write(term, posting);
			}
		}
	}

	public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

		private Map<String, Integer> topTerms;

		@Override
		protected void setup(Context context) throws IOException {
			topTerms = loadTopTerms(context.getConfiguration(), context.getCacheFiles());
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, String> tfByDoc = new HashMap<String, String>();
			for (Text value : values) {
				String posting = value.toString();
				int colon = posting.lastIndexOf(':');
				String doc = posting.substring(0, colon);
				if (tfByDoc.put(doc, posting.substring(colon + 1)) != null) {
					context.getCounter(Counters.DUPLICATE_DOC_IN_POSTINGS).increment(1);
				}
			}
			List<String> docs = new ArrayList<String>(tfByDoc.keySet());
			Collections.sort(docs, DOC_ID_ORDER);

			StringBuilder postings = new StringBuilder();
			for (String doc : docs) {
				if (postings.length() > 0) {
					postings.append(',');
				}
				postings.append(doc).append(':').append(tfByDoc.get(doc));
			}

			Integer expectedDf = topTerms.get(key.toString());
			if (expectedDf == null || expectedDf != docs.size()) {
				context.getCounter(Counters.DF_MISMATCH).increment(1);
				System.err.println("DF mismatch for " + key + ": expected " + expectedDf + ", postings " + docs.size());
			}
			context.getCounter(Counters.POSTINGS).increment(docs.size());
			context.write(key, new Text(postings.toString()));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println(
					"Usage: InvertedIndex [generic options] <stopwords.txt> <top100_df.tsv> <input_dir> <output_dir>");
			return 2;
		}
		Path stopwords = new Path(args[0]);
		Path topTerms = new Path(args[1]);

		Configuration conf = getConf();
		conf.set(DocumentFrequency.STOPWORDS_NAME, stopwords.getName());
		conf.set(TOP_TERMS_NAME, topTerms.getName());
		if (conf.get("mapreduce.input.fileinputformat.split.maxsize") == null) {
			conf.setLong("mapreduce.input.fileinputformat.split.maxsize", WholeDocumentInputFormat.DEFAULT_MAX_SPLIT_SIZE);
		}

		Job job = Job.getInstance(conf, "p2a-inverted-index");
		job.setJarByClass(InvertedIndex.class);
		job.addCacheFile(stopwords.getFileSystem(conf).makeQualified(stopwords).toUri());
		job.addCacheFile(topTerms.getFileSystem(conf).makeQualified(topTerms).toUri());
		job.setInputFormatClass(WholeDocumentInputFormat.class);
		job.setMapperClass(IndexMapper.class);
		job.setReducerClass(IndexReducer.class);
		job.setNumReduceTasks(1); // single index file, terms in sorted order
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new InvertedIndex(), args));
	}
}
