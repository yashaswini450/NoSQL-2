import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * Input format that delivers one record per article: key = DOC_ID (file name without ".txt"),
 * value = full article text.
 *
 * Files are never split, so a document is always seen whole by exactly one map() call. This makes
 * per-document de-duplication (DF) and per-document counting (TF) exact. Many small files are packed
 * into one CombineFileSplit (max size set by mapreduce.input.fileinputformat.split.maxsize), which
 * avoids launching one map task per article.
 */
public class WholeDocumentInputFormat extends CombineFileInputFormat<Text, Text> {

	public static final long DEFAULT_MAX_SPLIT_SIZE = 64L * 1024 * 1024;

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		return new CombineFileRecordReader<Text, Text>((CombineFileSplit) split, context, DocumentReader.class);
	}

	/** Reads the idx-th file of a CombineFileSplit as a single (docId, text) record. */
	public static class DocumentReader extends RecordReader<Text, Text> {

		private final Path path;
		private final long length;
		private final Configuration conf;
		private boolean processed = false;
		private final Text key = new Text();
		private final Text value = new Text();

		public DocumentReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) {
			this.path = split.getPath(idx);
			this.length = split.getLength(idx);
			this.conf = context.getConfiguration();
		}

		public static String docId(Path path) {
			String name = path.getName();
			return name.endsWith(".txt") ? name.substring(0, name.length() - 4) : name;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) {
			// all state is set in the constructor
		}

		@Override
		public boolean nextKeyValue() throws IOException {
			if (processed) {
				return false;
			}
			byte[] contents = new byte[(int) length];
			FileSystem fs = path.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(path);
				IOUtils.readFully(in, contents, 0, contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			key.set(docId(path));
			value.set(new String(contents, StandardCharsets.UTF_8));
			processed = true;
			return true;
		}

		@Override
		public Text getCurrentKey() {
			return key;
		}

		@Override
		public Text getCurrentValue() {
			return value;
		}

		@Override
		public float getProgress() {
			return processed ? 1.0f : 0.0f;
		}

		@Override
		public void close() {
			// stream is closed in nextKeyValue()
		}
	}
}
