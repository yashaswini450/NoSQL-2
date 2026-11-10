import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import opennlp.tools.stemmer.PorterStemmer;

/**
 * Shared term pipeline for Problem 2 (DF, inverted index and query processing).
 *
 * Pipeline applied to every document:
 *   1. lowercase
 *   2. split on [^a-z0-9]+ (punctuation and non-ASCII characters act as separators)
 *   3. skip empty tokens
 *   4. skip stop words (checked on the raw token, before stemming)
 *   5. stem with the OpenNLP PorterStemmer
 *   6. skip empty stems
 */
public class TextPreprocessor {

	private final Set<String> stopwords;
	private final PorterStemmer stemmer = new PorterStemmer();

	public TextPreprocessor(Set<String> stopwords) {
		this.stopwords = stopwords;
	}

	/** Returns the stemmed, non-stop terms of the text in document order (repeats kept). */
	public List<String> terms(String text) {
		List<String> result = new ArrayList<String>();
		for (String token : text.toLowerCase().split("[^a-z0-9]+")) {
			if (token.isEmpty() || stopwords.contains(token)) {
				continue;
			}
			String stem = stemmer.stem(token);
			if (!stem.isEmpty()) {
				result.add(stem);
			}
		}
		return result;
	}

	/**
	 * Reads a cached file (e.g. stopwords.txt) through the Hadoop FileSystem API, so it works
	 * in both local and distributed mode. Lines are trimmed and lowercased; blank lines are skipped.
	 */
	public static List<String> readCachedLines(Configuration conf, URI uri) throws IOException {
		Path path = new Path(uri);
		FileSystem fs = path.getFileSystem(conf);
		List<String> lines = new ArrayList<String>();
		try (BufferedReader reader = new BufferedReader(
				new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {
			String line;
			while ((line = reader.readLine()) != null) {
				line = line.trim();
				if (!line.isEmpty()) {
					lines.add(line);
				}
			}
		}
		return lines;
	}

	public static Set<String> loadStopwords(Configuration conf, URI uri) throws IOException {
		Set<String> stopwords = new HashSet<String>();
		for (String line : readCachedLines(conf, uri)) {
			stopwords.add(line.toLowerCase());
		}
		return stopwords;
	}

	/** Finds the cached file whose name matches fileName. */
	public static URI findCacheFile(URI[] cacheFiles, String fileName) throws IOException {
		if (cacheFiles != null) {
			for (URI uri : cacheFiles) {
				if (new Path(uri.getPath()).getName().equals(fileName)) {
					return uri;
				}
			}
		}
		throw new IOException("Cache file not found: " + fileName);
	}
}
