import java.io.*; import java.nio.file.*; import java.util.*; import java.util.stream.*;
import opennlp.tools.stemmer.PorterStemmer;
/** TEST-ONLY stand-in for Person 3's Part 2(a): builds top-100-DF inverted index locally. */
public class LocalIndexStandIn {
  public static void main(String[] a) throws Exception {
    Set<String> stop = new HashSet<>(); for (String l : Files.readAllLines(Paths.get(a[0]))) if(!l.trim().isEmpty()) stop.add(l.trim().toLowerCase());
    PorterStemmer st = new PorterStemmer();
    Map<String,Map<String,Integer>> idx = new HashMap<>();
    File[] fs = new File(a[1]).listFiles(); Arrays.sort(fs);
    for (File f : fs) for (String line : Files.readAllLines(f.toPath())) for (String t : line.toLowerCase().split("[^a-z0-9]+")) {
      if (t.isEmpty()||stop.contains(t)) continue;
      idx.computeIfAbsent(st.stem(t), k->new TreeMap<>()).merge(f.getName().replace(".txt",""),1,Integer::sum); }
    List<String> top = idx.keySet().stream().sorted((x,y)->{int c=idx.get(y).size()-idx.get(x).size(); return c!=0?c:x.compareTo(y);}).limit(100).collect(Collectors.toList());
    try (PrintWriter w = new PrintWriter(a[2])) { for (String t : top) w.println(t+"\t"+idx.get(t).entrySet().stream().map(e->e.getKey()+":"+e.getValue()).collect(Collectors.joining(","))); }
    System.out.println(fs.length+" docs");
  }
}
