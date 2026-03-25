// Query a Lucene index with a list of words and report per-query timing.
//
// Usage (via gradle task):
//   ./tests/java/gradlew -p tests/java -q queryIndex \
//       -PindexDir=/tmp/index -PwordsFile=/tmp/words.txt [-PoutputFile=/tmp/results.txt]

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class QueryIndex {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: QueryIndex <index-dir> <words-file> [output-file]");
            System.exit(1);
        }

        String indexDir = args[0];
        String wordsFile = args[1];
        String outputFile = args.length > 2 ? args[2] : null;

        List<String> words = Files.readAllLines(Paths.get(wordsFile)).stream()
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();

        try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)))) {
            IndexSearcher searcher = new IndexSearcher(reader);

            // Warm up: run all queries once to let JIT compile hot paths
            for (String word : words) {
                TermQuery query = new TermQuery(new Term("contents", word));
                searcher.search(query, 10);
            }

            // Collect results in memory — no I/O during timed section
            String[] results = new String[words.size()];
            long startNanos = System.nanoTime();

            for (int i = 0; i < words.size(); i++) {
                String word = words.get(i);
                TermQuery query = new TermQuery(new Term("contents", word));
                TopDocs topDocs = searcher.search(query, 10);

                StringBuilder sb = new StringBuilder();
                sb.append(String.format("%-20s hits=%-6d", word, topDocs.totalHits.value()));
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    sb.append(String.format("  doc=%-5d score=%.4f", sd.doc, sd.score));
                }
                results[i] = sb.toString();
            }

            long elapsedNanos = System.nanoTime() - startNanos;
            double elapsedMs = elapsedNanos / 1_000_000.0;
            double avgUs = elapsedNanos / 1_000.0 / words.size();

            // Write results to file or stdout
            if (outputFile != null) {
                try (PrintWriter pw = new PrintWriter(outputFile)) {
                    for (String line : results) {
                        pw.println(line);
                    }
                }
            } else {
                for (String line : results) {
                    System.out.println(line);
                }
            }

            // Timing always goes to stdout
            System.out.printf("Queried %d words in %.2f ms%n", words.size(), elapsedMs);
            System.out.printf("Average: %.2f µs/query%n", avgUs);
        }
    }
}
