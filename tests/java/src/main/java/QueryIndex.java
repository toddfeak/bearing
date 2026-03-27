// Query a Lucene index with a list of query strings and report per-query timing.
//
// Each line in the queries file is a query in Lucene standard syntax:
//   - bare word:       algorithms     (TermQuery)
//   - boolean MUST:    +algorithms +data  (BooleanQuery with MUST clauses)
//
// Usage (via gradle task):
//   ./tests/java/gradlew -p tests/java -q queryIndex \
//       -PindexDir=/tmp/index -PqueriesFile=/tmp/queries.txt [-PoutputFile=/tmp/results.txt]

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.FSDirectory;

public class QueryIndex {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: QueryIndex <index-dir> <queries-file> [output-file]");
            System.exit(1);
        }

        String indexDir = args[0];
        String queriesFile = args[1];
        String outputFile = args.length > 2 ? args[2] : null;

        List<String> queries = Files.readAllLines(Paths.get(queriesFile)).stream()
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();

        try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)))) {
            IndexSearcher searcher = new IndexSearcher(reader);
            QueryParser parser = new QueryParser("contents", new StandardAnalyzer(CharArraySet.EMPTY_SET));

            // Warm up: run all queries once to let JIT compile hot paths
            for (String queryStr : queries) {
                Query query = parser.parse(queryStr);
                searcher.search(query, new TopScoreDocCollectorManager(10, Integer.MAX_VALUE));
            }

            // Collect results in memory — no I/O during timed section
            String[] results = new String[queries.size()];
            long startNanos = System.nanoTime();

            for (int i = 0; i < queries.size(); i++) {
                String queryStr = queries.get(i);
                Query query = parser.parse(queryStr);
                TopDocs topDocs = searcher.search(query, new TopScoreDocCollectorManager(10, Integer.MAX_VALUE));

                StringBuilder sb = new StringBuilder();
                sb.append(String.format("%-30s hits=%-6d", queryStr, topDocs.totalHits.value()));
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    sb.append(String.format("  doc=%-5d score=%.4f", sd.doc, sd.score));
                }
                results[i] = sb.toString();
            }

            long elapsedNanos = System.nanoTime() - startNanos;
            double elapsedMs = elapsedNanos / 1_000_000.0;
            double avgUs = elapsedNanos / 1_000.0 / queries.size();

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
            System.out.printf("Queried %d queries in %.2f ms%n", queries.size(), elapsedMs);
            System.out.printf("Average: %.2f µs/query%n", avgUs);
        }
    }
}
