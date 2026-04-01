// SPDX-License-Identifier: Apache-2.0

// DEBT: Limited copy of IndexAllFields.java for the newindex pipeline.
// Only indexes the field types that newindex currently supports (stored + text).
// As newindex gains field types, this indexer should grow to match IndexAllFields.

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

/**
 * Indexes documents with the same fields as newindex_demo for cross-validation.
 *
 * Usage: java IndexNewindex <docs-dir> <index-dir> [--threads N] [--compound]
 */
public class IndexNewindex {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java IndexNewindex <docs-dir> <index-dir> [--threads N] [--compound]");
            System.exit(1);
        }

        Path docsDir = Paths.get(args[0]);
        Path indexDir = Paths.get(args[1]);
        int numThreads = 1;
        boolean useCompoundFile = false;

        for (int i = 2; i < args.length; i++) {
            if ("--threads".equals(args[i]) && i + 1 < args.length) {
                numThreads = Integer.parseInt(args[++i]);
            } else if ("--compound".equals(args[i])) {
                useCompoundFile = true;
            }
        }

        if (!Files.isDirectory(docsDir)) {
            System.err.println("Document directory '" + docsDir + "' does not exist");
            System.exit(1);
        }

        // Clean up existing index
        if (Files.exists(indexDir)) {
            Files.walk(indexDir)
                .filter(Files::isRegularFile)
                .forEach(p -> { try { Files.delete(p); } catch (IOException e) { /* ignore */ } });
        }
        Files.createDirectories(indexDir);

        System.out.println("Indexing to '" + indexDir + "' from '" + docsDir + "'"
            + (numThreads > 1 ? " (" + numThreads + " threads)" : "") + "...");

        // Collect all file paths first
        List<Path> filePaths = new ArrayList<>();
        Files.walkFileTree(docsDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                filePaths.add(file);
                return FileVisitResult.CONTINUE;
            }
        });

        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setUseCompoundFile(useCompoundFile);
        try (IndexWriter writer = new IndexWriter(FSDirectory.open(indexDir), config)) {
            if (numThreads <= 1) {
                for (Path file : filePaths) {
                    indexDoc(writer, file);
                }
            } else {
                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                for (Path file : filePaths) {
                    executor.submit(() -> {
                        try {
                            indexDoc(writer, file);
                        } catch (IOException e) {
                            System.err.println("Error indexing " + file + ": " + e.getMessage());
                        }
                    });
                }
                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.MINUTES);
            }
        }

        System.out.println("Indexed " + filePaths.size() + " documents. Done.");
    }

    static void indexDoc(IndexWriter writer, Path file) throws IOException {
        String contents = Files.readString(file);
        String fileName = file.getFileName().toString();
        String title = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;
        long fileSize = Files.size(file);
        long modified = Files.getLastModifiedTime(file).toMillis();

        Document doc = new Document();

        // Matches newindex_demo make_document field-for-field:

        // "path" — KeywordField: StringField + SortedSetDocValuesField
        doc.add(new StringField("path", file.toString(), Field.Store.YES));
        doc.add(new SortedSetDocValuesField("path", new BytesRef(file.toString())));

        // "modified" — LongField
        doc.add(new LongField("modified", modified, Field.Store.NO));

        // "contents" — TextField (DEBT: no term vectors yet, needs Phase 8)
        doc.add(new TextField("contents", contents, Field.Store.NO));

        // "title" — StringField (indexed + stored)
        doc.add(new StringField("title", title, Field.Store.YES));

        // "size" — IntField (stored)
        doc.add(new IntField("size", (int) fileSize, Field.Store.YES));

        // "score" — FloatField (stored)
        doc.add(new FloatField("score", (float)(fileSize % 100) / 10.0f, Field.Store.YES));

        // "rating" — DoubleField (stored)
        doc.add(new DoubleField("rating", fileSize * 1.5, Field.Store.YES));

        // Stored-only fields
        doc.add(new StoredField("notes", "indexed by Java"));
        doc.add(new StoredField("extra_int", (int)(fileSize % 1000)));
        doc.add(new StoredField("extra_float", (float)(fileSize % 100) / 3.0f));
        doc.add(new StoredField("extra_double", fileSize * 0.123));

        // LatLonPoint
        double lat = 40.7128 + (fileSize % 10) * 0.01;
        double lon = -74.006 + (fileSize % 10) * 0.01;
        doc.add(new LatLonPoint("location", lat, lon));

        // Range fields
        doc.add(new IntRange("int_range", new int[]{(int) fileSize}, new int[]{(int) fileSize + 100}));
        doc.add(new LongRange("long_range", new long[]{fileSize}, new long[]{fileSize + 1000}));
        doc.add(new FloatRange("float_range", new float[]{fileSize / 10.0f}, new float[]{fileSize / 10.0f + 1.0f}));
        doc.add(new DoubleRange("double_range", new double[]{fileSize * 0.1}, new double[]{fileSize * 0.1 + 1.0}));

        // FeatureField
        doc.add(new FeatureField("features", "pagerank", (float)(fileSize % 100) / 10.0f + 0.5f));
        doc.add(new FeatureField("features", "freshness", (float)(fileSize % 50) / 5.0f + 1.0f));

        // Doc-values-only fields
        doc.add(new NumericDocValuesField("dv_count", fileSize));
        doc.add(new BinaryDocValuesField("dv_hash",
            new BytesRef(String.format("%016x", fileSize))));
        doc.add(new SortedDocValuesField("dv_category", new BytesRef(title)));
        doc.add(new SortedSetDocValuesField("dv_tag", new BytesRef(title)));
        doc.add(new SortedNumericDocValuesField("dv_priority", fileSize % 10));

        // Sparse doc values — only even-numbered docs
        Integer docNum = parseDocNum(title);
        if (docNum != null && docNum % 2 == 0) {
            doc.add(new NumericDocValuesField("sparse_count", docNum * 100L));
        }

        writer.addDocument(doc);
        System.out.println("  indexed: " + file);
    }

    static Integer parseDocNum(String title) {
        int idx = title.lastIndexOf('_');
        if (idx < 0) return null;
        try {
            return Integer.parseInt(title.substring(idx + 1));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
