// SPDX-License-Identifier: Apache-2.0

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

/**
 * Indexes documents with ALL field types for cross-validation with the Rust indexer.
 *
 * Usage: java IndexAllFields <docs-dir> <index-dir> [--threads N]
 */
public class IndexAllFields {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java IndexAllFields <docs-dir> <index-dir> [--threads N]");
            System.exit(1);
        }

        Path docsDir = Paths.get(args[0]);
        Path indexDir = Paths.get(args[1]);
        int numThreads = 1;

        for (int i = 2; i < args.length; i++) {
            if ("--threads".equals(args[i]) && i + 1 < args.length) {
                numThreads = Integer.parseInt(args[++i]);
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
        long modified = Files.getLastModifiedTime(file).toMillis();
        long fileSize = Files.size(file);
        String fileName = file.getFileName().toString();
        String title = fileName.contains(".") ? fileName.substring(0, fileName.lastIndexOf('.')) : fileName;

        Document doc = new Document();

        // Same 3 fields as the standard indexer
        doc.add(new KeywordField("path", file.toString(), Field.Store.YES));
        doc.add(new LongField("modified", modified, Field.Store.NO));
        doc.add(new TextField("contents", contents, Field.Store.NO));

        // New field types
        doc.add(new StringField("title", title, Field.Store.YES));
        doc.add(new IntField("size", (int) fileSize, Field.Store.YES));
        doc.add(new FloatField("score", (fileSize % 100) / 10.0f, Field.Store.YES));
        doc.add(new DoubleField("rating", fileSize * 1.5, Field.Store.YES));
        doc.add(new StoredField("notes", "indexed by Java"));
        doc.add(new StoredField("extra_int", (int) (fileSize % 1000)));
        doc.add(new StoredField("extra_float", (float) (fileSize % 100) / 3.0f));
        doc.add(new StoredField("extra_double", fileSize * 0.123));

        writer.addDocument(doc);
        System.out.println("  indexed: " + file);
    }
}
