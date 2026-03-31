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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

/**
 * Indexes documents with the same fields as newindex_demo for cross-validation.
 *
 * Fields:
 *   "path"     — StoredField (stored only, not indexed)
 *   "contents" — TextField (tokenized + stored)
 *   "title"    — StoredField (stored only, not indexed)
 *   "notes"    — StoredField (stored only, not indexed)
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

        Document doc = new Document();

        // Same 4 fields as newindex_demo make_document:
        doc.add(new StringField("path", file.toString(), Field.Store.YES));
        doc.add(new TextField("contents", contents, Field.Store.NO));
        doc.add(new StringField("title", title, Field.Store.YES));
        doc.add(new StoredField("notes", "indexed by Java"));

        writer.addDocument(doc);
        System.out.println("  indexed: " + file);
    }
}
