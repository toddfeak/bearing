// SPDX-License-Identifier: Apache-2.0

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;

/**
 * Indexes 10 documents with term vectors for cross-validation with the
 * Rust e2e_term_vectors integration test.
 *
 * Usage: java IndexTermVectors <index-dir>
 */
public class IndexTermVectors {

    private static final FieldType TV_TYPE;
    static {
        TV_TYPE = new FieldType();
        TV_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        TV_TYPE.setTokenized(true);
        TV_TYPE.setStoreTermVectors(true);
        TV_TYPE.setStoreTermVectorPositions(true);
        TV_TYPE.setStoreTermVectorOffsets(true);
        TV_TYPE.freeze();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java IndexTermVectors <index-dir>");
            System.exit(1);
        }

        Path indexDir = Paths.get(args[0]);

        if (Files.exists(indexDir)) {
            Files.walk(indexDir)
                .filter(Files::isRegularFile)
                .forEach(p -> { try { Files.delete(p); } catch (IOException e) { /* ignore */ } });
        }
        Files.createDirectories(indexDir);

        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
        config.setUseCompoundFile(false);

        try (IndexWriter writer = new IndexWriter(FSDirectory.open(indexDir), config)) {
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                String docId = String.format("doc-%03d", i);
                doc.add(new StringField("id", docId, Field.Store.YES));
                doc.add(new TextField("body", "doc values test " + i, TextField.Store.NO));
                doc.add(new Field("contents",
                    "the quick brown fox jumps over the lazy dog number " + i, TV_TYPE));
                writer.addDocument(doc);
            }
        }

        System.out.println("Indexed 10 documents with term vectors to '" + indexDir + "'");
    }
}
