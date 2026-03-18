// SPDX-License-Identifier: Apache-2.0

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

/**
 * Indexes documents with all doc values field types for cross-validation
 * with the Rust indexer's new NUMERIC, BINARY, and SORTED support.
 *
 * Usage: java IndexDocValues <index-dir>
 */
public class IndexDocValues {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java IndexDocValues <index-dir>");
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
                doc.add(new TextField("body", "doc values test " + i, TextField.Store.NO));
                doc.add(new NumericDocValuesField("count", i * 10L));
                doc.add(new BinaryDocValuesField("hash",
                    new BytesRef(new byte[]{ (byte)(i * 11), (byte)(i * 22) })));
                doc.add(new SortedDocValuesField("category",
                    new BytesRef("cat-" + (i % 3))));
                doc.add(new SortedSetDocValuesField("tag",
                    new BytesRef("tag-" + (i % 5))));
                doc.add(new SortedNumericDocValuesField("priority", i % 4));
                writer.addDocument(doc);
            }
        }

        System.out.println("Indexed 10 documents with doc values to '" + indexDir + "'");
    }
}
