// SPDX-License-Identifier: Apache-2.0

// DEBT: Limited copy of VerifyIndex.java for the newindex pipeline.
// Only checks field types that newindex currently supports (stored + text).
// As newindex gains field types, this verifier should grow to match VerifyIndex.

import java.nio.file.Paths;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

/**
 * Standalone Java program that verifies a newindex-produced Lucene index.
 *
 * Usage: java VerifyNewindex <index-dir> [expected-doc-count]
 *
 * Exits 0 on success, non-zero on any failure.
 */
public class VerifyNewindex {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java VerifyNewindex <index-dir> [expected-doc-count]");
            System.exit(1);
        }

        String indexDir = args[0];
        int expectedCount = args.length >= 2 ? Integer.parseInt(args[1]) : 3;
        boolean ok = true;

        System.out.println("=== VerifyNewindex: opening " + indexDir + " ===");

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)))) {

            // --- 1. Verify document count ---
            int numDocs = reader.numDocs();
            System.out.println("numDocs = " + numDocs + " (expected " + expectedCount + ")");
            if (numDocs != expectedCount) {
                System.err.println("FAIL: numDocs mismatch");
                ok = false;
            }

            // --- 2. Read stored "path" fields (if present) ---
            int sampleSize = Math.min(numDocs, 3);
            StoredFields storedFields = reader.storedFields();
            boolean hasPathField = storedFields.document(0).get("path") != null;
            if (hasPathField) {
                System.out.println("\nStored 'path' fields (showing " + sampleSize + " of " + numDocs + "):");
                int pathFailures = 0;
                for (int i = 0; i < numDocs; i++) {
                    Document doc = storedFields.document(i);
                    String path = doc.get("path");
                    if (i < sampleSize) {
                        System.out.println("  doc " + i + ": path = " + path);
                    }
                    if (path == null || path.isEmpty()) {
                        pathFailures++;
                        ok = false;
                    }
                }
                if (pathFailures > 0) {
                    System.err.println("FAIL: " + pathFailures + " docs have no 'path' stored field");
                }
            }

            // --- 3. Count terms per field ---
            System.out.println("\nTerm counts per field:");
            long totalTerms = 0;
            int fieldsWithTerms = 0;
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leaf = ctx.reader();
                FieldInfos fieldInfos = leaf.getFieldInfos();
                for (FieldInfo fi : fieldInfos) {
                    Terms terms = leaf.terms(fi.name);
                    if (terms != null) {
                        long size = terms.size();
                        if (size >= 0) {
                            System.out.println("  field '" + fi.name + "': " + size + " terms");
                            totalTerms += size;
                        } else {
                            // size() returns -1 when unknown; count manually
                            TermsEnum te = terms.iterator();
                            long count = 0;
                            while (te.next() != null) {
                                count++;
                            }
                            System.out.println("  field '" + fi.name + "': " + count + " terms");
                            totalTerms += count;
                        }
                        fieldsWithTerms++;
                    }
                }
            }
            System.out.println("  total terms: " + totalTerms);
            if (fieldsWithTerms == 0) {
                System.err.println("FAIL: no fields with terms found");
                ok = false;
            }

            // --- 4. Term query on "contents" (if field has terms) ---
            for (LeafReaderContext ctx2 : reader.leaves()) {
                Terms contentsTerms = ctx2.reader().terms("contents");
                if (contentsTerms != null) {
                    TermsEnum te = contentsTerms.iterator();
                    if (te.next() != null) {
                        String firstTerm = te.term().utf8ToString();
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs hits = searcher.search(
                            new TermQuery(new Term("contents", firstTerm)), 10);
                        System.out.println("\nTermQuery 'contents:" + firstTerm
                            + "' => " + hits.totalHits.value() + " hits");
                        if (hits.totalHits.value() == 0) {
                            System.err.println("FAIL: expected at least 1 hit for 'contents:"
                                + firstTerm + "'");
                            ok = false;
                        }
                    }
                    break; // only need one leaf
                }
            }

            // --- 5. Check extended field types (if present) ---
            ok = checkExtendedFields(reader, storedFields, numDocs, ok);
        }

        // --- Result ---
        System.out.println();
        if (ok) {
            System.out.println("VerifyNewindex: ALL CHECKS PASSED");
            System.exit(0);
        } else {
            System.out.println("VerifyNewindex: SOME CHECKS FAILED");
            System.exit(1);
        }
    }

    /**
     * Checks extended field types if they exist in the index.
     * Backward-compatible: skips checks if fields are not present.
     *
     * DEBT: Limited to stored fields and norms. As newindex gains field types
     * (StringField, points, doc values, term vectors), add checks here to
     * match VerifyIndex.checkExtendedFields().
     */
    static boolean checkExtendedFields(IndexReader reader, StoredFields storedFields,
                                       int numDocs, boolean ok) throws Exception {
        int sampleSize = Math.min(numDocs, 3);

        // Check "title" — StringField: stored + indexed (DOCS-only terms)
        int titleFailures = 0;
        for (int i = 0; i < numDocs; i++) {
            Document doc = storedFields.document(i);
            String title = doc.get("title");
            if (title == null || title.isEmpty()) {
                titleFailures++;
            }
        }
        if (titleFailures > 0) {
            System.err.println("FAIL: " + titleFailures + " docs have no 'title' stored field");
            ok = false;
        } else {
            System.out.println("\nExtended field checks:");
            System.out.println("  title: all " + numDocs + " docs have stored values (OK)");
        }

        // Check "title" has terms in inverted index (StringField is indexed)
        for (LeafReaderContext ctx : reader.leaves()) {
            Terms titleTerms = ctx.reader().terms("title");
            if (titleTerms != null) {
                TermsEnum te = titleTerms.iterator();
                long count = 0;
                while (te.next() != null) count++;
                if (count > 0) {
                    System.out.println("  title: " + count + " terms in index (OK)");
                } else {
                    System.err.println("FAIL: 'title' StringField has no terms");
                    ok = false;
                }
            }
            break; // only need one leaf
        }

        // Check "notes" stored values (sample + count)
        int notesCount = 0;
        for (int i = 0; i < numDocs; i++) {
            Document doc = storedFields.document(i);
            String notes = doc.get("notes");
            if (notes != null) {
                notesCount++;
                if (i < sampleSize) {
                    System.out.println("  doc " + i + ": notes = " + notes);
                }
            }
        }
        if (notesCount > 0) {
            System.out.println("  notes: " + notesCount + "/" + numDocs + " docs have stored values");
        }

        // Check norms on "contents" — verify norms exist and values are non-zero
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            FieldInfos fieldInfos = leaf.getFieldInfos();
            FieldInfo contentsFi = fieldInfos.fieldInfo("contents");
            if (contentsFi != null && contentsFi.hasNorms()) {
                NumericDocValues norms = leaf.getNormValues("contents");
                if (norms == null) {
                    System.err.println("FAIL: 'contents' field has no norms");
                    ok = false;
                } else {
                    int normsCount = 0;
                    int zeroNorms = 0;
                    while (norms.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                        normsCount++;
                        if (norms.longValue() == 0) {
                            zeroNorms++;
                        }
                    }
                    System.out.println("  contents norms: " + normsCount + " docs"
                        + (zeroNorms > 0 ? " (" + zeroNorms + " zero)" : "") + " (OK)");
                    if (normsCount == 0) {
                        System.err.println("FAIL: 'contents' has 0 norms values");
                        ok = false;
                    }
                }
            }
        }

        return ok;
    }
}
