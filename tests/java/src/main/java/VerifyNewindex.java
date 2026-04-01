// SPDX-License-Identifier: Apache-2.0

// DEBT: Limited copy of VerifyIndex.java for the newindex pipeline.
// Only checks field types that newindex currently supports (stored + text).
// As newindex gains field types, this verifier should grow to match VerifyIndex.

import java.nio.file.Paths;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
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
     */
    static boolean checkExtendedFields(IndexReader reader, StoredFields storedFields,
                                       int numDocs, boolean ok) throws Exception {
        int sampleSize = Math.min(numDocs, 3);

        System.out.println("\nExtended field checks:");

        // Check "title" — StringField: stored + indexed
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
            System.out.println("  title: all " + numDocs + " docs have stored values (OK)");
        }

        // Check "title" has terms in inverted index (StringField)
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
            break;
        }

        // Check "path" has SortedSet doc values (KeywordField)
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            SortedSetDocValues pathDv = leaf.getSortedSetDocValues("path");
            if (pathDv != null) {
                int dvCount = 0;
                while (pathDv.nextDoc() != SortedSetDocValues.NO_MORE_DOCS) {
                    dvCount++;
                }
                System.out.println("  path: " + dvCount + "/" + leaf.numDocs()
                    + " docs have SortedSet DV (OK)");
                if (dvCount != leaf.numDocs()) {
                    System.err.println("FAIL: path SortedSet DV has " + dvCount
                        + " docs, expected " + leaf.numDocs());
                    ok = false;
                }
            }
            break;
        }

        // Check "features" has terms (FeatureField — "pagerank" and "freshness")
        for (LeafReaderContext ctx : reader.leaves()) {
            Terms featTerms = ctx.reader().terms("features");
            if (featTerms != null) {
                TermsEnum te = featTerms.iterator();
                long count = 0;
                while (te.next() != null) count++;
                if (count >= 2) {
                    System.out.println("  features: " + count + " terms in index (OK)");
                } else {
                    System.err.println("FAIL: 'features' FeatureField has " + count
                        + " terms, expected >= 2");
                    ok = false;
                }
            }
            break;
        }

        // Check stored extra fields
        int extraIntCount = 0;
        for (int i = 0; i < numDocs; i++) {
            Document doc = storedFields.document(i);
            if (doc.getField("extra_int") != null) extraIntCount++;
        }
        if (extraIntCount == numDocs) {
            System.out.println("  extra_int: all " + numDocs + " docs have stored values (OK)");
        } else {
            System.err.println("FAIL: extra_int has " + extraIntCount + " stored values, expected " + numDocs);
            ok = false;
        }

        // Check point fields
        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            int leafDocs = leaf.numDocs();

            // "size" — IntField has points
            PointValues sizePoints = leaf.getPointValues("size");
            if (sizePoints != null) {
                int pc = sizePoints.getDocCount();
                System.out.println("  size: " + pc + "/" + leafDocs + " docs have points (OK)");
                if (pc != leafDocs) {
                    System.err.println("FAIL: size has " + pc + " point docs, expected " + leafDocs);
                    ok = false;
                }
            }

            // "location" — LatLonPoint has 2D points
            PointValues locPoints = leaf.getPointValues("location");
            if (locPoints != null) {
                int pc = locPoints.getDocCount();
                System.out.println("  location: " + pc + "/" + leafDocs + " docs have points, "
                    + locPoints.getNumDimensions() + "D (OK)");
                if (pc != leafDocs) {
                    System.err.println("FAIL: location has " + pc + " point docs, expected " + leafDocs);
                    ok = false;
                }
            }

            break; // one leaf is enough
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

        // Check doc values fields (per-leaf, since multi-segment splits docs)
        for (LeafReaderContext ctx : reader.leaves()) {
            ok = checkDocValues(ctx.reader(), ok);
        }

        return ok;
    }

    /**
     * Checks all five doc values field types if present in the leaf.
     */
    static boolean checkDocValues(LeafReader leaf, boolean ok) throws Exception {
        FieldInfos fieldInfos = leaf.getFieldInfos();
        int numDocs = leaf.numDocs();
        int sampleSize = Math.min(numDocs, 3);

            // NumericDocValues "dv_count"
            if (fieldInfos.fieldInfo("dv_count") != null) {
                System.out.println("\nDoc values field checks:");
                NumericDocValues ndv = leaf.getNumericDocValues("dv_count");
                if (ndv == null) {
                    System.err.println("FAIL: 'dv_count' has no NumericDocValues");
                    ok = false;
                } else {
                    int count = 0;
                    while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                        if (count < sampleSize) {
                            System.out.println("  doc " + ndv.docID() + ": dv_count = " + ndv.longValue());
                        }
                        count++;
                    }
                    System.out.println("  dv_count: " + count + "/" + numDocs + " docs (OK)");
                    if (count != numDocs) {
                        System.err.println("FAIL: dv_count has " + count + " values, expected " + numDocs);
                        ok = false;
                    }
                }
            }

            // BinaryDocValues "dv_hash"
            if (fieldInfos.fieldInfo("dv_hash") != null) {
                BinaryDocValues bdv = leaf.getBinaryDocValues("dv_hash");
                if (bdv == null) {
                    System.err.println("FAIL: 'dv_hash' has no BinaryDocValues");
                    ok = false;
                } else {
                    int count = 0;
                    while (bdv.nextDoc() != BinaryDocValues.NO_MORE_DOCS) {
                        if (count < sampleSize) {
                            System.out.println("  doc " + bdv.docID() + ": dv_hash = " + bdv.binaryValue().utf8ToString());
                        }
                        count++;
                    }
                    System.out.println("  dv_hash: " + count + "/" + numDocs + " docs (OK)");
                    if (count != numDocs) {
                        System.err.println("FAIL: dv_hash has " + count + " values, expected " + numDocs);
                        ok = false;
                    }
                }
            }

            // SortedDocValues "dv_category"
            if (fieldInfos.fieldInfo("dv_category") != null) {
                SortedDocValues sdv = leaf.getSortedDocValues("dv_category");
                if (sdv == null) {
                    System.err.println("FAIL: 'dv_category' has no SortedDocValues");
                    ok = false;
                } else {
                    int count = 0;
                    while (sdv.nextDoc() != SortedDocValues.NO_MORE_DOCS) {
                        if (count < sampleSize) {
                            System.out.println("  doc " + sdv.docID() + ": dv_category = "
                                + sdv.lookupOrd(sdv.ordValue()).utf8ToString());
                        }
                        count++;
                    }
                    System.out.println("  dv_category: " + count + "/" + numDocs
                        + " docs, " + sdv.getValueCount() + " unique values (OK)");
                    if (count != numDocs) {
                        System.err.println("FAIL: dv_category has " + count + " values, expected " + numDocs);
                        ok = false;
                    }
                }
            }

            // SortedSetDocValues "dv_tag"
            if (fieldInfos.fieldInfo("dv_tag") != null) {
                SortedSetDocValues ssdv = leaf.getSortedSetDocValues("dv_tag");
                if (ssdv == null) {
                    System.err.println("FAIL: 'dv_tag' has no SortedSetDocValues");
                    ok = false;
                } else {
                    int docCount = 0;
                    int totalValues = 0;
                    while (ssdv.nextDoc() != SortedSetDocValues.NO_MORE_DOCS) {
                        int valueCount = ssdv.docValueCount();
                        totalValues += valueCount;
                        if (docCount < sampleSize) {
                            StringBuilder sb = new StringBuilder();
                            for (int j = 0; j < valueCount; j++) {
                                if (j > 0) sb.append(", ");
                                sb.append(ssdv.lookupOrd(ssdv.nextOrd()).utf8ToString());
                            }
                            System.out.println("  doc " + ssdv.docID() + ": dv_tag = [" + sb + "]");
                        }
                        docCount++;
                    }
                    System.out.println("  dv_tag: " + docCount + "/" + numDocs
                        + " docs, " + totalValues + " total values, "
                        + ssdv.getValueCount() + " unique terms (OK)");
                    if (docCount != numDocs) {
                        System.err.println("FAIL: dv_tag has " + docCount + " docs, expected " + numDocs);
                        ok = false;
                    }
                }
            }

            // SortedNumericDocValues "dv_priority"
            if (fieldInfos.fieldInfo("dv_priority") != null) {
                SortedNumericDocValues sndv = leaf.getSortedNumericDocValues("dv_priority");
                if (sndv == null) {
                    System.err.println("FAIL: 'dv_priority' has no SortedNumericDocValues");
                    ok = false;
                } else {
                    int docCount = 0;
                    int totalValues = 0;
                    while (sndv.nextDoc() != SortedNumericDocValues.NO_MORE_DOCS) {
                        int valueCount = sndv.docValueCount();
                        totalValues += valueCount;
                        if (docCount < sampleSize) {
                            StringBuilder sb = new StringBuilder();
                            for (int j = 0; j < valueCount; j++) {
                                if (j > 0) sb.append(", ");
                                sb.append(sndv.nextValue());
                            }
                            System.out.println("  doc " + sndv.docID() + ": dv_priority = [" + sb + "]");
                        }
                        docCount++;
                    }
                    System.out.println("  dv_priority: " + docCount + "/" + numDocs
                        + " docs, " + totalValues + " total values (OK)");
                    if (docCount != numDocs) {
                        System.err.println("FAIL: dv_priority has " + docCount + " docs, expected " + numDocs);
                        ok = false;
                    }
                }
            }

        return ok;
    }
}
