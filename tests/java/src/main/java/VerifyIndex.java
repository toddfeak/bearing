// SPDX-License-Identifier: Apache-2.0

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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

/**
 * Standalone Java program that verifies a Lucene index written by the Rust indexer.
 *
 * Usage: java VerifyIndex <index-dir> [expected-doc-count]
 *
 * Exits 0 on success, non-zero on any failure.
 */
public class VerifyIndex {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java VerifyIndex <index-dir> [expected-doc-count]");
            System.exit(1);
        }

        String indexDir = args[0];
        int expectedCount = args.length >= 2 ? Integer.parseInt(args[1]) : 3;
        boolean ok = true;

        System.out.println("=== VerifyIndex: opening " + indexDir + " ===");

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)))) {

            // --- 1. Verify document count ---
            int numDocs = reader.numDocs();
            System.out.println("numDocs = " + numDocs + " (expected " + expectedCount + ")");
            if (numDocs != expectedCount) {
                System.err.println("FAIL: numDocs mismatch");
                ok = false;
            }

            // --- 2. Read stored "path" fields ---
            int sampleSize = Math.min(numDocs, 3);
            System.out.println("\nStored 'path' fields (showing " + sampleSize + " of " + numDocs + "):");
            StoredFields storedFields = reader.storedFields();
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

            // --- 3. Count terms per field ---
            System.out.println("\nTerm counts per field:");
            long totalTerms = 0;
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leaf = ctx.reader();
                FieldInfos fieldInfos = leaf.getFieldInfos();
                for (FieldInfo fi : fieldInfos) {
                    Terms terms = leaf.terms(fi.name);
                    if (terms != null) {
                        long size = terms.size();
                        System.out.println("  field '" + fi.name + "': " + size + " terms");
                        if (size > 0) {
                            totalTerms += size;
                        }
                    }
                }
            }
            System.out.println("  total terms: " + totalTerms);
            if (totalTerms == 0) {
                System.err.println("FAIL: no terms found in any field");
                ok = false;
            }

            // --- 4. Term query on "contents" for "ancient" ---
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(new TermQuery(new Term("contents", "ancient")), 10);
            System.out.println("\nTermQuery 'contents:ancient' => " + hits.totalHits.value() + " hits");
            if (hits.totalHits.value() == 0) {
                System.err.println("FAIL: expected at least 1 hit for 'contents:ancient'");
                ok = false;
            }

            // --- 5. Check extended field types (if present) ---
            ok = checkExtendedFields(reader, storedFields, numDocs, ok);
        }

        // --- Result ---
        System.out.println();
        if (ok) {
            System.out.println("VerifyIndex: ALL CHECKS PASSED");
            System.exit(0);
        } else {
            System.out.println("VerifyIndex: SOME CHECKS FAILED");
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

        for (LeafReaderContext ctx : reader.leaves()) {
            LeafReader leaf = ctx.reader();
            FieldInfos fieldInfos = leaf.getFieldInfos();

            // Check StringField "title" — should have terms in inverted index
            FieldInfo titleFi = fieldInfos.fieldInfo("title");
            if (titleFi != null) {
                System.out.println("\nExtended field checks:");
                Terms titleTerms = leaf.terms("title");
                if (titleTerms == null || titleTerms.size() == 0) {
                    System.err.println("FAIL: 'title' StringField has no terms");
                    ok = false;
                } else {
                    System.out.println("  title: " + titleTerms.size() + " terms (OK)");
                }

                // Verify stored values for title (validate all, print sample)
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
            }

            // Check IntField "size" — should have points
            FieldInfo sizeFi = fieldInfos.fieldInfo("size");
            if (sizeFi != null) {
                PointValues sizePoints = leaf.getPointValues("size");
                if (sizePoints == null) {
                    System.err.println("FAIL: 'size' IntField has no point values");
                    ok = false;
                } else {
                    System.out.println("  size: " + sizePoints.size() + " points, "
                        + sizePoints.getNumDimensions() + " dims, "
                        + sizePoints.getBytesPerDimension() + " bytes/dim (OK)");
                    if (sizePoints.getNumDimensions() != 1 || sizePoints.getBytesPerDimension() != 4) {
                        System.err.println("FAIL: 'size' expected 1 dim, 4 bytes");
                        ok = false;
                    }
                }
                int sizeFailures = 0;
                for (int i = 0; i < numDocs; i++) {
                    Document doc = storedFields.document(i);
                    Number sizeVal = doc.getField("size").numericValue();
                    if (sizeVal == null) {
                        sizeFailures++;
                    } else if (i < sampleSize) {
                        System.out.println("  doc " + i + ": size = " + sizeVal.intValue());
                    }
                }
                if (sizeFailures > 0) {
                    System.err.println("FAIL: " + sizeFailures + " docs have no 'size' stored numeric");
                    ok = false;
                }
            }

            // Check FloatField "score" — should have points
            FieldInfo scoreFi = fieldInfos.fieldInfo("score");
            if (scoreFi != null) {
                PointValues scorePoints = leaf.getPointValues("score");
                if (scorePoints == null) {
                    System.err.println("FAIL: 'score' FloatField has no point values");
                    ok = false;
                } else {
                    System.out.println("  score: " + scorePoints.size() + " points, "
                        + scorePoints.getNumDimensions() + " dims, "
                        + scorePoints.getBytesPerDimension() + " bytes/dim (OK)");
                    if (scorePoints.getNumDimensions() != 1 || scorePoints.getBytesPerDimension() != 4) {
                        System.err.println("FAIL: 'score' expected 1 dim, 4 bytes");
                        ok = false;
                    }
                }
                int scoreFailures = 0;
                for (int i = 0; i < numDocs; i++) {
                    Document doc = storedFields.document(i);
                    Number scoreVal = doc.getField("score").numericValue();
                    if (scoreVal == null) {
                        scoreFailures++;
                    } else if (i < sampleSize) {
                        System.out.println("  doc " + i + ": score = " + scoreVal.floatValue());
                    }
                }
                if (scoreFailures > 0) {
                    System.err.println("FAIL: " + scoreFailures + " docs have no 'score' stored numeric");
                    ok = false;
                }
            }

            // Check DoubleField "rating" — should have points
            FieldInfo ratingFi = fieldInfos.fieldInfo("rating");
            if (ratingFi != null) {
                PointValues ratingPoints = leaf.getPointValues("rating");
                if (ratingPoints == null) {
                    System.err.println("FAIL: 'rating' DoubleField has no point values");
                    ok = false;
                } else {
                    System.out.println("  rating: " + ratingPoints.size() + " points, "
                        + ratingPoints.getNumDimensions() + " dims, "
                        + ratingPoints.getBytesPerDimension() + " bytes/dim (OK)");
                    if (ratingPoints.getNumDimensions() != 1 || ratingPoints.getBytesPerDimension() != 8) {
                        System.err.println("FAIL: 'rating' expected 1 dim, 8 bytes");
                        ok = false;
                    }
                }
                int ratingFailures = 0;
                for (int i = 0; i < numDocs; i++) {
                    Document doc = storedFields.document(i);
                    Number ratingVal = doc.getField("rating").numericValue();
                    if (ratingVal == null) {
                        ratingFailures++;
                    } else if (i < sampleSize) {
                        System.out.println("  doc " + i + ": rating = " + ratingVal.doubleValue());
                    }
                }
                if (ratingFailures > 0) {
                    System.err.println("FAIL: " + ratingFailures + " docs have no 'rating' stored numeric");
                    ok = false;
                }
            }

            // Check StoredField "notes" — stored-only, no index (sample only)
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

            // Check stored-only numeric fields (sample only)
            int extraIntCount = 0, extraFloatCount = 0, extraDoubleCount = 0;
            for (int i = 0; i < numDocs; i++) {
                Document doc = storedFields.document(i);

                if (doc.getField("extra_int") != null) {
                    Number v = doc.getField("extra_int").numericValue();
                    extraIntCount++;
                    if (i < sampleSize) {
                        System.out.println("  doc " + i + ": extra_int = " + (v != null ? v.intValue() : "null"));
                    }
                }
                if (doc.getField("extra_float") != null) {
                    Number v = doc.getField("extra_float").numericValue();
                    extraFloatCount++;
                    if (i < sampleSize) {
                        System.out.println("  doc " + i + ": extra_float = " + (v != null ? v.floatValue() : "null"));
                    }
                }
                if (doc.getField("extra_double") != null) {
                    Number v = doc.getField("extra_double").numericValue();
                    extraDoubleCount++;
                    if (i < sampleSize) {
                        System.out.println("  doc " + i + ": extra_double = " + (v != null ? v.doubleValue() : "null"));
                    }
                }
            }
            if (extraIntCount > 0) {
                System.out.println("  extra_int: " + extraIntCount + "/" + numDocs + " docs");
            }
            if (extraFloatCount > 0) {
                System.out.println("  extra_float: " + extraFloatCount + "/" + numDocs + " docs");
            }
            if (extraDoubleCount > 0) {
                System.out.println("  extra_double: " + extraDoubleCount + "/" + numDocs + " docs");
            }

            // Check LatLonPoint "location" — should have 2 dims, 4 bytes
            FieldInfo locationFi = fieldInfos.fieldInfo("location");
            if (locationFi != null) {
                PointValues locationPoints = leaf.getPointValues("location");
                if (locationPoints == null) {
                    System.err.println("FAIL: 'location' LatLonPoint has no point values");
                    ok = false;
                } else {
                    System.out.println("  location: " + locationPoints.size() + " points, "
                        + locationPoints.getNumDimensions() + " dims, "
                        + locationPoints.getBytesPerDimension() + " bytes/dim (OK)");
                    if (locationPoints.getNumDimensions() != 2 || locationPoints.getBytesPerDimension() != 4) {
                        System.err.println("FAIL: 'location' expected 2 dims, 4 bytes");
                        ok = false;
                    }
                }
            }

            // Check IntRange "int_range" — should have 2 dims (1 range dim * 2), 4 bytes
            FieldInfo intRangeFi = fieldInfos.fieldInfo("int_range");
            if (intRangeFi != null) {
                PointValues intRangePoints = leaf.getPointValues("int_range");
                if (intRangePoints == null) {
                    System.err.println("FAIL: 'int_range' has no point values");
                    ok = false;
                } else {
                    System.out.println("  int_range: " + intRangePoints.size() + " points, "
                        + intRangePoints.getNumDimensions() + " dims, "
                        + intRangePoints.getBytesPerDimension() + " bytes/dim (OK)");
                    if (intRangePoints.getNumDimensions() != 2 || intRangePoints.getBytesPerDimension() != 4) {
                        System.err.println("FAIL: 'int_range' expected 2 dims, 4 bytes");
                        ok = false;
                    }
                }
            }

            // Check LongRange "long_range" — should have 2 dims, 8 bytes
            FieldInfo longRangeFi = fieldInfos.fieldInfo("long_range");
            if (longRangeFi != null) {
                PointValues longRangePoints = leaf.getPointValues("long_range");
                if (longRangePoints == null) {
                    System.err.println("FAIL: 'long_range' has no point values");
                    ok = false;
                } else {
                    System.out.println("  long_range: " + longRangePoints.size() + " points, "
                        + longRangePoints.getNumDimensions() + " dims, "
                        + longRangePoints.getBytesPerDimension() + " bytes/dim (OK)");
                    if (longRangePoints.getNumDimensions() != 2 || longRangePoints.getBytesPerDimension() != 8) {
                        System.err.println("FAIL: 'long_range' expected 2 dims, 8 bytes");
                        ok = false;
                    }
                }
            }

            // Check FloatRange "float_range" — should have 2 dims, 4 bytes
            FieldInfo floatRangeFi = fieldInfos.fieldInfo("float_range");
            if (floatRangeFi != null) {
                PointValues floatRangePoints = leaf.getPointValues("float_range");
                if (floatRangePoints == null) {
                    System.err.println("FAIL: 'float_range' has no point values");
                    ok = false;
                } else {
                    System.out.println("  float_range: " + floatRangePoints.size() + " points, "
                        + floatRangePoints.getNumDimensions() + " dims, "
                        + floatRangePoints.getBytesPerDimension() + " bytes/dim (OK)");
                    if (floatRangePoints.getNumDimensions() != 2 || floatRangePoints.getBytesPerDimension() != 4) {
                        System.err.println("FAIL: 'float_range' expected 2 dims, 4 bytes");
                        ok = false;
                    }
                }
            }

            // Check DoubleRange "double_range" — should have 2 dims, 8 bytes
            FieldInfo doubleRangeFi = fieldInfos.fieldInfo("double_range");
            if (doubleRangeFi != null) {
                PointValues doubleRangePoints = leaf.getPointValues("double_range");
                if (doubleRangePoints == null) {
                    System.err.println("FAIL: 'double_range' has no point values");
                    ok = false;
                } else {
                    System.out.println("  double_range: " + doubleRangePoints.size() + " points, "
                        + doubleRangePoints.getNumDimensions() + " dims, "
                        + doubleRangePoints.getBytesPerDimension() + " bytes/dim (OK)");
                    if (doubleRangePoints.getNumDimensions() != 2 || doubleRangePoints.getBytesPerDimension() != 8) {
                        System.err.println("FAIL: 'double_range' expected 2 dims, 8 bytes");
                        ok = false;
                    }
                }
            }

            // Check FeatureField "features" — DOCS_AND_FREQS index options
            FieldInfo featuresFi = fieldInfos.fieldInfo("features");
            if (featuresFi != null) {
                Terms featuresTerms = leaf.terms("features");
                if (featuresTerms == null || featuresTerms.size() == 0) {
                    System.err.println("FAIL: 'features' FeatureField has no terms");
                    ok = false;
                } else {
                    System.out.println("  features: " + featuresTerms.size() + " terms (OK)");
                }
            }

            // Check doc-values-only fields (if present)
            int leafDocs = leaf.numDocs();
            int leafSampleSize = Math.min(leafDocs, 3);
            ok = checkDocValues(leaf, leafDocs, leafSampleSize, ok);
        }

        return ok;
    }

    static boolean checkDocValues(LeafReader leaf, int numDocs, int sampleSize,
                                  boolean ok) throws Exception {
        FieldInfos fieldInfos = leaf.getFieldInfos();

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
