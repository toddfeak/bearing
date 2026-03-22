// SPDX-License-Identifier: Apache-2.0

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;

/**
 * Generates a JSON summary of an index's structure and aggregate statistics.
 *
 * <p>Used for golden-file testing: the output can be compared against a checked-in
 * expected summary to verify that an index (written by Java or Rust) has the
 * correct structure.
 *
 * <p>Usage: GenerateIndexSummary &lt;index_dir&gt;
 */
public class GenerateIndexSummary {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: GenerateIndexSummary <index_dir>");
            System.exit(1);
        }

        Path indexPath = Path.of(args[0]);
        try (FSDirectory dir = FSDirectory.open(indexPath);
             DirectoryReader reader = DirectoryReader.open(dir)) {
            String json = generateSummary(reader);
            System.out.println(json);
        }
    }

    static String generateSummary(DirectoryReader reader) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");

        int totalDocs = reader.numDocs();
        sb.append("  \"totalDocs\": ").append(totalDocs).append(",\n");
        sb.append("  \"maxDoc\": ").append(reader.maxDoc()).append(",\n");

        sb.append("  \"segments\": [\n");
        List<LeafReaderContext> leaves = reader.leaves();
        for (int i = 0; i < leaves.size(); i++) {
            LeafReader leaf = leaves.get(i).reader();
            if (i > 0) sb.append(",\n");
            appendSegment(sb, leaf, i);
        }
        sb.append("\n  ]\n");
        sb.append("}\n");
        return sb.toString();
    }

    static void appendSegment(StringBuilder sb, LeafReader leaf, int index) throws IOException {
        String indent = "    ";
        sb.append(indent).append("{\n");
        sb.append(indent).append("  \"index\": ").append(index).append(",\n");
        sb.append(indent).append("  \"maxDoc\": ").append(leaf.maxDoc()).append(",\n");
        sb.append(indent).append("  \"numDocs\": ").append(leaf.numDocs()).append(",\n");

        // Fields
        sb.append(indent).append("  \"fields\": [\n");
        FieldInfos fieldInfos = leaf.getFieldInfos();
        List<FieldInfo> sorted = new ArrayList<>();
        for (FieldInfo fi : fieldInfos) {
            sorted.add(fi);
        }
        sorted.sort(Comparator.comparingInt(fi -> fi.number));

        for (int i = 0; i < sorted.size(); i++) {
            FieldInfo fi = sorted.get(i);
            if (i > 0) sb.append(",\n");
            appendField(sb, leaf, fi);
        }
        sb.append("\n").append(indent).append("  ]\n");
        sb.append(indent).append("}");
    }

    static void appendField(StringBuilder sb, LeafReader leaf, FieldInfo fi) throws IOException {
        String indent = "        ";
        sb.append(indent).append("{\n");
        sb.append(indent).append("  \"name\": \"").append(escapeJson(fi.name)).append("\",\n");
        sb.append(indent).append("  \"number\": ").append(fi.number).append(",\n");
        sb.append(indent).append("  \"indexOptions\": \"").append(fi.getIndexOptions()).append("\",\n");
        sb.append(indent).append("  \"hasNorms\": ").append(fi.hasNorms()).append(",\n");
        sb.append(indent).append("  \"storeTermVector\": ").append(fi.hasTermVectors()).append(",\n");
        sb.append(indent).append("  \"hasPayloads\": ").append(fi.hasPayloads()).append(",\n");
        sb.append(indent).append("  \"docValuesType\": \"").append(fi.getDocValuesType()).append("\",\n");

        // Point dimensions
        sb.append(indent).append("  \"pointDimensionCount\": ").append(fi.getPointDimensionCount()).append(",\n");
        sb.append(indent).append("  \"pointIndexDimensionCount\": ").append(fi.getPointIndexDimensionCount()).append(",\n");
        sb.append(indent).append("  \"pointNumBytes\": ").append(fi.getPointNumBytes()).append(",\n");

        // Term count (if indexed)
        long termCount = 0;
        if (fi.getIndexOptions() != IndexOptions.NONE) {
            Terms terms = leaf.terms(fi.name);
            if (terms != null) {
                termCount = terms.size();
                if (termCount == -1) {
                    // Some implementations don't know the count; iterate
                    termCount = 0;
                    TermsEnum te = terms.iterator();
                    while (te.next() != null) {
                        termCount++;
                    }
                }
            }
        }
        sb.append(indent).append("  \"termCount\": ").append(termCount).append(",\n");

        // Doc values doc count
        long dvDocCount = 0;
        switch (fi.getDocValuesType()) {
            case NUMERIC:
                NumericDocValues ndv = leaf.getNumericDocValues(fi.name);
                if (ndv != null) while (ndv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) dvDocCount++;
                break;
            case BINARY:
                BinaryDocValues bdv = leaf.getBinaryDocValues(fi.name);
                if (bdv != null) while (bdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) dvDocCount++;
                break;
            case SORTED:
                SortedDocValues sdv = leaf.getSortedDocValues(fi.name);
                if (sdv != null) while (sdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) dvDocCount++;
                break;
            case SORTED_SET:
                SortedSetDocValues ssdv = leaf.getSortedSetDocValues(fi.name);
                if (ssdv != null) while (ssdv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) dvDocCount++;
                break;
            case SORTED_NUMERIC:
                SortedNumericDocValues sndv = leaf.getSortedNumericDocValues(fi.name);
                if (sndv != null) while (sndv.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) dvDocCount++;
                break;
            default:
                break;
        }
        sb.append(indent).append("  \"dvDocCount\": ").append(dvDocCount).append(",\n");

        // Norms doc count
        long normsDocCount = 0;
        if (fi.hasNorms()) {
            NumericDocValues norms = leaf.getNormValues(fi.name);
            if (norms != null) {
                while (norms.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    normsDocCount++;
                }
            }
        }
        sb.append(indent).append("  \"normsDocCount\": ").append(normsDocCount).append("\n");

        sb.append(indent).append("}");
    }

    static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
