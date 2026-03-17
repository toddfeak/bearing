// SPDX-License-Identifier: Apache-2.0

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;

/**
 * Verifies that a Lucene index uses suffix compression in .tim blocks.
 *
 * Uses reflection to access package-private fields on the blocktree reader
 * to inspect the actual compression algorithm stored in each block.
 *
 * Usage: java VerifyTimCompression <index-dir>
 *
 * Requires JVM arg:
 *   --add-opens org.apache.lucene.core/org.apache.lucene.codecs.lucene103.blocktree=ALL-UNNAMED
 *
 * Exits 0 if at least one block uses LOWERCASE_ASCII and one uses LZ4.
 * Exits 1 if no compression is found.
 */
public class VerifyTimCompression {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java VerifyTimCompression <index-dir>");
            System.exit(1);
        }

        String indexDir = args[0];
        System.out.println("=== VerifyTimCompression: opening " + indexDir + " ===");

        // Compression code names matching CompressionAlgorithm enum
        String[] ALG_NAMES = {"NO_COMPRESSION", "LOWERCASE_ASCII", "LZ4"};

        // Overall counts across all fields
        Map<Integer, Integer> totalCounts = new HashMap<>();
        totalCounts.put(0, 0);
        totalCounts.put(1, 0);
        totalCounts.put(2, 0);

        try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)))) {
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leaf = ctx.reader();
                FieldInfos fieldInfos = leaf.getFieldInfos();

                for (FieldInfo fi : fieldInfos) {
                    if (fi.getIndexOptions() == IndexOptions.NONE) {
                        continue;
                    }

                    Terms terms = leaf.terms(fi.name);
                    if (terms == null) {
                        continue;
                    }

                    TermsEnum termsEnum = terms.iterator();

                    // Access the currentFrame field via reflection
                    Field currentFrameField = termsEnum.getClass().getDeclaredField("currentFrame");
                    currentFrameField.setAccessible(true);

                    // Get the compressionAlg and fp fields from the frame class
                    Object frame = currentFrameField.get(termsEnum);
                    Class<?> frameClass = frame.getClass();
                    Field compressionAlgField = frameClass.getDeclaredField("compressionAlg");
                    compressionAlgField.setAccessible(true);
                    Field fpField = frameClass.getDeclaredField("fp");
                    fpField.setAccessible(true);

                    // Track unique blocks by FP
                    Map<Long, Integer> blockAlgs = new HashMap<>();

                    // CompressionAlgorithm is an enum with abstract methods, so each
                    // constant is an anonymous inner class. The 'code' field is on
                    // the declaring superclass, not the anonymous subclass.
                    Field codeField = null;

                    while (termsEnum.next() != null) {
                        frame = currentFrameField.get(termsEnum);
                        long fp = fpField.getLong(frame);

                        if (!blockAlgs.containsKey(fp)) {
                            Object alg = compressionAlgField.get(frame);
                            if (codeField == null) {
                                // Resolve once — the declaring class has the 'code' field
                                Class<?> algClass = alg.getClass();
                                while (algClass != null) {
                                    try {
                                        codeField = algClass.getDeclaredField("code");
                                        codeField.setAccessible(true);
                                        break;
                                    } catch (NoSuchFieldException e) {
                                        algClass = algClass.getSuperclass();
                                    }
                                }
                            }
                            int code = codeField.getInt(alg);
                            blockAlgs.put(fp, code);
                        }
                    }

                    // Aggregate per-field counts
                    Map<Integer, Integer> fieldCounts = new HashMap<>();
                    fieldCounts.put(0, 0);
                    fieldCounts.put(1, 0);
                    fieldCounts.put(2, 0);
                    for (int code : blockAlgs.values()) {
                        fieldCounts.merge(code, 1, Integer::sum);
                        totalCounts.merge(code, 1, Integer::sum);
                    }

                    System.out.println("  Field '" + fi.name + "': "
                        + fieldCounts.get(0) + " NO_COMPRESSION, "
                        + fieldCounts.get(1) + " LOWERCASE_ASCII, "
                        + fieldCounts.get(2) + " LZ4");
                }
            }
        }

        System.out.println();
        System.out.println("Total blocks:");
        for (int code = 0; code < 3; code++) {
            System.out.println("  " + ALG_NAMES[code] + ": " + totalCounts.get(code));
        }

        int lowercaseCount = totalCounts.get(1);
        int lz4Count = totalCounts.get(2);

        if (lowercaseCount == 0 && lz4Count == 0) {
            System.err.println("FAIL: no blocks use any compression");
            System.exit(1);
        }

        if (lowercaseCount == 0) {
            System.err.println("FAIL: no blocks use LOWERCASE_ASCII compression");
            System.exit(1);
        }

        if (lz4Count == 0) {
            System.err.println("FAIL: no blocks use LZ4 compression");
            System.exit(1);
        }

        System.out.println("VerifyTimCompression: PASSED");
    }
}
