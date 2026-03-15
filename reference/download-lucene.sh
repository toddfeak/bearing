#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

# Downloads and builds Apache Lucene 10.3.2 source code into this directory.
# The built JARs are used by test scripts (e2e_indexfiles.sh, compare_java_rust.sh)
# and the source is used as the canonical reference for porting.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LUCENE_VERSION="10.3.2"
LUCENE_DIR="$SCRIPT_DIR/lucene-${LUCENE_VERSION}"
TARBALL="lucene-${LUCENE_VERSION}-src.tgz"
DOWNLOAD_URL="https://archive.apache.org/dist/lucene/java/${LUCENE_VERSION}/${TARBALL}"

if [ -d "$LUCENE_DIR" ]; then
    echo "Lucene $LUCENE_VERSION already exists at $LUCENE_DIR"
    echo "To re-download, remove it first: rm -rf $LUCENE_DIR"
    exit 0
fi

echo "Downloading Lucene $LUCENE_VERSION source..."
curl -fSL -o "$SCRIPT_DIR/$TARBALL" "$DOWNLOAD_URL"

echo "Extracting..."
tar -xzf "$SCRIPT_DIR/$TARBALL" -C "$SCRIPT_DIR"
rm "$SCRIPT_DIR/$TARBALL"

echo "Building lucene-core JAR..."
cd "$LUCENE_DIR"
./gradlew :lucene:core:jar --no-daemon -q

JAR_PATH="$LUCENE_DIR/lucene/core/build/libs/lucene-core-${LUCENE_VERSION}-SNAPSHOT.jar"
if [ -f "$JAR_PATH" ]; then
    echo "Build successful: $JAR_PATH"
else
    echo "ERROR: Expected JAR not found at $JAR_PATH"
    exit 1
fi

echo "Done. Lucene $LUCENE_VERSION source and JARs are ready."
