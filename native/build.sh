#!/bin/sh
# Builds libstratosbridge.so - StratosDB's own fixed native extension
# bridge. See README.md in this directory and NativeExtensionBridge.java
# (stratosdb-sql module) for what this is and why it exists.
#
# This is a real, separate, one-time native build step outside Maven's
# own build (Maven does not compile C code). StratosDB itself runs
# completely fine with no extensions and without ever running this
# script at all - it's only needed the moment CREATE EXTENSION is
# actually used.
#
# Requires: gcc, and a JDK install with its own jni.h/jni_md.h headers
# (set JAVA_HOME if it isn't already, or this script tries a few common
# locations).
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -z "$JAVA_HOME" ]; then
    for candidate in /usr/lib/jvm/java-21-openjdk-amd64 /usr/lib/jvm/default-java "$(dirname "$(dirname "$(readlink -f "$(which java)")")")"; do
        if [ -f "$candidate/include/jni.h" ]; then
            JAVA_HOME="$candidate"
            break
        fi
    done
fi
if [ -z "$JAVA_HOME" ] || [ ! -f "$JAVA_HOME/include/jni.h" ]; then
    echo "Could not find a JDK with jni.h - set JAVA_HOME to your JDK install and re-run." >&2
    exit 1
fi

JNI_MD_DIR="$JAVA_HOME/include/linux"
if [ ! -d "$JNI_MD_DIR" ]; then
    JNI_MD_DIR="$JAVA_HOME/include/darwin" # macOS
fi

echo "Using JAVA_HOME=$JAVA_HOME"

# Compile NativeExtensionBridge.java on its own (no other module classes needed - it
# only uses the JDK and slf4j) so javac -h can generate a fresh, guaranteed-correct
# JNI header matching the actual class, rather than trusting a hand-maintained one.
BRIDGE_JAVA="$REPO_ROOT/stratosdb-sql/src/main/java/com/stratosdb/sql/extension/NativeExtensionBridge.java"
SLF4J_JARS=$(find / -maxdepth 6 -name "slf4j-api*.jar" 2>/dev/null | head -1)
if [ -z "$SLF4J_JARS" ]; then
    echo "Warning: slf4j-api jar not found on this machine; if javac fails below, add it to -cp." >&2
fi

javac -cp "$SLF4J_JARS" -d "$SCRIPT_DIR/classes" -h "$SCRIPT_DIR" "$BRIDGE_JAVA"

gcc -shared -fPIC \
    -I"$JAVA_HOME/include" -I"$JNI_MD_DIR" \
    -I"$SCRIPT_DIR" \
    "$SCRIPT_DIR/stratosbridge.c" \
    -o "$SCRIPT_DIR/$(uname -s | grep -qi darwin && echo libstratosbridge.dylib || echo libstratosbridge.so)" \
    -ldl

echo "Built $SCRIPT_DIR/libstratosbridge.so"
echo "Run StratosDB with this directory on the classpath / working directory as ./native, or set STRATOSDB_NATIVE_DIR=$SCRIPT_DIR"
