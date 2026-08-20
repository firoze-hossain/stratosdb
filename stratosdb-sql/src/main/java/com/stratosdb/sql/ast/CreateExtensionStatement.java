package com.stratosdb.sql.ast;

/**
 * CREATE EXTENSION name AS 'path_to_shared_library.so'.
 *
 * A real, honest, Java-native-code-based analog of real Postgres's own
 * CREATE EXTENSION - this engine is written in Java, not C, so instead
 * of Postgres's own dynamic-loader-plus-fmgr machinery, this loads the
 * given shared library via a fixed, pre-built JNI bridge
 * (NativeExtensionBridge) that internally uses real dlopen/dlsym, not a
 * separate, invented mechanism - see ExecutorEngine.executeCreateExtension
 * and NativeExtensionBridge's own javadoc for the full, honestly-stated
 * scope and real limitations (a native library, once loaded into this
 * JVM process, cannot be unloaded again - DROP EXTENSION removes the SQL-
 * level registration, not the loaded library itself).
 *
 * libraryPath holds the raw, still-quoted STRING_LITERAL text exactly as
 * parsed (matching this project's own established convention - e.g.
 * CallStatement's own args()) - unquoted/unescaped by the executor via
 * its existing parseLiteral, not re-implemented here.
 */
public record CreateExtensionStatement(String name, String libraryPath) implements Statement {}
