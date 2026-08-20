package com.stratosdb.sql.ast;

import java.util.List;

/**
 * CREATE [OR REPLACE] FUNCTION name(params) RETURNS type AS
 * extension_name, 'native_symbol' LANGUAGE C.
 *
 * A deliberate departure from real Postgres's own C-function syntax
 * (AS 'obj_file', 'link_symbol' - two string literals, no prior
 * CREATE EXTENSION needed): extensionName here is the name of an
 * already-registered CreateExtensionStatement, looked up in
 * ExecutorEngine's own extension registry rather than re-specifying a
 * raw file path on every function. See executeCreateNativeFunction's
 * own javadoc for the real, honestly-stated scope - integer-only
 * arguments and return value in this first version, since that's the
 * calling convention NativeExtensionBridge's own fixed JNI entry point
 * actually implements.
 */
public record CreateNativeFunctionStatement(String name, List<FunctionParam> params, String returnType,
                                             String extensionName, String nativeSymbol, boolean orReplace) implements Statement {}
