package com.stratosdb.sql.ast;

import java.util.List;

/**
 * CREATE [OR REPLACE] FUNCTION name(params) RETURNS returnType AS $$ body $$ LANGUAGE lang.
 * body is the dollar-quoted text with its $$ delimiters already stripped -
 * a real SQL statement (currently SELECT-shaped only, see ExecutorEngine)
 * referencing the parameter names as if they were plain identifiers,
 * substituted with the caller's actual argument values at invocation time.
 */
public record CreateFunctionStatement(String name, List<FunctionParam> params, String returnType,
                                       String body, String language, boolean orReplace) implements Statement {}
