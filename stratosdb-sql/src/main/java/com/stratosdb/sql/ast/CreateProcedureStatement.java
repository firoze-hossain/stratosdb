package com.stratosdb.sql.ast;

import java.util.List;

/**
 * CREATE [OR REPLACE] PROCEDURE name(params) AS $$ body $$ LANGUAGE lang.
 * Unlike CreateFunctionStatement, body may contain MULTIPLE statements
 * separated by semicolons - a procedure's real, distinguishing purpose is
 * running a sequence of side-effecting statements (INSERT/UPDATE/DELETE),
 * not computing and returning one scalar value. No RETURNS clause: a
 * procedure has no return value at all (see ExecutorEngine.executeCall's
 * own javadoc for the real, honestly-stated scope this implies).
 */
public record CreateProcedureStatement(String name, List<FunctionParam> params,
                                        String body, String language, boolean orReplace) implements Statement {}
