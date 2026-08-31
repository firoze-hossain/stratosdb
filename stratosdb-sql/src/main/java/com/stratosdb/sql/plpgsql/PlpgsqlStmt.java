package com.stratosdb.sql.plpgsql;

import java.util.List;

/**
 * A real statement inside a procedural block's own BEGIN...END body - the
 * real control flow (If/While/ForRange/Loop/Exit/Continue) this engine
 * previously had none of at all, plus Assignment, Return, Raise, and a
 * real, embedded SQL statement (see PlpgsqlInterpreter's own javadoc for
 * how EmbeddedSql is actually executed).
 */
public sealed interface PlpgsqlStmt {
    record Assignment(String variableName, PlpgsqlExpr value) implements PlpgsqlStmt {}

    /** One IF/ELSIF branch: its own condition and the statements to run when it's true. IF itself is branches.get(0); every subsequent entry is an ELSIF. */
    record IfBranch(PlpgsqlExpr condition, List<PlpgsqlStmt> body) {}
    record If(List<IfBranch> branches, List<PlpgsqlStmt> elseBody) implements PlpgsqlStmt {}

    record While(PlpgsqlExpr condition, List<PlpgsqlStmt> body) implements PlpgsqlStmt {}
    record ForRange(String loopVariable, PlpgsqlExpr from, PlpgsqlExpr to, List<PlpgsqlStmt> body) implements PlpgsqlStmt {}
    record Loop(List<PlpgsqlStmt> body) implements PlpgsqlStmt {}

    /** whenCondition is null for a bare EXIT/CONTINUE (always taken); non-null for EXIT WHEN/CONTINUE WHEN (conditionally taken). */
    record Exit(PlpgsqlExpr whenCondition) implements PlpgsqlStmt {}
    record Continue(PlpgsqlExpr whenCondition) implements PlpgsqlStmt {}

    /** value is null for a bare RETURN (a procedure, or a function returning void); non-null for RETURN <expr>. */
    record Return(PlpgsqlExpr value) implements PlpgsqlStmt {}

    /** level is one of "NOTICE", "EXCEPTION", "WARNING" (defaults to "NOTICE" when omitted - see PlpgsqlParser). "EXCEPTION" genuinely aborts execution, matching real Postgres's own RAISE EXCEPTION; NOTICE/WARNING are informational only. */
    record Raise(String level, String message) implements PlpgsqlStmt {}

    /** sqlText is the real, raw SQL text captured between this statement's own start and its terminating semicolon (semicolon excluded) - see PlpgsqlInterpreter's own javadoc for the real substitution-then-real-parse flow this goes through at execution time, including this engine's own new, real "SELECT ... INTO variable" support. */
    record EmbeddedSql(String sqlText) implements PlpgsqlStmt {}
}
