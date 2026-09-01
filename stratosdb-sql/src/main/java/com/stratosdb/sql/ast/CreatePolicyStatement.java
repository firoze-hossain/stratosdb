package com.stratosdb.sql.ast;

/**
 * CREATE POLICY policyName ON tableName [FOR SELECT|INSERT|UPDATE|DELETE|ALL]
 * [TO roleName] USING (expr) [WITH CHECK (expr)] - real row-level security,
 * this project's own previously entirely-missing gap. A policy restricts
 * which real rows a given command may see (USING) or write (WITH CHECK) on
 * a table that has had RLS ENABLEd (see AlterTableEnableRlsStatement).
 *
 * command is one of "SELECT"/"INSERT"/"UPDATE"/"DELETE"/"ALL" (real
 * Postgres's own default when FOR is omitted). roleName is null when TO
 * was omitted (real Postgres's own implicit PUBLIC - the policy applies
 * to every role). withCheckExpr is null when WITH CHECK was omitted -
 * real Postgres's own real rule (reused by ExecutorEngine's own RLS
 * enforcement) is that USING is reused as the check for UPDATE when WITH
 * CHECK is absent, and INSERT requires an explicit WITH CHECK to restrict
 * newly-written rows at all (USING alone has no rows yet to filter for a
 * fresh INSERT).
 *
 * usingExpr/withCheckExpr reuse this engine's own existing WhereExpr AST -
 * a policy's own predicate is structurally identical to an ordinary WHERE
 * clause predicate, so this lets ExecutorEngine reuse its own existing,
 * proven evaluateWhereExpr logic directly, rather than a second, separate
 * expression evaluator just for policies.
 */
public record CreatePolicyStatement(String policyName, String tableName, String command,
                                     String roleName, WhereExpr usingExpr, WhereExpr withCheckExpr) implements Statement {}
