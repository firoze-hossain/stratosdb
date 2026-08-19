package com.stratosdb.sql.ast;

/**
 * CREATE TRIGGER name {BEFORE|AFTER} {INSERT|UPDATE|DELETE} ON table
 * FOR EACH ROW EXECUTE {FUNCTION|PROCEDURE} handlerName().
 *
 * A real, deliberately scoped-down implementation - see
 * ExecutorEngine.fireTriggers' own javadoc for the honest scope
 * statement, including why this engine allows a PROCEDURE as a trigger
 * handler (real Postgres requires a special RETURNS TRIGGER function),
 * and why a BEFORE trigger here cannot modify the row or cancel the
 * operation the way real Postgres's own BEFORE triggers can (this
 * engine's stored functions/procedures have no return-value mechanism
 * for a trigger to use for that).
 *
 * timing is "BEFORE" or "AFTER"; event is "INSERT", "UPDATE", or
 * "DELETE"; isFunction distinguishes EXECUTE FUNCTION from EXECUTE
 * PROCEDURE, since the handler name is looked up in a different
 * registry depending on which keyword was used.
 */
public record CreateTriggerStatement(String name, String timing, String event,
                                      String tableName, String handlerName, boolean isFunction) implements Statement {}
