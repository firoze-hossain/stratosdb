package com.stratosdb.sql.ast;

/**
 * CREATE ROLE name [WITH] [LOGIN|NOLOGIN] [SUPERUSER|NOSUPERUSER] [PASSWORD 'x'].
 *
 * login/superuser default false (NOLOGIN/NOSUPERUSER), matching real
 * Postgres's own defaults for a bare CREATE ROLE with no attributes.
 * password is the raw, still-quoted SQL text (or null) - resolved by the
 * executor, which also bridges it to a real, authenticatable credential
 * via RoleCredentialSink (see ExecutorEngine's own javadoc for that
 * interface) rather than only being tracked for show.
 */
public record CreateRoleStatement(String roleName, boolean login, boolean superuser, String password) implements Statement {}
