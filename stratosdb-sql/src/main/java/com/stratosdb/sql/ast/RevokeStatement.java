package com.stratosdb.sql.ast;

import java.util.List;

/** REVOKE privilege [, privilege...] ON [TABLE] table_name FROM role_name. */
public record RevokeStatement(List<String> privileges, String tableName, String roleName) implements Statement {}
