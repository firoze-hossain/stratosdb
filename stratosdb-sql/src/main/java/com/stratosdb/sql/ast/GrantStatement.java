package com.stratosdb.sql.ast;

import java.util.List;

/** GRANT privilege [, privilege...] ON [TABLE] table_name TO role_name. Each privilege is one of "SELECT"/"INSERT"/"UPDATE"/"DELETE"/"ALL" (ALL expanded to the other four at grant time - see ExecutorEngine.executeGrant). */
public record GrantStatement(List<String> privileges, String tableName, String roleName) implements Statement {}
