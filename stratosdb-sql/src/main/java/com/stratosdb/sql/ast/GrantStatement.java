package com.stratosdb.sql.ast;

import java.util.List;

/**
 * GRANT privilege [, privilege...] ON [TABLE] table_name TO role_name [, role_name...].
 * Each privilege is one of "SELECT"/"INSERT"/"UPDATE"/"DELETE"/"ALL" (ALL expanded to the other four at grant time - see ExecutorEngine.executeGrant).
 */
public record GrantStatement(List<String> privileges, String tableName, List<String> roleNames) implements Statement {
    public GrantStatement(List<String> privileges, String tableName, String roleName) {
        this(privileges, tableName, List.of(roleName));
    }

    public String roleName() {
        return roleNames != null && !roleNames.isEmpty() ? roleNames.get(0) : null;
    }
}
