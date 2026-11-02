package com.stratosdb.sql.ast;

import java.util.List;

/**
 * REVOKE privilege [, privilege...] ON [TABLE] table_name FROM role_name [, role_name...].
 */
public record RevokeStatement(List<String> privileges, String tableName, List<String> roleNames) implements Statement {
    public RevokeStatement(List<String> privileges, String tableName, String roleName) {
        this(privileges, tableName, List.of(roleName));
    }

    public String roleName() {
        return roleNames != null && !roleNames.isEmpty() ? roleNames.get(0) : null;
    }
}
