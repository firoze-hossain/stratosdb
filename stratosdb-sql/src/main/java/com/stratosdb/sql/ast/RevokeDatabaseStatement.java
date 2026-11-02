package com.stratosdb.sql.ast;

import java.util.List;

/**
 * REVOKE dbPrivilege [, dbPrivilege...] ON DATABASE database_name FROM role_name [, role_name...].
 */
public record RevokeDatabaseStatement(List<String> privileges, String databaseName, List<String> roleNames) implements Statement {
    public RevokeDatabaseStatement(List<String> privileges, String databaseName, String roleName) {
        this(privileges, databaseName, List.of(roleName));
    }

    public String roleName() {
        return roleNames != null && !roleNames.isEmpty() ? roleNames.get(0) : null;
    }
}
