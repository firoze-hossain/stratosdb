package com.stratosdb.sql.ast;

import java.util.List;

/**
 * GRANT dbPrivilege [, dbPrivilege...] ON DATABASE database_name TO role_name [, role_name...].
 * Privileges include "CONNECT", "CREATE", "TEMP", "TEMPORARY", "ALL".
 */
public record GrantDatabaseStatement(List<String> privileges, String databaseName, List<String> roleNames) implements Statement {
    public GrantDatabaseStatement(List<String> privileges, String databaseName, String roleName) {
        this(privileges, databaseName, List.of(roleName));
    }

    public String roleName() {
        return roleNames != null && !roleNames.isEmpty() ? roleNames.get(0) : null;
    }
}
