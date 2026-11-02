package com.stratosdb.sql.ast;

import java.util.List;

/**
 * GRANT role_name [, role_name...] TO role_name [, role_name...] [WITH ADMIN OPTION].
 */
public record GrantRoleStatement(List<String> roles, List<String> grantees, boolean withAdminOption) implements Statement {
    public GrantRoleStatement(String role, String grantee) {
        this(List.of(role), List.of(grantee), false);
    }

    public String role() {
        return roles != null && !roles.isEmpty() ? roles.get(0) : null;
    }

    public String grantee() {
        return grantees != null && !grantees.isEmpty() ? grantees.get(0) : null;
    }
}
