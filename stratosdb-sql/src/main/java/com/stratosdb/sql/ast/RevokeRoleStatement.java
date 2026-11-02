package com.stratosdb.sql.ast;

import java.util.List;

/**
 * REVOKE [ADMIN OPTION FOR] role_name [, role_name...] FROM role_name [, role_name...].
 */
public record RevokeRoleStatement(List<String> roles, List<String> grantees, boolean adminOptionOnly) implements Statement {
    public RevokeRoleStatement(String role, String grantee) {
        this(List.of(role), List.of(grantee), false);
    }

    public String role() {
        return roles != null && !roles.isEmpty() ? roles.get(0) : null;
    }

    public String grantee() {
        return grantees != null && !grantees.isEmpty() ? grantees.get(0) : null;
    }
}
