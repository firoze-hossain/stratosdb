package com.stratosdb.sql.ast;

/** DROP POLICY policyName ON tableName - removes a previously-created row-level security policy (see CreatePolicyStatement). */
public record DropPolicyStatement(String policyName, String tableName) implements Statement {}
