package com.stratosdb.sql.ast;

/** ALTER TABLE tableName ENABLE ROW LEVEL SECURITY - see CreatePolicyStatement's own javadoc for the full row-level security feature this is part of. */
public record AlterTableEnableRlsStatement(String tableName) implements Statement {}
