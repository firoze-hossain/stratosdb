package com.stratosdb.sql.ast;

/** ALTER TABLE tableName FORCE ROW LEVEL SECURITY - makes RLS policies apply even to the table's own owner, real Postgres's own real distinction between merely ENABLEing RLS (owner still bypasses it) and FORCEing it (owner is bound by it too, matching everyone else). */
public record AlterTableForceRlsStatement(String tableName) implements Statement {}
