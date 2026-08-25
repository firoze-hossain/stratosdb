package com.stratosdb.sql.ast;

import java.util.List;

/**
 * COPY table_name [(col1, col2, ...)] {FROM | TO} {'filepath' | STDIN | STDOUT}
 *   [[WITH] (FORMAT {TEXT|CSV}, DELIMITER 'x', HEADER, NULL 'str')]
 *
 * columns is null when no explicit column list was given (meaning: every
 * column, in the table's own current order - resolved by the executor at
 * execution time, not fixed at parse time, since the table's own column
 * order can itself change via ALTER TABLE between when this statement is
 * parsed and when a prepared/replayed version of it might run).
 *
 * target is the raw destination text: a still-quoted file path string,
 * or the literal "STDIN"/"STDOUT" - the executor decides which based on
 * isStdio, not by re-parsing target's own text.
 *
 * Real, honestly-stated scope: TEXT and CSV formats only, no BINARY -
 * see ExecutorEngine's own COPY implementation for the exact format
 * details this mirrors from real Postgres (tab-delimited with `\N` for
 * NULL and backslash-escaping for TEXT; comma-delimited with
 * double-quote field-quoting for CSV).
 */
public record CopyStatement(
    String tableName,
    List<String> columns,
    boolean isFrom,
    String target,
    boolean isStdio,
    String format,
    String delimiter,
    boolean header,
    String nullString
) implements Statement {}
