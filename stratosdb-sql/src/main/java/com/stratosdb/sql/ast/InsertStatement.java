package com.stratosdb.sql.ast;

import java.util.List;

/**
 * columns is empty when no explicit column list was given (INSERT INTO t
 * VALUES (...)), meaning values map positionally to the table's own column
 * order - not empty when given, meaning each value maps to its NAMED
 * column, in whatever order the statement specified.
 *
 * rows: one entry per VALUES tuple - a real, standard SQL feature (and one
 * PostgreSQL, MySQL, SQL Server, and SQLite all support) this engine was
 * missing entirely until now: {@code INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6);}
 * inserts three real, separate rows from one statement, not one. A plain,
 * single-row INSERT is simply the one-element case of this same list -
 * there is no separate "single row" representation anymore.
 *
 * returningColumns: the column(s) named by a real RETURNING clause - empty
 * (not null) when none was given. A single entry of "*" means RETURNING *
 * (every column, in the table's own declared order) - see
 * ExecutorEngine.finishInsert for how this is actually honored. Added
 * specifically because Django's own postgresql backend always appends
 * RETURNING to its own generated INSERT, even for an explicitly-supplied
 * primary key value, found missing entirely during a real, broad
 * driver/ORM verification pass - and it's real, standard Postgres syntax
 * many other serious clients/ORMs rely on too, not a Django-specific quirk.
 * With a real multi-row INSERT, RETURNING now yields one real row per
 * inserted row, in insertion order - PostgreSQL's own real behavior for
 * exactly this combination.
 */
public record InsertStatement(String tableName, List<String> columns, List<List<String>> rows, List<String> returningColumns) implements Statement {}