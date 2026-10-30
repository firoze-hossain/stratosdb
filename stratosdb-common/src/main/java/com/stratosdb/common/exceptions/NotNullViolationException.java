package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

/**
 * Standard SQLSTATE 23502 (not_null_violation).
 * Thrown when an INSERT or UPDATE attempts to store a NULL value into a column with a NOT NULL constraint.
 */
public class NotNullViolationException extends StratosDBException {
    public static final String SQLSTATE = "23502";

    private final String tableName;
    private final String columnName;

    public NotNullViolationException(String tableName, String columnName) {
        this(tableName, columnName, "null value in column \"" + columnName + "\" of relation \"" + tableName + "\" violates not-null constraint");
    }

    public NotNullViolationException(String tableName, String columnName, String message) {
        super(message, ErrorCodes.NOT_NULL_VIOLATION);
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public NotNullViolationException(String message) {
        super(message, ErrorCodes.NOT_NULL_VIOLATION);
        this.tableName = null;
        this.columnName = null;
    }

    @Override
    public String getSqlState() {
        return SQLSTATE;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }
}
