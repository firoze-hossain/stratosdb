package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

/**
 * Standard SQLSTATE 23505 (unique_violation).
 * Thrown when an INSERT or UPDATE statement violates a PRIMARY KEY or UNIQUE constraint,
 * or when a duplicate key is inserted into a unique index.
 */
public class UniqueConstraintViolationException extends StratosDBException {
    public static final String SQLSTATE = "23505";

    private final String tableName;
    private final String constraintName;
    private final Object violatingKey;

    public UniqueConstraintViolationException(String message) {
        super(message, ErrorCodes.UNIQUE_VIOLATION);
        this.tableName = null;
        this.constraintName = null;
        this.violatingKey = null;
    }

    public UniqueConstraintViolationException(String tableName, String constraintName, Object violatingKey) {
        super(formatMessage(tableName, constraintName, violatingKey), ErrorCodes.UNIQUE_VIOLATION);
        this.tableName = tableName;
        this.constraintName = constraintName;
        this.violatingKey = violatingKey;
    }

    public UniqueConstraintViolationException(String tableName, String constraintName, Object violatingKey, String message) {
        super(message, ErrorCodes.UNIQUE_VIOLATION);
        this.tableName = tableName;
        this.constraintName = constraintName;
        this.violatingKey = violatingKey;
    }

    private static String formatMessage(String tableName, String constraintName, Object violatingKey) {
        String cName = constraintName != null ? constraintName : "unique_constraint";
        String keyStr = violatingKey != null ? violatingKey.toString() : "null";
        return "duplicate key value violates unique constraint \"" + cName + "\": Key (" + keyStr + ") already exists.";
    }

    @Override
    public String getSqlState() {
        return SQLSTATE;
    }

    public String getTableName() {
        return tableName;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public Object getViolatingKey() {
        return violatingKey;
    }
}
