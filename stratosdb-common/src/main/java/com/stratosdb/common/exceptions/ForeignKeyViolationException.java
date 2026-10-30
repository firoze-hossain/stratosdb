package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

/**
 * Standard SQLSTATE 23503 (foreign_key_violation).
 * Thrown when an INSERT, UPDATE, or DELETE statement violates a FOREIGN KEY constraint.
 */
public class ForeignKeyViolationException extends StratosDBException {
    public static final String SQLSTATE = "23503";

    private final String tableName;
    private final String constraintName;
    private final Object violatingKey;

    public ForeignKeyViolationException(String message) {
        super(message, ErrorCodes.FOREIGN_KEY_VIOLATION);
        this.tableName = null;
        this.constraintName = null;
        this.violatingKey = null;
    }

    public ForeignKeyViolationException(String tableName, String constraintName, String message) {
        this(tableName, constraintName, null, message);
    }

    public ForeignKeyViolationException(String tableName, String constraintName, Object violatingKey, String message) {
        super(message, ErrorCodes.FOREIGN_KEY_VIOLATION);
        this.tableName = tableName;
        this.constraintName = constraintName;
        this.violatingKey = violatingKey;
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
