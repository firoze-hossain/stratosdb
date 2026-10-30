package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

/**
 * Standard SQLSTATE 23514 (check_violation).
 * Thrown when an INSERT or UPDATE violates a CHECK constraint expression.
 */
public class CheckViolationException extends StratosDBException {
    public static final String SQLSTATE = "23514";

    private final String tableName;
    private final String constraintName;
    private final String expression;

    public CheckViolationException(String tableName, String constraintName, String expression) {
        this(tableName, constraintName, expression,
             "new row for relation \"" + tableName + "\" violates check constraint \"" + (constraintName != null ? constraintName : "check_constraint") + "\"");
    }

    public CheckViolationException(String tableName, String constraintName, String expression, String message) {
        super(message, ErrorCodes.CHECK_VIOLATION);
        this.tableName = tableName;
        this.constraintName = constraintName;
        this.expression = expression;
    }

    public CheckViolationException(String message) {
        super(message, ErrorCodes.CHECK_VIOLATION);
        this.tableName = null;
        this.constraintName = null;
        this.expression = null;
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

    public String getExpression() {
        return expression;
    }
}
