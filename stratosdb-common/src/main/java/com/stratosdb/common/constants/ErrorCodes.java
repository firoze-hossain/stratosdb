package com.stratosdb.common.constants;

public class ErrorCodes {
    public static final int SUCCESS = 0;
    public static final int ERROR = 1;
    public static final int TABLE_NOT_FOUND = 100;
    public static final int COLUMN_NOT_FOUND = 101;
    public static final int SYNTAX_ERROR = 102;
    public static final int TRANSACTION_ERROR = 200;
    public static final int DEADLOCK = 201;
    public static final int STORAGE_ERROR = 300;
    public static final int PERMISSION_DENIED = 400;
    public static final int UNIQUE_VIOLATION = 23505;
    public static final int NOT_NULL_VIOLATION = 23502;
    public static final int FOREIGN_KEY_VIOLATION = 23503;
    public static final int CHECK_VIOLATION = 23514;
}