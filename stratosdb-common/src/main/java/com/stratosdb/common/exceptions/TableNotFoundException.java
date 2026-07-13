package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

public class TableNotFoundException extends StratosDBException {
    private final String tableName;
    
    public TableNotFoundException(String tableName) {
        super("Table not found: " + tableName, ErrorCodes.TABLE_NOT_FOUND);
        this.tableName = tableName;
    }
    
    public String getTableName() {
        return tableName;
    }
}