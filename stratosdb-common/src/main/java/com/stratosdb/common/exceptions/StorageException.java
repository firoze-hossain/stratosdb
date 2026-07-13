package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

public class StorageException extends StratosDBException {
    public StorageException(String message) {
        super(message, ErrorCodes.STORAGE_ERROR);
    }
}