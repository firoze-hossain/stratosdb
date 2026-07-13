package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

public class StratosDBException extends RuntimeException {
    private final int errorCode;
    
    public StratosDBException(String message) {
        super(message);
        this.errorCode = ErrorCodes.ERROR;
    }
    
    public StratosDBException(String message, int errorCode) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public StratosDBException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = ErrorCodes.ERROR;
    }
    
    public int getErrorCode() {
        return errorCode;
    }
}






