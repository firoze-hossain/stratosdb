package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

public class DeadlockException extends StratosDBException {
    public DeadlockException(String message) {
        super(message, ErrorCodes.DEADLOCK);
    }
}