package com.stratosdb.common.exceptions;

import com.stratosdb.common.constants.ErrorCodes;

public class TransactionException extends StratosDBException {
    public TransactionException(String message) {
        super(message, ErrorCodes.TRANSACTION_ERROR);
    }
}
