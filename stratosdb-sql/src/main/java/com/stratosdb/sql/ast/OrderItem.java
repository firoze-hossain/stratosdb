package com.stratosdb.sql.ast;

public record OrderItem(String column, boolean ascending) {
    public boolean descending() {
        return !ascending;
    }
}
