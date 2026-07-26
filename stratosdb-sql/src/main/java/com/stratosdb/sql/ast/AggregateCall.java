package com.stratosdb.sql.ast;

/**
 * function: COUNT/SUM/AVG/MIN/MAX (uppercase). argument: "*" or a column name.
 * alias: null if no AS was given.
 */
public record AggregateCall(String function, String argument, String alias) {
    /** The name this aggregate's value should be shown/keyed under: the alias if given, else the canonical "FUNC(arg)" form. */
    public String displayName() {
        return alias != null ? alias : canonicalForm();
    }

    /** The "FUNC(arg)" form - what a HAVING clause references, regardless of any SELECT-list alias. */
    public String canonicalForm() {
        return function + "(" + argument + ")";
    }
}
