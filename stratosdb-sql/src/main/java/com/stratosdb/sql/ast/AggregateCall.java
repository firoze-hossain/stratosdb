package com.stratosdb.sql.ast;

/**
 * function: COUNT/SUM/AVG/MIN/MAX (uppercase). argument: "*" or a column name.
 * alias: null if no AS was given.
 * distinct: true if DISTINCT was specified, e.g. COUNT(DISTINCT col).
 */
public record AggregateCall(String function, String argument, String alias, boolean distinct) {
    public AggregateCall(String function, String argument, String alias) {
        this(function, argument, alias, false);
    }

    /** The name this aggregate's value should be shown/keyed under: the alias if given, else the canonical "FUNC(arg)" form. */
    public String displayName() {
        return alias != null ? alias : canonicalForm();
    }

    /** The "FUNC(arg)" form - what a HAVING clause references, regardless of any SELECT-list alias. */
    public String canonicalForm() {
        return function + "(" + (distinct ? "DISTINCT " : "") + argument + ")";
    }
}
