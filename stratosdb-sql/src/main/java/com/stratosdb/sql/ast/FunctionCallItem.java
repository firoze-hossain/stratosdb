package com.stratosdb.sql.ast;

import java.util.List;

/**
 * A user-defined function call in a SELECT list: functionName(arg1, arg2, ...).
 * Each arg is either a literal's raw text or a bare column name (resolved
 * against the current row at projection time - see ExecutorEngine).
 * alias: null if no AS was given.
 */
public record FunctionCallItem(String functionName, List<String> args, String alias) {
    /** The name this call's value should be shown/keyed under: the alias if given, else the canonical "name(args)" form, matching AggregateCall's own convention. */
    public String displayName() {
        return alias != null ? alias : canonicalForm();
    }

    public String canonicalForm() {
        return functionName + "(" + String.join(", ", args) + ")";
    }
}
