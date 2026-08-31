package com.stratosdb.sql.ast;

import java.util.List;

/**
 * CREATE TYPE typeName AS ENUM ('val1', 'val2', ...) - a real, named enum
 * type, referenced afterward as an ordinary column type (see dataType's
 * own grammar rule for the bare-IDENTIFIER fallback that lets a real
 * user-defined type name be used exactly like any built-in one). This
 * project's own honestly-named "no custom types, no enums" gap.
 */
public record CreateTypeStatement(String typeName, List<String> enumValues) implements Statement {}
