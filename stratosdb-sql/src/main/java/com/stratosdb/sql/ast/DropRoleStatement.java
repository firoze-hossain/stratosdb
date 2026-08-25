package com.stratosdb.sql.ast;

public record DropRoleStatement(String roleName) implements Statement {}
