package com.stratosdb.sql.parser;

import com.stratosdb.sql.ast.*;
import com.stratosdb.transaction.IsolationLevel;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SqlParser {
    private static final Logger LOG = LoggerFactory.getLogger(SqlParser.class);

    /**
     * True if sql has no real tokens once whitespace and comments are
     * stripped - e.g. a line that's entirely a `--` comment, or a
     * `/* *\/`-only block, or pure whitespace. Uses the real lexer
     * (not a separate, potentially-diverging regex) specifically so a
     * string literal that happens to contain "--" or "/*" is never
     * mistaken for an actual comment the way a naive text-based check
     * could be - the lexer already knows the difference correctly.
     *
     * Exists for exactly one real reason: a comment-only line (like the
     * header lines stratosdump's own generated output starts with) is
     * not a syntax error to report - it's an empty query, the same as
     * real Postgres treats it, and StdWireServer uses this to decide
     * that rather than attempting to parse it and surfacing a confusing
     * "mismatched input EOF" error for what is, in fact, valid SQL.
     */
    public boolean isEffectivelyEmpty(String sql) {
        CharStream charStream = CharStreams.fromString(sql);
        StratosSQLLexer lexer = new StratosSQLLexer(charStream);
        lexer.removeErrorListeners(); // a lexer error here (e.g. an unterminated string) means real content exists - not this method's job to report
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();
        return tokens.size() == 1 && tokens.get(0).getType() == Token.EOF;
    }

    public Statement parse(String sql) {
        LOG.debug("Parsing: {}", sql);

        CharStream charStream = CharStreams.fromString(sql);
        StratosSQLLexer lexer = new StratosSQLLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        StratosSQLParser parser = new StratosSQLParser(tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                    int line, int charPositionInLine, String msg,
                                    RecognitionException e) {
                throw new RuntimeException("Syntax error at line " + line + ":" + charPositionInLine + " - " + msg);
            }
        });

        StratosSQLParser.ParseContext parseContext = parser.parse();
        return buildStatement(parseContext.sqlStatement());
    }

    private Statement buildStatement(StratosSQLParser.SqlStatementContext ctx) {
        if (ctx.createTable() != null) {
            return buildCreateTable(ctx.createTable());
        } else if (ctx.createIndex() != null) {
            return buildCreateIndex(ctx.createIndex());
        } else if (ctx.insert() != null) {
            return buildInsert(ctx.insert());
        } else if (ctx.select() != null) {
            return buildSelect(ctx.select());
        } else if (ctx.update() != null) {
            return buildUpdate(ctx.update());
        } else if (ctx.delete() != null) {
            return buildDelete(ctx.delete());
        } else if (ctx.dropTable() != null) {
            return buildDropTable(ctx.dropTable());
        } else if (ctx.copyStatement() != null) {
            return buildCopyStatement(ctx.copyStatement());
        } else if (ctx.createRole() != null) {
            return buildCreateRole(ctx.createRole());
        } else if (ctx.dropRole() != null) {
            return new DropRoleStatement(ctx.dropRole().roleName().getText());
        } else if (ctx.grantStatement() != null) {
            return buildGrantStatement(ctx.grantStatement());
        } else if (ctx.revokeStatement() != null) {
            return buildRevokeStatement(ctx.revokeStatement());
        } else if (ctx.alterTableAddColumn() != null) {
            StratosSQLParser.AlterTableAddColumnContext actx = ctx.alterTableAddColumn();
            String defaultVal = actx.defaultValue() != null ? actx.defaultValue().getText() : null;
            return new AlterTableAddColumnStatement(parseTableName(actx.tableName()), cleanColumnName(actx.columnName().getText()), actx.dataType().getText(), defaultVal);
        } else if (ctx.alterTableDropColumn() != null) {
            StratosSQLParser.AlterTableDropColumnContext actx = ctx.alterTableDropColumn();
            return new AlterTableDropColumnStatement(parseTableName(actx.tableName()), cleanColumnName(actx.columnName().getText()));
        } else if (ctx.alterTableRenameColumn() != null) {
            StratosSQLParser.AlterTableRenameColumnContext actx = ctx.alterTableRenameColumn();
            return new AlterTableRenameColumnStatement(parseTableName(actx.tableName()), cleanColumnName(actx.columnName(0).getText()), cleanColumnName(actx.columnName(1).getText()));
        } else if (ctx.alterTableRenameTable() != null) {
            StratosSQLParser.AlterTableRenameTableContext actx = ctx.alterTableRenameTable();
            return new AlterTableRenameTableStatement(parseTableName(actx.tableName(0)), parseTableName(actx.tableName(1)));
        } else if (ctx.alterTableAlterColumnType() != null) {
            StratosSQLParser.AlterTableAlterColumnTypeContext actx = ctx.alterTableAlterColumnType();
            return new AlterTableAlterColumnTypeStatement(parseTableName(actx.tableName()), cleanColumnName(actx.columnName().getText()), actx.dataType().getText());
        } else if (ctx.alterTableSetDefault() != null) {
            StratosSQLParser.AlterTableSetDefaultContext actx = ctx.alterTableSetDefault();
            return new AlterTableSetDefaultStatement(parseTableName(actx.tableName()), cleanColumnName(actx.columnName().getText()), actx.defaultValue().getText());
        } else if (ctx.alterTableDropDefault() != null) {
            StratosSQLParser.AlterTableDropDefaultContext actx = ctx.alterTableDropDefault();
            return new AlterTableDropDefaultStatement(parseTableName(actx.tableName()), cleanColumnName(actx.columnName().getText()));
        } else if (ctx.alterTableAddConstraint() != null) {
            return buildAlterTableAddConstraint(ctx.alterTableAddConstraint());
        } else if (ctx.alterTableDropConstraint() != null) {
            return buildAlterTableDropConstraint(ctx.alterTableDropConstraint());
        } else if (ctx.dropIndex() != null) {
            return new DropIndexStatement(cleanIdentifier(ctx.dropIndex().indexName().getText()), ctx.dropIndex().PLSQL_IF() != null);
        } else if (ctx.alterTableEnableRls() != null) {
            return new AlterTableEnableRlsStatement(parseTableName(ctx.alterTableEnableRls().tableName()));
        } else if (ctx.alterTableDisableRls() != null) {
            return new AlterTableDisableRlsStatement(parseTableName(ctx.alterTableDisableRls().tableName()));
        } else if (ctx.alterTableForceRls() != null) {
            return new AlterTableForceRlsStatement(parseTableName(ctx.alterTableForceRls().tableName()));
        } else if (ctx.createPolicy() != null) {
            StratosSQLParser.CreatePolicyContext pctx = ctx.createPolicy();
            String command = pctx.policyCommand() != null ? pctx.policyCommand().getText().toUpperCase() : "ALL";
            String roleName = pctx.roleName() != null ? pctx.roleName().getText() : null;
            WhereExpr usingExpr = buildWhereExpr(pctx.expression(0));
            WhereExpr withCheckExpr = pctx.expression().size() > 1 ? buildWhereExpr(pctx.expression(1)) : null;
            return new CreatePolicyStatement(pctx.policyName().getText(), pctx.tableName().getText(), command, roleName, usingExpr, withCheckExpr);
        } else if (ctx.dropPolicy() != null) {
            StratosSQLParser.DropPolicyContext pctx = ctx.dropPolicy();
            return new DropPolicyStatement(pctx.policyName().getText(), pctx.tableName().getText());
        } else if (ctx.showTables() != null) {
            return new ShowTablesStatement();
        } else if (ctx.showStats() != null) {
            return new ShowStatsStatement();
        } else if (ctx.showTableStats() != null) {
            return new ShowTableStatsStatement();
        } else if (ctx.showStatements() != null) {
            return new ShowStatementsStatement();
        } else if (ctx.showActivity() != null) {
            return new ShowActivityStatement();
        } else if (ctx.showTransactionIsolationLevel() != null) {
            return new ShowTransactionIsolationLevelStatement();
        } else if (ctx.showParameter() != null) {
            return new ShowParameterStatement(ctx.showParameter().IDENTIFIER().getText());
        } else if (ctx.setParameter() != null) {
            // ctx.setParameter().IDENTIFIER() returns BOTH the parameter name and,
            // when the value itself is a bare identifier rather than a literal
            // (e.g. SET search_path = public), that value too - the parameter
            // name is always the FIRST one; a literal value is read separately
            // via ctx.setParameter().literal() when present.
            String paramName = ctx.setParameter().IDENTIFIER(0).getText();
            String value = ctx.setParameter().literal() != null
                ? ctx.setParameter().literal().getText()
                : ctx.setParameter().IDENTIFIER(1).getText();
            return new SetParameterStatement(paramName, value);
        } else if (ctx.showCatalog() != null) {
            return new ShowCatalogStatement();
        } else if (ctx.explain() != null) {
            return new ExplainStatement(buildSelect(ctx.explain().select()));
        } else if (ctx.analyze() != null) {
            String tbl = ctx.analyze().tableName() != null ? ctx.analyze().tableName().getText() : null;
            return new AnalyzeStatement(tbl);
        } else if (ctx.vacuum() != null) {
            return new VacuumStatement(ctx.vacuum().tableName().getText());
        } else if (ctx.reindexStatement() != null) {
            return buildReindex(ctx.reindexStatement());
        } else if (ctx.checkpointStatement() != null) {
            return new CheckpointStatement();
        } else if (ctx.promoteStatement() != null) {
            return new PromoteStatement();
        } else if (ctx.createDatabase() != null) {
            return new CreateDatabaseStatement(ctx.createDatabase().databaseName().getText());
        } else if (ctx.dropDatabase() != null) {
            return new DropDatabaseStatement(ctx.dropDatabase().databaseName().getText());
        } else if (ctx.showDatabases() != null) {
            return new ShowDatabasesStatement();
        } else if (ctx.beginTxn() != null) {
            IsolationLevel level = null;
            if (ctx.beginTxn().isolationLevel() != null && !ctx.beginTxn().isolationLevel().isEmpty()) {
                level = parseIsolationLevel(ctx.beginTxn().isolationLevel(0));
            }
            return new BeginStatement(level);
        } else if (ctx.setTransaction() != null) {
            IsolationLevel level = (ctx.setTransaction().isolationLevel() != null && !ctx.setTransaction().isolationLevel().isEmpty())
                ? parseIsolationLevel(ctx.setTransaction().isolationLevel(0))
                : null;
            boolean isLocal = ctx.setTransaction().LOCAL() != null;
            return new SetTransactionIsolationStatement(level, isLocal);
        } else if (ctx.setSessionCharacteristics() != null) {
            IsolationLevel level = (ctx.setSessionCharacteristics().isolationLevel() != null && !ctx.setSessionCharacteristics().isolationLevel().isEmpty())
                ? parseIsolationLevel(ctx.setSessionCharacteristics().isolationLevel(0))
                : null;
            return new SetSessionCharacteristicsStatement(level);
        } else if (ctx.commitTxn() != null) {
            return new CommitStatement();
        } else if (ctx.rollbackTxn() != null) {
            return new RollbackStatement();
        } else if (ctx.createView() != null) {
            return new CreateViewStatement(ctx.createView().viewName().getText(), buildSelect(ctx.createView().select()));
        } else if (ctx.dropView() != null) {
            return new DropViewStatement(ctx.dropView().viewName().getText());
        } else if (ctx.savepoint() != null) {
            return new SavepointStatement(cleanIdentifier(ctx.savepoint().savepointName().getText()));
        } else if (ctx.releaseSavepoint() != null) {
            return new ReleaseSavepointStatement(cleanIdentifier(ctx.releaseSavepoint().savepointName().getText()));
        } else if (ctx.rollbackToSavepoint() != null) {
            return new RollbackToSavepointStatement(cleanIdentifier(ctx.rollbackToSavepoint().savepointName().getText()));
        } else if (ctx.selectWithCte() != null) {
            StratosSQLParser.SelectWithCteContext cteCtx = ctx.selectWithCte();
            String cteName = cleanIdentifier(cteCtx.cteName().getText());
            boolean hasRecursiveKeyword = cteCtx.RECURSIVE() != null;
            boolean hasUnionAllStructure = cteCtx.select().size() == 3;
            if (hasRecursiveKeyword != hasUnionAllStructure) {
                throw new IllegalArgumentException("WITH RECURSIVE requires a \"base UNION ALL recursive\" structure inside the parentheses, and vice versa - got RECURSIVE=" + hasRecursiveKeyword + " but a UNION ALL structure=" + hasUnionAllStructure);
            }
            if (hasRecursiveKeyword) {
                SelectStatement baseQuery = buildSelect(cteCtx.select(0));
                SelectStatement recursiveQuery = buildSelect(cteCtx.select(1));
                SelectStatement outerQuery = buildSelect(cteCtx.select(2));
                return new RecursiveCteSelectStatement(cteName, baseQuery, recursiveQuery, outerQuery);
            }
            SelectStatement cteQuery = buildSelect(cteCtx.select(0));
            SelectStatement outerQuery = buildSelect(cteCtx.select(1));
            return new CteSelectStatement(cteName, cteQuery, outerQuery);
        } else if (ctx.createSequence() != null) {
            return buildCreateSequence(ctx.createSequence());
        } else if (ctx.alterSequence() != null) {
            return buildAlterSequence(ctx.alterSequence());
        } else if (ctx.dropSequence() != null) {
            return new DropSequenceStatement(cleanSequenceName(ctx.dropSequence().sequenceName().getText()), ctx.dropSequence().PLSQL_IF() != null);
        } else if (ctx.createType() != null) {
            String typeName = ctx.createType().typeName().getText();
            List<String> enumValues = new ArrayList<>();
            for (var literal : ctx.createType().STRING_LITERAL()) {
                enumValues.add(unquoteStringLiteral(literal.getText()));
            }
            return new CreateTypeStatement(typeName, enumValues);
        } else if (ctx.dropType() != null) {
            return new DropTypeStatement(ctx.dropType().typeName().getText());
        } else if (ctx.createFunction() != null) {
            return buildCreateFunction(ctx.createFunction());
        } else if (ctx.dropFunction() != null) {
            return new DropFunctionStatement(stripSchemaQualifier(ctx.dropFunction().functionName().getText()));
        } else if (ctx.createProcedure() != null) {
            return buildCreateProcedure(ctx.createProcedure());
        } else if (ctx.dropProcedure() != null) {
            return new DropProcedureStatement(ctx.dropProcedure().procedureName().getText());
        } else if (ctx.callStatement() != null) {
            return buildCallStatement(ctx.callStatement());
        } else if (ctx.createTrigger() != null) {
            return buildCreateTrigger(ctx.createTrigger());
        } else if (ctx.dropTrigger() != null) {
            return new DropTriggerStatement(ctx.dropTrigger().triggerName().getText(), ctx.dropTrigger().tableName().getText());
        } else if (ctx.createExtension() != null) {
            StratosSQLParser.CreateExtensionContext extCtx = ctx.createExtension();
            String name = extCtx.extensionName().getText();
            String rawPath = extCtx.STRING_LITERAL().getText();
            return new CreateExtensionStatement(name, rawPath);
        } else if (ctx.dropExtension() != null) {
            return new DropExtensionStatement(ctx.dropExtension().extensionName().getText());
        } else if (ctx.createNativeFunction() != null) {
            return buildCreateNativeFunction(ctx.createNativeFunction());
        }
        throw new IllegalArgumentException("Unsupported SQL statement");
    }

    /**
     * Strips an optional schema qualifier (e.g. "pg_catalog.version" -> "version")
     * from a function name - this engine has no real schema/namespace concept at
     * all, so a qualified reference is treated the same way a schema-less engine
     * reasonably would: the qualifier is accepted syntactically (so real clients
     * that always write "pg_catalog.version()" - virtually every serious Postgres
     * driver/ORM does, for at least server-version detection - don't hit a syntax
     * error), but only the actual function name after the last dot is ever looked
     * up.
     */
    /** Strips a real STRING_LITERAL token's own surrounding single quotes and un-escapes a doubled '' into a real, single ' - the standard SQL escaping rule, the same one ExecutorEngine's own unescapeStringLiteral already applies to a value at execution time; this is purely a PARSE-time convenience for a statement (like CREATE TYPE's own enum value list) whose literal values are needed directly in the AST itself, not resolved later against a row. */
    private static String unquoteStringLiteral(String stringLiteralText) {
        String inner = stringLiteralText.substring(1, stringLiteralText.length() - 1);
        return inner.replace("''", "'");
    }

    private static String stripSchemaQualifier(String possiblyQualifiedName) {
        int lastDot = possiblyQualifiedName.lastIndexOf('.');
        return lastDot >= 0 ? possiblyQualifiedName.substring(lastDot + 1) : possiblyQualifiedName;
    }

    private CreateFunctionStatement buildCreateFunction(StratosSQLParser.CreateFunctionContext ctx) {
        String name = stripSchemaQualifier(ctx.functionName().getText());
        List<FunctionParam> params = new ArrayList<>();
        for (StratosSQLParser.FunctionParamContext paramCtx : ctx.functionParam()) {
            params.add(new FunctionParam(paramCtx.IDENTIFIER().getText(), paramCtx.dataType().getText()));
        }
        String returnType = ctx.dataType().getText();
        // Strip the $$ ... $$ delimiters - DOLLAR_QUOTED_STRING's own raw text
        // includes them (ANTLR gives back the exact matched text for a token),
        // but the stored body should be just the SQL inside.
        String rawDollarQuoted = ctx.DOLLAR_QUOTED_STRING().getText();
        String body = rawDollarQuoted.substring(2, rawDollarQuoted.length() - 2).trim();
        String language = ctx.SQL_LANG() != null ? "SQL" : ctx.IDENTIFIER().getText();
        boolean orReplace = ctx.REPLACE() != null;
        return new CreateFunctionStatement(name, params, returnType, body, language, orReplace);
    }

    /** extensionName is a bare identifier looked up in ExecutorEngine's own extension registry (see CreateNativeFunctionStatement's own javadoc for why this differs from real Postgres's own two-string-literal 'obj_file', 'symbol' convention). nativeSymbol's raw STRING_LITERAL text is kept quoted, unquoted later by the executor's own parseLiteral, matching this project's established convention. */
    private CreateNativeFunctionStatement buildCreateNativeFunction(StratosSQLParser.CreateNativeFunctionContext ctx) {
        String name = stripSchemaQualifier(ctx.functionName().getText());
        List<FunctionParam> params = new ArrayList<>();
        for (StratosSQLParser.FunctionParamContext paramCtx : ctx.functionParam()) {
            params.add(new FunctionParam(paramCtx.IDENTIFIER().getText(), paramCtx.dataType().getText()));
        }
        String returnType = ctx.dataType().getText();
        String extensionName = ctx.extensionName().getText();
        String rawSymbol = ctx.STRING_LITERAL().getText();
        boolean orReplace = ctx.REPLACE() != null;
        return new CreateNativeFunctionStatement(name, params, returnType, extensionName, rawSymbol, orReplace);
    }

    private CreateProcedureStatement buildCreateProcedure(StratosSQLParser.CreateProcedureContext ctx) {
        String name = ctx.procedureName().getText();
        List<FunctionParam> params = new ArrayList<>();
        for (StratosSQLParser.FunctionParamContext paramCtx : ctx.functionParam()) {
            params.add(new FunctionParam(paramCtx.IDENTIFIER().getText(), paramCtx.dataType().getText()));
        }
        String rawDollarQuoted = ctx.DOLLAR_QUOTED_STRING().getText();
        String body = rawDollarQuoted.substring(2, rawDollarQuoted.length() - 2).trim();
        String language = ctx.SQL_LANG() != null ? "SQL" : ctx.IDENTIFIER().getText();
        boolean orReplace = ctx.REPLACE() != null;
        return new CreateProcedureStatement(name, params, body, language, orReplace);
    }

    private CallStatement buildCallStatement(StratosSQLParser.CallStatementContext ctx) {
        String name = ctx.procedureName().getText();
        List<String> args = new ArrayList<>();
        for (StratosSQLParser.FunctionArgContext argCtx : ctx.functionArg()) {
            args.add(argCtx.getText());
        }
        return new CallStatement(name, args);
    }

    private CreateTriggerStatement buildCreateTrigger(StratosSQLParser.CreateTriggerContext ctx) {
        String name = ctx.triggerName().getText();
        String timing = ctx.BEFORE() != null ? "BEFORE" : "AFTER";
        String event = ctx.INSERT() != null ? "INSERT" : ctx.UPDATE() != null ? "UPDATE" : "DELETE";
        String tableName = ctx.tableName().getText();
        String handlerName = ctx.triggerHandlerName().getText();
        boolean isFunction = ctx.FUNCTION() != null;
        return new CreateTriggerStatement(name, timing, event, tableName, handlerName, isFunction);
    }

    private CreateIndexStatement buildCreateIndex(StratosSQLParser.CreateIndexContext ctx) {
        String indexName = cleanIdentifier(ctx.indexName().getText());
        String tableName = parseTableName(ctx.tableName());
        String columnName = cleanColumnName(ctx.columnName(0).getText());
        String columnName2 = ctx.columnName().size() > 1 ? cleanColumnName(ctx.columnName(1).getText()) : null;
        boolean isUnique = ctx.UNIQUE() != null;
        boolean ifNotExists = ctx.PLSQL_IF() != null;
        CreateIndexStatement.IndexType indexType;
        if (ctx.HASH() != null) indexType = CreateIndexStatement.IndexType.HASH;
        else if (ctx.BRIN() != null) indexType = CreateIndexStatement.IndexType.BRIN;
        else if (ctx.GIN() != null) indexType = CreateIndexStatement.IndexType.GIN;
        else if (ctx.BITMAP() != null) indexType = CreateIndexStatement.IndexType.BITMAP;
        else if (ctx.GIST() != null) indexType = CreateIndexStatement.IndexType.GIST;
        else indexType = CreateIndexStatement.IndexType.BTREE; // default, matching Postgres's own convention
        return new CreateIndexStatement(indexName, tableName, columnName, columnName2, indexType, isUnique, ifNotExists);
    }

    /**
     * The grammar has always had a proper `update` rule (UPDATE ... SET ...
     * WHERE ...), but this method never existed and buildStatement() never
     * checked for it - so any UPDATE statement fell through every branch
     * above and hit the IllegalArgumentException at the bottom, regardless
     * of what the executor could or couldn't do with it.
     */
    private UpdateStatement buildUpdate(StratosSQLParser.UpdateContext ctx) {
        String tableName = parseTableName(ctx.tableName());
        List<Assignment> assignments = new ArrayList<>();
        for (StratosSQLParser.AssignmentContext assignCtx : ctx.assignment()) {
            String column = cleanColumnName(assignCtx.columnName().getText());
            String value = assignCtx.literal().getText();
            assignments.add(new Assignment(column, value));
        }
        WhereExpr where = buildWhereExpr(ctx.expression());
        return new UpdateStatement(tableName, assignments, where);
    }

    private CreateTableStatement buildCreateTable(StratosSQLParser.CreateTableContext ctx) {
        String tableName = parseTableName(ctx.tableName());
        boolean ifNotExists = ctx.PLSQL_IF() != null;
        List<ColumnDefinition> columns = new ArrayList<>();
        List<ForeignKeyDefinition> foreignKeys = new ArrayList<>();
        List<CheckConstraintDefinition> checkConstraints = new ArrayList<>();

        for (StratosSQLParser.ColumnDefContext colCtx : ctx.columnDef()) {
            String name = cleanColumnName(colCtx.columnName().getText());
            String type = colCtx.dataType().getText();
            boolean notNull = false;
            String defaultValue = null;
            boolean primaryKey = false;
            boolean isUnique = false;

            if (colCtx.columnConstraint() != null) {
                for (StratosSQLParser.ColumnConstraintContext cCtx : colCtx.columnConstraint()) {
                    if (cCtx.NOT() != null) {
                        notNull = true;
                    }
                    if (cCtx.PRIMARY() != null) {
                        primaryKey = true;
                        notNull = true;
                    }
                    if (cCtx.UNIQUE() != null) {
                        isUnique = true;
                    }
                    if (cCtx.DEFAULT() != null && cCtx.defaultValue() != null) {
                        defaultValue = cCtx.defaultValue().getText();
                    }
                    if (cCtx.REFERENCES() != null) {
                        String fkName = cCtx.constraintName() != null ? cleanIdentifier(cCtx.constraintName().getText()) : tableName + "_" + name + "_fkey";
                        String parentTable = parseTableName(cCtx.tableName());
                        List<String> parentCols = cCtx.columnName() != null ? List.of(cleanColumnName(cCtx.columnName().getText())) : List.of();
                        ForeignKeyDefinition.ActionType onDelete = ForeignKeyDefinition.ActionType.RESTRICT;
                        ForeignKeyDefinition.ActionType onUpdate = ForeignKeyDefinition.ActionType.RESTRICT;
                        for (StratosSQLParser.FkActionContext actCtx : cCtx.fkAction()) {
                            if (actCtx.DELETE() != null) {
                                onDelete = parseFkActionType(actCtx.fkActionType());
                            } else if (actCtx.UPDATE() != null) {
                                onUpdate = parseFkActionType(actCtx.fkActionType());
                            }
                        }
                        foreignKeys.add(new ForeignKeyDefinition(fkName, tableName, List.of(name), parentTable, parentCols, onDelete, onUpdate));
                    }
                    if (cCtx.CHECK() != null && cCtx.expression() != null) {
                        String chkName = cCtx.constraintName() != null ? cleanIdentifier(cCtx.constraintName().getText()) : tableName + "_" + name + "_check";
                        WhereExpr expr = buildWhereExpr(cCtx.expression());
                        String exprSql = cCtx.expression().getText();
                        checkConstraints.add(new CheckConstraintDefinition(chkName, exprSql, expr));
                    }
                }
            }
            columns.add(new ColumnDefinition(name, type, notNull, defaultValue, primaryKey, isUnique));
        }

        List<String> primaryKeyColumns = new ArrayList<>();
        List<CreateTableStatement.TableConstraint> tableConstraints = new ArrayList<>();

        if (ctx.tableConstraint() != null) {
            for (StratosSQLParser.TableConstraintContext tcCtx : ctx.tableConstraint()) {
                String cName = tcCtx.constraintName() != null ? cleanIdentifier(tcCtx.constraintName().getText()) : null;
                if (tcCtx.PRIMARY() != null) {
                    List<String> cols = extractColumns(tcCtx.columnNameList(0));
                    primaryKeyColumns.addAll(cols);
                    tableConstraints.add(new CreateTableStatement.TableConstraint(cName, "PRIMARY KEY", cols));
                } else if (tcCtx.UNIQUE() != null) {
                    List<String> cols = extractColumns(tcCtx.columnNameList(0));
                    tableConstraints.add(new CreateTableStatement.TableConstraint(cName, "UNIQUE", cols));
                } else if (tcCtx.FOREIGN() != null) {
                    List<String> childCols = extractColumns(tcCtx.childCols);
                    String fkName = cName != null ? cName : tableName + "_" + String.join("_", childCols) + "_fkey";
                    String parentTable = parseTableName(tcCtx.tableName());
                    List<String> parentCols = tcCtx.parentCols != null ? extractColumns(tcCtx.parentCols) : List.of();
                    ForeignKeyDefinition.ActionType onDelete = ForeignKeyDefinition.ActionType.RESTRICT;
                    ForeignKeyDefinition.ActionType onUpdate = ForeignKeyDefinition.ActionType.RESTRICT;
                    for (StratosSQLParser.FkActionContext actCtx : tcCtx.fkAction()) {
                        if (actCtx.DELETE() != null) {
                            onDelete = parseFkActionType(actCtx.fkActionType());
                        } else if (actCtx.UPDATE() != null) {
                            onUpdate = parseFkActionType(actCtx.fkActionType());
                        }
                    }
                    foreignKeys.add(new ForeignKeyDefinition(fkName, tableName, childCols, parentTable, parentCols, onDelete, onUpdate));
                } else if (tcCtx.CHECK() != null && tcCtx.expression() != null) {
                    String chkName = cName != null ? cName : tableName + "_check";
                    WhereExpr expr = buildWhereExpr(tcCtx.expression());
                    String exprSql = tcCtx.expression().getText();
                    checkConstraints.add(new CheckConstraintDefinition(chkName, exprSql, expr));
                }
            }
        }

        return new CreateTableStatement(tableName, columns, primaryKeyColumns, tableConstraints, foreignKeys, checkConstraints, ifNotExists);
    }

    private List<String> extractColumns(StratosSQLParser.ColumnNameListContext ctx) {
        List<String> cols = new ArrayList<>();
        if (ctx != null && ctx.columnName() != null) {
            for (StratosSQLParser.ColumnNameContext c : ctx.columnName()) {
                cols.add(cleanColumnName(c.getText()));
            }
        }
        return cols;
    }

    private ForeignKeyDefinition.ActionType parseFkActionType(StratosSQLParser.FkActionTypeContext ctx) {
        if (ctx == null) return ForeignKeyDefinition.ActionType.RESTRICT;
        if (ctx.CASCADE() != null) return ForeignKeyDefinition.ActionType.CASCADE;
        if (ctx.SET() != null) return ForeignKeyDefinition.ActionType.SET_NULL;
        if (ctx.RESTRICT() != null) return ForeignKeyDefinition.ActionType.RESTRICT;
        return ForeignKeyDefinition.ActionType.NO_ACTION;
    }

    public WhereExpr parseWhereExpression(String exprSql) {
        if (exprSql == null || exprSql.isBlank()) return null;
        CharStream charStream = CharStreams.fromString(exprSql);
        StratosSQLLexer lexer = new StratosSQLLexer(charStream);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        StratosSQLParser parser = new StratosSQLParser(tokens);
        return buildWhereExpr(parser.expression());
    }

    private InsertStatement buildInsert(StratosSQLParser.InsertContext ctx) {
        String tableName = parseTableName(ctx.tableName());

        // One real row per VALUES tuple - see InsertStatement's own javadoc
        // for why this is now a list of rows rather than a single flat list.
        List<List<String>> rows = new ArrayList<>();
        for (StratosSQLParser.ValueListContext valueListCtx : ctx.valueList()) {
            List<String> row = new ArrayList<>();
            for (StratosSQLParser.InsertValueContext valCtx : valueListCtx.insertValue()) {
                if (valCtx.literal() != null) {
                    row.add(valCtx.literal().getText());
                } else {
                    // A nextval('seq') or currval('seq') call - stored as its
                    // own raw text (e.g. "nextval('t_id_seq')") for the
                    // executor to recognize and resolve at insert time,
                    // rather than trying to parse it as a literal.
                    row.add(valCtx.getText());
                }
            }
            rows.add(row);
        }

        // The optional (col1, col2, ...) list right after the table name - a
        // real, previously-silent bug this fixes: this was parsed by the
        // grammar but never actually captured here, so every INSERT with an
        // explicit column list silently mapped its values POSITIONALLY
        // against the table's full schema instead of against the NAMED
        // columns - e.g. INSERT INTO t (name, age) VALUES ('Alice', 30)
        // would put 'Alice' into whatever the table's FIRST column happened
        // to be, not into 'name'. Found while investigating SERIAL/sequence
        // support, which fundamentally depends on this working correctly
        // (SERIAL's entire point is letting a column be omitted).
        List<String> columns = new ArrayList<>();
        for (StratosSQLParser.ColumnNameContext colCtx : ctx.columnName()) {
            columns.add(cleanColumnName(colCtx.getText()));
        }

        List<String> returningColumns = new ArrayList<>();
        if (ctx.returningClause() != null) {
            if (ctx.returningClause().STAR() != null) {
                returningColumns.add("*");
            } else {
                for (StratosSQLParser.ColumnNameContext colCtx : ctx.returningClause().columnName()) {
                    returningColumns.add(cleanColumnName(colCtx.getText()));
                }
            }
        }

        return new InsertStatement(tableName, columns, rows, returningColumns);
    }

    /**
     * Recursively walks the labeled `expression` alternatives into a real
     * WhereExpr tree. Subqueries recurse back into buildSelect, so
     * arbitrarily nested subqueries (a subquery whose own WHERE contains
     * another subquery) fall out naturally from the recursion - no special
     * casing needed for depth.
     */
    private WhereExpr buildWhereExpr(StratosSQLParser.ExpressionContext ctx) {
        if (ctx == null) {
            return null;
        }
        if (ctx instanceof StratosSQLParser.ParenExprContext c) {
            return buildWhereExpr(c.expression());
        }
        if (ctx instanceof StratosSQLParser.NotExprContext c) {
            return new WhereExpr.Not(buildWhereExpr(c.expression()));
        }
        if (ctx instanceof StratosSQLParser.AndExprContext c) {
            return new WhereExpr.And(buildWhereExpr(c.expression(0)), buildWhereExpr(c.expression(1)));
        }
        if (ctx instanceof StratosSQLParser.OrExprContext c) {
            return new WhereExpr.Or(buildWhereExpr(c.expression(0)), buildWhereExpr(c.expression(1)));
        }
        if (ctx instanceof StratosSQLParser.EqColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(cleanColumnName(c.columnName(0).getText()), "=", cleanColumnName(c.columnName(1).getText()));
        }
        if (ctx instanceof StratosSQLParser.GtColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(cleanColumnName(c.columnName(0).getText()), ">", cleanColumnName(c.columnName(1).getText()));
        }
        if (ctx instanceof StratosSQLParser.LtColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(cleanColumnName(c.columnName(0).getText()), "<", cleanColumnName(c.columnName(1).getText()));
        }
        if (ctx instanceof StratosSQLParser.GeColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(cleanColumnName(c.columnName(0).getText()), ">=", cleanColumnName(c.columnName(1).getText()));
        }
        if (ctx instanceof StratosSQLParser.LeColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(cleanColumnName(c.columnName(0).getText()), "<=", cleanColumnName(c.columnName(1).getText()));
        }
        if (ctx instanceof StratosSQLParser.NeColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(cleanColumnName(c.columnName(0).getText()), "!=", cleanColumnName(c.columnName(1).getText()));
        }
        if (ctx instanceof StratosSQLParser.EqCompareContext c) {
            return new WhereExpr.Comparison(cleanColumnName(c.columnName().getText()), "=", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.GtCompareContext c) {
            return new WhereExpr.Comparison(cleanColumnName(c.columnName().getText()), ">", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.LtCompareContext c) {
            return new WhereExpr.Comparison(cleanColumnName(c.columnName().getText()), "<", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.GeCompareContext c) {
            return new WhereExpr.Comparison(cleanColumnName(c.columnName().getText()), ">=", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.LeCompareContext c) {
            return new WhereExpr.Comparison(cleanColumnName(c.columnName().getText()), "<=", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.NeCompareContext c) {
            return new WhereExpr.Comparison(cleanColumnName(c.columnName().getText()), "!=", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.LikeCompareContext c) {
            return new WhereExpr.Like(cleanColumnName(c.columnName().getText()), c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.ContainsCompareContext c) {
            return new WhereExpr.Contains(cleanColumnName(c.columnName().getText()), c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.ArrayContainsCompareContext c) {
            return new WhereExpr.ArrayContains(cleanColumnName(c.columnName().getText()), c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.TsMatchCompareContext c) {
            return new WhereExpr.TsMatch(cleanColumnName(c.columnName().getText()), c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.EqZeroArgFunctionCompareContext c) {
            return new WhereExpr.EqZeroArgFunction(cleanColumnName(c.columnName().getText()), c.IDENTIFIER().getText());
        }
        if (ctx instanceof StratosSQLParser.JsonExtractTextEqCompareContext c) {
            return new WhereExpr.JsonExtractTextEquals(cleanColumnName(c.columnName().getText()), c.literal(0).getText(), c.literal(1).getText());
        }
        if (ctx instanceof StratosSQLParser.RangeOverlapsCompareContext c) {
            return new WhereExpr.RangeOverlaps(cleanColumnName(c.columnName(0).getText()), cleanColumnName(c.columnName(1).getText()), c.literal(0).getText(), c.literal(1).getText());
        }
        if (ctx instanceof StratosSQLParser.InListExprContext c) {
            return new WhereExpr.InList(cleanColumnName(c.columnName().getText()), buildValueList(c.valueList()), false);
        }
        if (ctx instanceof StratosSQLParser.NotInListExprContext c) {
            return new WhereExpr.InList(cleanColumnName(c.columnName().getText()), buildValueList(c.valueList()), true);
        }
        if (ctx instanceof StratosSQLParser.InSubqueryExprContext c) {
            return new WhereExpr.InSubquery(cleanColumnName(c.columnName().getText()), buildSelect(c.select()), false);
        }
        if (ctx instanceof StratosSQLParser.NotInSubqueryExprContext c) {
            return new WhereExpr.InSubquery(cleanColumnName(c.columnName().getText()), buildSelect(c.select()), true);
        }
        if (ctx instanceof StratosSQLParser.EqSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(cleanColumnName(c.columnName().getText()), "=", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.GtSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(cleanColumnName(c.columnName().getText()), ">", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.LtSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(cleanColumnName(c.columnName().getText()), "<", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.GeSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(cleanColumnName(c.columnName().getText()), ">=", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.LeSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(cleanColumnName(c.columnName().getText()), "<=", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.NeSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(cleanColumnName(c.columnName().getText()), "!=", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.ExistsExprContext c) {
            return new WhereExpr.ExistsSubquery(buildSelect(c.select()), false);
        }
        if (ctx instanceof StratosSQLParser.NotExistsExprContext c) {
            return new WhereExpr.ExistsSubquery(buildSelect(c.select()), true);
        }
        throw new IllegalArgumentException("Unsupported expression: " + ctx.getText());
    }

    private List<String> buildValueList(StratosSQLParser.ValueListContext ctx) {
        List<String> values = new ArrayList<>();
        for (StratosSQLParser.InsertValueContext val : ctx.insertValue()) {
            values.add(val.getText());
        }
        return values;
    }

    private SelectStatement buildSelect(StratosSQLParser.SelectContext ctx) {
        // A real, FROM-less SELECT ("SELECT version()", "SELECT 1") is genuinely
        // valid Postgres SQL, and virtually every serious client/ORM relies on it
        // for at least server-version detection at connection time - see
        // ExecutorEngine.executeSelect's own comment for how this is executed
        // (the select list's own expressions evaluated exactly once, with no table
        // to iterate at all, producing exactly one output row).
        String tableName = null;
        SelectStatement fromSubquery = null;
        String tableAlias = null;

        if (ctx.tableSource() != null) {
            if (ctx.tableSource().tableName() != null) {
                tableName = parseTableName(ctx.tableSource().tableName());
            }
            if (ctx.tableSource().select() != null) {
                fromSubquery = buildSelect(ctx.tableSource().select());
            }
            if (ctx.tableSource().alias() != null) {
                tableAlias = cleanIdentifier(ctx.tableSource().alias().getText());
            }
        }
        List<String> columns = new ArrayList<>();
        List<String> columnAliases = new ArrayList<>();
        List<AggregateCall> aggregates = new ArrayList<>();
        List<WindowFunctionCall> windowFunctions = new ArrayList<>();
        List<FunctionCallItem> functionCalls = new ArrayList<>();

        if (ctx.selectList().STAR() != null) {
            columns.add("*");
        } else {
            for (StratosSQLParser.SelectItemContext itemCtx : ctx.selectList().selectItem()) {
                String alias = itemCtx.alias() != null ? cleanIdentifier(itemCtx.alias().getText()) : null;
                if (itemCtx.windowFunction() != null) {
                    windowFunctions.add(buildWindowFunctionCall(itemCtx.windowFunction(), alias));
                } else if (itemCtx.aggregateFunction() != null) {
                    aggregates.add(buildAggregateCall(itemCtx.aggregateFunction(), alias));
                } else if (itemCtx.functionCall() != null) {
                    functionCalls.add(buildFunctionCallItem(itemCtx.functionCall(), alias));
                } else if (itemCtx.columnName() != null) {
                    columns.add(cleanColumnName(itemCtx.columnName().getText()));
                    columnAliases.add(alias);
                } else if (itemCtx.literal() != null) {
                    columns.add(itemCtx.literal().getText());
                    columnAliases.add(alias);
                } else {
                    // The `expression (AS alias)?` selectItem alternative - not really meaningful
                    // for this engine's simple SELECT list (an expression like "age > 30" isn't a
                    // projectable column), kept only because the grammar allows it. Fall back to
                    // raw text rather than silently dropping it.
                    columns.add(cleanColumnName(itemCtx.getText()));
                }
            }
        }

        List<JoinClause> joins = new ArrayList<>();
        for (StratosSQLParser.JoinClauseContext joinCtx : ctx.joinClause()) {
            joins.add(buildJoinClause(joinCtx));
        }

        List<String> groupBy = new ArrayList<>();
        if (ctx.groupByList() != null) {
            for (StratosSQLParser.ColumnNameContext colCtx : ctx.groupByList().columnName()) {
                groupBy.add(cleanColumnName(colCtx.getText()));
            }
        }

        String havingClause = ctx.havingClause() != null ? ctx.havingClause().getText() : null;
        WhereExpr where = buildWhereExpr(ctx.expression());
        String limit = ctx.limitValue() != null ? ctx.limitValue().getText() : null;
        String offset = ctx.offsetValue() != null ? ctx.offsetValue().getText() : null;

        List<OrderItem> orderItems = new ArrayList<>();
        String orderByText = null;
        if (ctx.orderList() != null) {
            orderByText = ctx.orderList().getText();
            for (StratosSQLParser.OrderItemContext itemCtx : ctx.orderList().orderItem()) {
                boolean ascending = itemCtx.DESC() == null;
                orderItems.add(new OrderItem(cleanColumnName(itemCtx.columnName().getText()), ascending));
            }
        }

        boolean forUpdate = ctx.forUpdateClause() != null;
        boolean distinct = ctx.DISTINCT() != null;
        return new SelectStatement(tableName, columns, where, orderByText, limit, offset, joins, aggregates, groupBy, havingClause, windowFunctions, functionCalls, columnAliases, orderItems, fromSubquery, tableAlias, forUpdate, distinct);
    }

    private IsolationLevel parseIsolationLevel(StratosSQLParser.IsolationLevelContext ctx) {
        if (ctx == null) return IsolationLevel.READ_COMMITTED;
        if (ctx.READ() != null && ctx.COMMITTED() != null) return IsolationLevel.READ_COMMITTED;
        if (ctx.REPEATABLE() != null && ctx.READ() != null) return IsolationLevel.REPEATABLE_READ;
        if (ctx.SERIALIZABLE() != null) return IsolationLevel.SERIALIZABLE;
        if (ctx.READ() != null && ctx.UNCOMMITTED() != null) return IsolationLevel.READ_UNCOMMITTED;
        return IsolationLevel.fromString(ctx.getText());
    }

    private FunctionCallItem buildFunctionCallItem(StratosSQLParser.FunctionCallContext ctx, String alias) {
        String functionName = stripSchemaQualifier(ctx.functionName().getText());
        List<String> args = new ArrayList<>();
        for (StratosSQLParser.FunctionArgContext argCtx : ctx.functionArg()) {
            args.add(argCtx.getText());
        }
        return new FunctionCallItem(functionName, args, alias);
    }


    private WindowFunctionCall buildWindowFunctionCall(StratosSQLParser.WindowFunctionContext ctx, String alias) {
        String functionName;
        if (ctx.ROW_NUMBER() != null) functionName = "ROW_NUMBER";
        else if (ctx.RANK() != null) functionName = "RANK";
        else functionName = "DENSE_RANK";

        List<String> partitionBy = new ArrayList<>();
        if (ctx.groupByList() != null) {
            for (StratosSQLParser.ColumnNameContext colCtx : ctx.groupByList().columnName()) {
                partitionBy.add(cleanColumnName(colCtx.getText()));
            }
        }

        List<WindowOrderItem> orderBy = new ArrayList<>();
        if (ctx.orderList() != null) {
            for (StratosSQLParser.OrderItemContext itemCtx : ctx.orderList().orderItem()) {
                boolean descending = itemCtx.DESC() != null;
                orderBy.add(new WindowOrderItem(cleanColumnName(itemCtx.columnName().getText()), descending));
            }
        }

        return new WindowFunctionCall(functionName, partitionBy, orderBy, alias != null ? alias : functionName);
    }

    private AggregateCall buildAggregateCall(StratosSQLParser.AggregateFunctionContext ctx, String alias) {
        String function;
        if (ctx.COUNT() != null) function = "COUNT";
        else if (ctx.SUM() != null) function = "SUM";
        else if (ctx.AVG() != null) function = "AVG";
        else if (ctx.MIN() != null) function = "MIN";
        else function = "MAX";

        String argument = ctx.STAR() != null ? "*" : cleanColumnName(ctx.columnName().getText());
        boolean distinct = ctx.DISTINCT() != null;
        return new AggregateCall(function, argument, alias, distinct);
    }

    private JoinClause buildJoinClause(StratosSQLParser.JoinClauseContext ctx) {
        String tableName = null;
        SelectStatement subquery = null;
        String tableAlias = null;

        if (ctx.tableSource() != null) {
            if (ctx.tableSource().tableName() != null) {
                tableName = parseTableName(ctx.tableSource().tableName());
            }
            if (ctx.tableSource().select() != null) {
                subquery = buildSelect(ctx.tableSource().select());
            }
            if (ctx.tableSource().alias() != null) {
                tableAlias = cleanIdentifier(ctx.tableSource().alias().getText());
            }
        }

        List<StratosSQLParser.ColumnNameContext> columns = ctx.columnName();
        String leftColumn = cleanColumnName(columns.get(0).getText());
        String rightColumn = cleanColumnName(columns.get(1).getText());
        JoinClause.JoinType joinType = JoinClause.JoinType.INNER;
        if (ctx.joinType() != null) {
            if (ctx.joinType().LEFT() != null) {
                joinType = JoinClause.JoinType.LEFT;
            } else if (ctx.joinType().RIGHT() != null) {
                joinType = JoinClause.JoinType.RIGHT;
            } else if (ctx.joinType().FULL() != null) {
                joinType = JoinClause.JoinType.FULL;
            } else if (ctx.joinType().CROSS() != null) {
                joinType = JoinClause.JoinType.CROSS;
            } else if (ctx.joinType().SEMI() != null) {
                joinType = JoinClause.JoinType.SEMI;
            } else if (ctx.joinType().ANTI() != null) {
                joinType = JoinClause.JoinType.ANTI;
            }
        }
        return new JoinClause(tableName, leftColumn, rightColumn, joinType, subquery, tableAlias);
    }

    private DeleteStatement buildDelete(StratosSQLParser.DeleteContext ctx) {
        String tableName = parseTableName(ctx.tableName());
        WhereExpr where = buildWhereExpr(ctx.expression());
        return new DeleteStatement(tableName, where);
    }

    private DropTableStatement buildDropTable(StratosSQLParser.DropTableContext ctx) {
        String tableName = parseTableName(ctx.tableName());
        boolean ifExists = ctx.PLSQL_IF() != null;
        return new DropTableStatement(tableName, ifExists);
    }

    private AlterTableAddConstraintStatement buildAlterTableAddConstraint(StratosSQLParser.AlterTableAddConstraintContext ctx) {
        String tableName = parseTableName(ctx.tableName());
        StratosSQLParser.TableConstraintContext tcCtx = ctx.tableConstraint();
        String constraintName = tcCtx.constraintName() != null ? cleanIdentifier(tcCtx.constraintName().getText()) : null;
        if (tcCtx.PRIMARY() != null) {
            List<String> cols = extractColumns(tcCtx.columnNameList(0));
            return new AlterTableAddConstraintStatement(tableName, constraintName, "PRIMARY KEY", cols);
        } else if (tcCtx.UNIQUE() != null) {
            List<String> cols = extractColumns(tcCtx.columnNameList(0));
            return new AlterTableAddConstraintStatement(tableName, constraintName, "UNIQUE", cols);
        } else if (tcCtx.FOREIGN() != null) {
            List<String> childCols = extractColumns(tcCtx.childCols);
            String parentTable = parseTableName(tcCtx.tableName());
            List<String> parentCols = tcCtx.parentCols != null ? extractColumns(tcCtx.parentCols) : List.of();
            ForeignKeyDefinition.ActionType onDelete = ForeignKeyDefinition.ActionType.RESTRICT;
            ForeignKeyDefinition.ActionType onUpdate = ForeignKeyDefinition.ActionType.RESTRICT;
            for (StratosSQLParser.FkActionContext actCtx : tcCtx.fkAction()) {
                if (actCtx.DELETE() != null) {
                    onDelete = parseFkActionType(actCtx.fkActionType());
                } else if (actCtx.UPDATE() != null) {
                    onUpdate = parseFkActionType(actCtx.fkActionType());
                }
            }
            return new AlterTableAddConstraintStatement(tableName, constraintName, "FOREIGN KEY", childCols, parentTable, parentCols, onDelete.name(), onUpdate.name(), null);
        } else if (tcCtx.CHECK() != null) {
            WhereExpr expr = buildWhereExpr(tcCtx.expression());
            return new AlterTableAddConstraintStatement(tableName, constraintName, "CHECK", List.of(), null, List.of(), null, null, expr);
        }
        throw new IllegalArgumentException("Unknown table constraint: " + tcCtx.getText());
    }

    private AlterTableDropConstraintStatement buildAlterTableDropConstraint(StratosSQLParser.AlterTableDropConstraintContext ctx) {
        String tableName = parseTableName(ctx.tableName());
        String constraintName = cleanIdentifier(ctx.constraintName().getText());
        boolean ifExists = ctx.PLSQL_IF() != null;
        return new AlterTableDropConstraintStatement(tableName, constraintName, ifExists);
    }

    private CreateSequenceStatement buildCreateSequence(StratosSQLParser.CreateSequenceContext ctx) {
        String name = cleanSequenceName(ctx.sequenceName().getText());
        boolean ifNotExists = ctx.PLSQL_IF() != null;
        long startValue = 1;
        long incrementBy = 1;
        Long minValue = null;
        Long maxValue = null;
        Long cache = null;
        boolean cycle = false;

        for (StratosSQLParser.SequenceOptionContext opt : ctx.sequenceOption()) {
            if (opt.START() != null && opt.INTEGER_LITERAL() != null) {
                startValue = Long.parseLong(opt.INTEGER_LITERAL().getText());
            } else if (opt.INCREMENT() != null && opt.INTEGER_LITERAL() != null) {
                incrementBy = Long.parseLong(opt.INTEGER_LITERAL().getText());
            } else if (opt.MINVALUE() != null && opt.INTEGER_LITERAL() != null) {
                minValue = Long.parseLong(opt.INTEGER_LITERAL().getText());
            } else if (opt.MAXVALUE() != null && opt.INTEGER_LITERAL() != null) {
                maxValue = Long.parseLong(opt.INTEGER_LITERAL().getText());
            } else if (opt.CACHE() != null && opt.INTEGER_LITERAL() != null) {
                cache = Long.parseLong(opt.INTEGER_LITERAL().getText());
            } else if (opt.CYCLE() != null && opt.NO() == null) {
                cycle = true;
            } else if (opt.NO() != null && opt.CYCLE() != null) {
                cycle = false;
            }
        }
        return new CreateSequenceStatement(name, startValue, incrementBy, ifNotExists, minValue, maxValue, cache, cycle);
    }

    private AlterSequenceStatement buildAlterSequence(StratosSQLParser.AlterSequenceContext ctx) {
        String name = cleanSequenceName(ctx.sequenceName().getText());
        boolean ifExists = ctx.PLSQL_IF() != null;
        Long restartValue = null;
        Long incrementBy = null;
        Long minValue = null;
        Long maxValue = null;
        Long cache = null;
        Boolean cycle = null;

        for (StratosSQLParser.AlterSequenceOptionContext opt : ctx.alterSequenceOption()) {
            if (opt.RESTART() != null) {
                if (opt.INTEGER_LITERAL() != null) {
                    restartValue = Long.parseLong(opt.INTEGER_LITERAL().getText());
                } else {
                    restartValue = 1L;
                }
            } else if (opt.sequenceOption() != null) {
                StratosSQLParser.SequenceOptionContext sOpt = opt.sequenceOption();
                if (sOpt.START() != null && sOpt.INTEGER_LITERAL() != null) {
                    restartValue = Long.parseLong(sOpt.INTEGER_LITERAL().getText());
                } else if (sOpt.INCREMENT() != null && sOpt.INTEGER_LITERAL() != null) {
                    incrementBy = Long.parseLong(sOpt.INTEGER_LITERAL().getText());
                } else if (sOpt.MINVALUE() != null && sOpt.INTEGER_LITERAL() != null) {
                    minValue = Long.parseLong(sOpt.INTEGER_LITERAL().getText());
                } else if (sOpt.MAXVALUE() != null && sOpt.INTEGER_LITERAL() != null) {
                    maxValue = Long.parseLong(sOpt.INTEGER_LITERAL().getText());
                } else if (sOpt.CACHE() != null && sOpt.INTEGER_LITERAL() != null) {
                    cache = Long.parseLong(sOpt.INTEGER_LITERAL().getText());
                } else if (sOpt.CYCLE() != null && sOpt.NO() == null) {
                    cycle = true;
                } else if (sOpt.NO() != null && sOpt.CYCLE() != null) {
                    cycle = false;
                }
            }
        }
        return new AlterSequenceStatement(name, restartValue, incrementBy, minValue, maxValue, cache, cycle, ifExists);
    }

    public static String cleanIdentifier(String s) {
        if (s == null) return null;
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
            s = s.substring(1, s.length() - 1).replace("\"\"", "\"");
        }
        return s;
    }

    public static String parseTableName(StratosSQLParser.TableNameContext ctx) {
        if (ctx == null) return null;
        if (ctx.anyIdentifier().size() == 2) {
            String schema = cleanIdentifier(ctx.anyIdentifier(0).getText());
            String table = cleanIdentifier(ctx.anyIdentifier(1).getText());
            if ("public".equalsIgnoreCase(schema)) {
                return table;
            }
            return table;
        }
        return cleanIdentifier(ctx.anyIdentifier(0).getText());
    }

    public static String cleanColumnName(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        if (raw.contains(".")) {
            String[] parts = raw.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) sb.append(".");
                sb.append(cleanIdentifier(parts[i]));
            }
            return sb.toString();
        }
        return cleanIdentifier(raw);
    }

    public static String cleanSequenceName(String raw) {
        if (raw == null) return null;
        raw = raw.trim();
        if (raw.startsWith("'") && raw.endsWith("'") && raw.length() >= 2) {
            raw = raw.substring(1, raw.length() - 1);
        }
        if (raw.endsWith("::regclass")) {
            raw = raw.substring(0, raw.length() - "::regclass".length()).trim();
            if (raw.startsWith("'") && raw.endsWith("'") && raw.length() >= 2) {
                raw = raw.substring(1, raw.length() - 1);
            }
        }
        if (raw.contains(".")) {
            String[] parts = raw.split("\\.");
            return cleanIdentifier(parts[parts.length - 1]);
        }
        return cleanIdentifier(raw);
    }

    private CopyStatement buildCopyStatement(StratosSQLParser.CopyStatementContext ctx) {
        String tableName = ctx.tableName().getText();
        List<String> columns = ctx.columnName().isEmpty() ? null
            : ctx.columnName().stream().map(RuleContext::getText).toList();
        boolean isFrom = ctx.FROM() != null;

        StratosSQLParser.CopyTargetContext targetCtx = ctx.copyTarget();
        boolean isStdio = targetCtx.STDIN() != null || targetCtx.STDOUT() != null;
        String target = isStdio ? targetCtx.getText().toUpperCase(java.util.Locale.ROOT) : targetCtx.STRING_LITERAL().getText();

        String format = null;
        String delimiter = null;
        boolean header = false;
        String nullString = null;
        for (StratosSQLParser.CopyOptionContext opt : ctx.copyOption()) {
            if (opt.FORMAT() != null) {
                format = (opt.CSV() != null ? "CSV" : "TEXT");
            } else if (opt.DELIMITER() != null) {
                delimiter = opt.STRING_LITERAL().getText();
            } else if (opt.HEADER() != null) {
                header = opt.FALSE() == null;
            } else if (opt.NULL() != null) {
                nullString = opt.STRING_LITERAL().getText();
            }
        }
        return new CopyStatement(tableName, columns, isFrom, target, isStdio, format, delimiter, header, nullString);
    }

    private CreateRoleStatement buildCreateRole(StratosSQLParser.CreateRoleContext ctx) {
        String roleName = ctx.roleName().getText();
        boolean login = false;
        boolean superuser = false;
        String password = null;
        for (StratosSQLParser.RoleOptionContext opt : ctx.roleOption()) {
            if (opt.LOGIN() != null) login = true;
            else if (opt.NOLOGIN() != null) login = false;
            else if (opt.SUPERUSER() != null) superuser = true;
            else if (opt.NOSUPERUSER() != null) superuser = false;
            else if (opt.PASSWORD() != null) password = opt.STRING_LITERAL().getText();
        }
        return new CreateRoleStatement(roleName, login, superuser, password);
    }

    private Statement buildGrantStatement(StratosSQLParser.GrantStatementContext ctx) {
        if (ctx.grantTable() != null) {
            StratosSQLParser.GrantTableContext tctx = ctx.grantTable();
            List<String> privileges = buildPrivilegeList(tctx.privilegeList());
            List<String> roles = tctx.roleList().roleName().stream().map(RuleContext::getText).toList();
            return new GrantStatement(privileges, tctx.tableName().getText(), roles);
        } else if (ctx.grantDatabase() != null) {
            StratosSQLParser.GrantDatabaseContext dctx = ctx.grantDatabase();
            List<String> privileges = buildDbPrivilegeList(dctx.dbPrivilegeList());
            List<String> roles = dctx.roleList().roleName().stream().map(RuleContext::getText).toList();
            return new GrantDatabaseStatement(privileges, dctx.databaseName().getText(), roles);
        } else if (ctx.grantRole() != null) {
            StratosSQLParser.GrantRoleContext rctx = ctx.grantRole();
            List<String> roles = rctx.roleList(0).roleName().stream().map(RuleContext::getText).toList();
            List<String> grantees = rctx.roleList(1).roleName().stream().map(RuleContext::getText).toList();
            boolean withAdmin = rctx.ADMIN() != null;
            return new GrantRoleStatement(roles, grantees, withAdmin);
        }
        throw new IllegalArgumentException("Unknown GRANT statement format");
    }

    private Statement buildRevokeStatement(StratosSQLParser.RevokeStatementContext ctx) {
        if (ctx.revokeTable() != null) {
            StratosSQLParser.RevokeTableContext tctx = ctx.revokeTable();
            List<String> privileges = buildPrivilegeList(tctx.privilegeList());
            List<String> roles = tctx.roleList().roleName().stream().map(RuleContext::getText).toList();
            return new RevokeStatement(privileges, tctx.tableName().getText(), roles);
        } else if (ctx.revokeDatabase() != null) {
            StratosSQLParser.RevokeDatabaseContext dctx = ctx.revokeDatabase();
            List<String> privileges = buildDbPrivilegeList(dctx.dbPrivilegeList());
            List<String> roles = dctx.roleList().roleName().stream().map(RuleContext::getText).toList();
            return new RevokeDatabaseStatement(privileges, dctx.databaseName().getText(), roles);
        } else if (ctx.revokeRole() != null) {
            StratosSQLParser.RevokeRoleContext rctx = ctx.revokeRole();
            List<String> roles = rctx.roleList(0).roleName().stream().map(RuleContext::getText).toList();
            List<String> grantees = rctx.roleList(1).roleName().stream().map(RuleContext::getText).toList();
            boolean adminOptionOnly = rctx.ADMIN() != null;
            return new RevokeRoleStatement(roles, grantees, adminOptionOnly);
        }
        throw new IllegalArgumentException("Unknown REVOKE statement format");
    }

    private List<String> buildDbPrivilegeList(StratosSQLParser.DbPrivilegeListContext ctx) {
        List<String> privileges = new ArrayList<>();
        for (StratosSQLParser.DbPrivilegeNameContext p : ctx.dbPrivilegeName()) {
            if (p.ALL() != null) {
                privileges.add("CONNECT");
                privileges.add("CREATE");
                privileges.add("TEMP");
            } else if (p.TEMPORARY() != null) {
                privileges.add("TEMP");
            } else {
                privileges.add(p.getText().toUpperCase(java.util.Locale.ROOT));
            }
        }
        return privileges;
    }

    private List<String> buildPrivilegeList(StratosSQLParser.PrivilegeListContext ctx) {
        List<String> privileges = new ArrayList<>();
        for (StratosSQLParser.PrivilegeNameContext p : ctx.privilegeName()) {
            if (p.ALL() != null) {
                privileges.add("SELECT");
                privileges.add("INSERT");
                privileges.add("UPDATE");
                privileges.add("DELETE");
            } else {
                privileges.add(p.getText().toUpperCase(java.util.Locale.ROOT));
            }
        }
        return privileges;
    }

    private Statement buildReindex(StratosSQLParser.ReindexStatementContext ctx) {
        if (ctx.INDEX() != null) {
            return new ReindexStatement(ReindexStatement.TargetType.INDEX, ctx.indexName().getText());
        } else if (ctx.TABLE() != null) {
            return new ReindexStatement(ReindexStatement.TargetType.TABLE, ctx.tableName().getText());
        } else {
            return new ReindexStatement(ReindexStatement.TargetType.TABLE, ctx.tableName().getText());
        }
    }
}