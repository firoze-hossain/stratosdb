package com.stratosdb.sql.parser;

import com.stratosdb.sql.ast.*;
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
            StratosSQLParser.GrantStatementContext gctx = ctx.grantStatement();
            List<String> privileges = buildPrivilegeList(gctx.privilegeList());
            return new GrantStatement(privileges, gctx.tableName().getText(), gctx.roleName().getText());
        } else if (ctx.revokeStatement() != null) {
            StratosSQLParser.RevokeStatementContext rctx = ctx.revokeStatement();
            List<String> privileges = buildPrivilegeList(rctx.privilegeList());
            return new RevokeStatement(privileges, rctx.tableName().getText(), rctx.roleName().getText());
        } else if (ctx.alterTableAddColumn() != null) {
            StratosSQLParser.AlterTableAddColumnContext actx = ctx.alterTableAddColumn();
            String defaultVal = actx.defaultValue() != null ? actx.defaultValue().getText() : null;
            return new AlterTableAddColumnStatement(actx.tableName().getText(), actx.columnName().getText(), actx.dataType().getText(), defaultVal);
        } else if (ctx.alterTableDropColumn() != null) {
            StratosSQLParser.AlterTableDropColumnContext actx = ctx.alterTableDropColumn();
            return new AlterTableDropColumnStatement(actx.tableName().getText(), actx.columnName().getText());
        } else if (ctx.alterTableRenameColumn() != null) {
            StratosSQLParser.AlterTableRenameColumnContext actx = ctx.alterTableRenameColumn();
            return new AlterTableRenameColumnStatement(actx.tableName().getText(), actx.columnName(0).getText(), actx.columnName(1).getText());
        } else if (ctx.alterTableRenameTable() != null) {
            StratosSQLParser.AlterTableRenameTableContext actx = ctx.alterTableRenameTable();
            return new AlterTableRenameTableStatement(actx.tableName(0).getText(), actx.tableName(1).getText());
        } else if (ctx.alterTableAlterColumnType() != null) {
            StratosSQLParser.AlterTableAlterColumnTypeContext actx = ctx.alterTableAlterColumnType();
            return new AlterTableAlterColumnTypeStatement(actx.tableName().getText(), actx.columnName().getText(), actx.dataType().getText());
        } else if (ctx.alterTableSetDefault() != null) {
            StratosSQLParser.AlterTableSetDefaultContext actx = ctx.alterTableSetDefault();
            return new AlterTableSetDefaultStatement(actx.tableName().getText(), actx.columnName().getText(), actx.defaultValue().getText());
        } else if (ctx.alterTableDropDefault() != null) {
            StratosSQLParser.AlterTableDropDefaultContext actx = ctx.alterTableDropDefault();
            return new AlterTableDropDefaultStatement(actx.tableName().getText(), actx.columnName().getText());
        } else if (ctx.showTables() != null) {
            return new ShowTablesStatement();
        } else if (ctx.showStats() != null) {
            return new ShowStatsStatement();
        } else if (ctx.showCatalog() != null) {
            return new ShowCatalogStatement();
        } else if (ctx.explain() != null) {
            return new ExplainStatement(buildSelect(ctx.explain().select()));
        } else if (ctx.analyze() != null) {
            return new AnalyzeStatement(ctx.analyze().tableName().getText());
        } else if (ctx.vacuum() != null) {
            return new VacuumStatement(ctx.vacuum().tableName().getText());
        } else if (ctx.checkpointStatement() != null) {
            return new CheckpointStatement();
        } else if (ctx.promoteStatement() != null) {
            return new PromoteStatement();
        } else if (ctx.beginTxn() != null) {
            return new BeginStatement();
        } else if (ctx.commitTxn() != null) {
            return new CommitStatement();
        } else if (ctx.rollbackTxn() != null) {
            return new RollbackStatement();
        } else if (ctx.createView() != null) {
            return new CreateViewStatement(ctx.createView().viewName().getText(), buildSelect(ctx.createView().select()));
        } else if (ctx.dropView() != null) {
            return new DropViewStatement(ctx.dropView().viewName().getText());
        } else if (ctx.savepoint() != null) {
            return new SavepointStatement(ctx.savepoint().savepointName().getText());
        } else if (ctx.releaseSavepoint() != null) {
            return new ReleaseSavepointStatement(ctx.releaseSavepoint().savepointName().getText());
        } else if (ctx.rollbackToSavepoint() != null) {
            return new RollbackToSavepointStatement(ctx.rollbackToSavepoint().savepointName().getText());
        } else if (ctx.selectWithCte() != null) {
            StratosSQLParser.SelectWithCteContext cteCtx = ctx.selectWithCte();
            String cteName = cteCtx.cteName().getText();
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
            StratosSQLParser.CreateSequenceContext seqCtx = ctx.createSequence();
            String name = seqCtx.sequenceName().getText();
            List<TerminalNode> ints = seqCtx.INTEGER_LITERAL();
            // START and INCREMENT are both optional and order-independent in
            // input, but the grammar only records their integer literals in
            // left-to-right appearance order - START's value (if given)
            // always comes before INCREMENT's in that list, matching how a
            // real "CREATE SEQUENCE x START WITH 100 INCREMENT BY 5" reads.
            long startValue = 1;
            long incrementBy = 1;
            if (seqCtx.START() != null && seqCtx.INCREMENT() != null && ints.size() == 2) {
                startValue = Long.parseLong(ints.get(0).getText());
                incrementBy = Long.parseLong(ints.get(1).getText());
            } else if (seqCtx.START() != null && ints.size() >= 1) {
                startValue = Long.parseLong(ints.get(0).getText());
            } else if (seqCtx.INCREMENT() != null && ints.size() >= 1) {
                incrementBy = Long.parseLong(ints.get(0).getText());
            }
            return new CreateSequenceStatement(name, startValue, incrementBy);
        } else if (ctx.dropSequence() != null) {
            return new DropSequenceStatement(ctx.dropSequence().sequenceName().getText());
        } else if (ctx.createFunction() != null) {
            return buildCreateFunction(ctx.createFunction());
        } else if (ctx.dropFunction() != null) {
            return new DropFunctionStatement(ctx.dropFunction().functionName().getText());
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

    private CreateFunctionStatement buildCreateFunction(StratosSQLParser.CreateFunctionContext ctx) {
        String name = ctx.functionName().getText();
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
        String name = ctx.functionName().getText();
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
        String indexName = ctx.indexName().getText();
        String tableName = ctx.tableName().getText();
        String columnName = ctx.columnName(0).getText();
        String columnName2 = ctx.columnName().size() > 1 ? ctx.columnName(1).getText() : null;
        CreateIndexStatement.IndexType indexType;
        if (ctx.HASH() != null) indexType = CreateIndexStatement.IndexType.HASH;
        else if (ctx.BRIN() != null) indexType = CreateIndexStatement.IndexType.BRIN;
        else if (ctx.GIN() != null) indexType = CreateIndexStatement.IndexType.GIN;
        else if (ctx.BITMAP() != null) indexType = CreateIndexStatement.IndexType.BITMAP;
        else if (ctx.GIST() != null) indexType = CreateIndexStatement.IndexType.GIST;
        else indexType = CreateIndexStatement.IndexType.BTREE; // default, matching Postgres's own convention
        return new CreateIndexStatement(indexName, tableName, columnName, columnName2, indexType);
    }

    /**
     * The grammar has always had a proper `update` rule (UPDATE ... SET ...
     * WHERE ...), but this method never existed and buildStatement() never
     * checked for it - so any UPDATE statement fell through every branch
     * above and hit the IllegalArgumentException at the bottom, regardless
     * of what the executor could or couldn't do with it.
     */
    private UpdateStatement buildUpdate(StratosSQLParser.UpdateContext ctx) {
        String tableName = ctx.tableName().getText();
        List<Assignment> assignments = new ArrayList<>();
        for (StratosSQLParser.AssignmentContext assignCtx : ctx.assignment()) {
            String column = assignCtx.columnName().getText();
            String value = assignCtx.literal().getText();
            assignments.add(new Assignment(column, value));
        }
        WhereExpr where = buildWhereExpr(ctx.expression());
        return new UpdateStatement(tableName, assignments, where);
    }

    private CreateTableStatement buildCreateTable(StratosSQLParser.CreateTableContext ctx) {
        String tableName = ctx.tableName().getText();
        List<ColumnDefinition> columns = new ArrayList<>();

        for (StratosSQLParser.ColumnDefContext colCtx : ctx.columnDef()) {
            String name = colCtx.columnName().getText();
            String type = colCtx.dataType().getText();
            // A second real "parsed but discarded" bug found alongside
            // INSERT's column-list one: NOT NULL and DEFAULT were both
            // hardcoded away here (false / null) regardless of what the
            // statement actually said, so neither ever reached the AST at
            // all - not even a matter of the executor ignoring them.
            boolean notNull = colCtx.NOT() != null;
            String defaultValue = colCtx.defaultValue() != null ? colCtx.defaultValue().getText() : null;
            columns.add(new ColumnDefinition(name, type, notNull, defaultValue));
        }

        return new CreateTableStatement(tableName, columns);
    }

    private InsertStatement buildInsert(StratosSQLParser.InsertContext ctx) {
        String tableName = ctx.tableName().getText();
        List<String> values = new ArrayList<>();

        if (ctx.valueList() != null) {
            for (StratosSQLParser.InsertValueContext valCtx : ctx.valueList().insertValue()) {
                if (valCtx.literal() != null) {
                    values.add(valCtx.literal().getText());
                } else {
                    // A nextval('seq') or currval('seq') call - stored as its
                    // own raw text (e.g. "nextval('t_id_seq')") for the
                    // executor to recognize and resolve at insert time,
                    // rather than trying to parse it as a literal.
                    values.add(valCtx.getText());
                }
            }
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
            columns.add(colCtx.getText());
        }

        return new InsertStatement(tableName, columns, values);
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
            return new WhereExpr.ColumnComparison(c.columnName(0).getText(), "=", c.columnName(1).getText());
        }
        if (ctx instanceof StratosSQLParser.GtColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(c.columnName(0).getText(), ">", c.columnName(1).getText());
        }
        if (ctx instanceof StratosSQLParser.LtColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(c.columnName(0).getText(), "<", c.columnName(1).getText());
        }
        if (ctx instanceof StratosSQLParser.GeColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(c.columnName(0).getText(), ">=", c.columnName(1).getText());
        }
        if (ctx instanceof StratosSQLParser.LeColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(c.columnName(0).getText(), "<=", c.columnName(1).getText());
        }
        if (ctx instanceof StratosSQLParser.NeColumnCompareContext c) {
            return new WhereExpr.ColumnComparison(c.columnName(0).getText(), "!=", c.columnName(1).getText());
        }
        if (ctx instanceof StratosSQLParser.EqCompareContext c) {
            return new WhereExpr.Comparison(c.columnName().getText(), "=", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.GtCompareContext c) {
            return new WhereExpr.Comparison(c.columnName().getText(), ">", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.LtCompareContext c) {
            return new WhereExpr.Comparison(c.columnName().getText(), "<", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.GeCompareContext c) {
            return new WhereExpr.Comparison(c.columnName().getText(), ">=", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.LeCompareContext c) {
            return new WhereExpr.Comparison(c.columnName().getText(), "<=", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.NeCompareContext c) {
            return new WhereExpr.Comparison(c.columnName().getText(), "!=", c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.LikeCompareContext c) {
            return new WhereExpr.Like(c.columnName().getText(), c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.ContainsCompareContext c) {
            return new WhereExpr.Contains(c.columnName().getText(), c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.ArrayContainsCompareContext c) {
            return new WhereExpr.ArrayContains(c.columnName().getText(), c.literal().getText());
        }
        if (ctx instanceof StratosSQLParser.JsonExtractTextEqCompareContext c) {
            return new WhereExpr.JsonExtractTextEquals(c.columnName().getText(), c.literal(0).getText(), c.literal(1).getText());
        }
        if (ctx instanceof StratosSQLParser.RangeOverlapsCompareContext c) {
            return new WhereExpr.RangeOverlaps(c.columnName(0).getText(), c.columnName(1).getText(), c.literal(0).getText(), c.literal(1).getText());
        }
        if (ctx instanceof StratosSQLParser.InListExprContext c) {
            return new WhereExpr.InList(c.columnName().getText(), buildValueList(c.valueList()), false);
        }
        if (ctx instanceof StratosSQLParser.NotInListExprContext c) {
            return new WhereExpr.InList(c.columnName().getText(), buildValueList(c.valueList()), true);
        }
        if (ctx instanceof StratosSQLParser.InSubqueryExprContext c) {
            return new WhereExpr.InSubquery(c.columnName().getText(), buildSelect(c.select()), false);
        }
        if (ctx instanceof StratosSQLParser.NotInSubqueryExprContext c) {
            return new WhereExpr.InSubquery(c.columnName().getText(), buildSelect(c.select()), true);
        }
        if (ctx instanceof StratosSQLParser.EqSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(c.columnName().getText(), "=", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.GtSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(c.columnName().getText(), ">", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.LtSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(c.columnName().getText(), "<", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.GeSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(c.columnName().getText(), ">=", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.LeSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(c.columnName().getText(), "<=", buildSelect(c.select()));
        }
        if (ctx instanceof StratosSQLParser.NeSubqueryExprContext c) {
            return new WhereExpr.ScalarSubqueryComparison(c.columnName().getText(), "!=", buildSelect(c.select()));
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
        String tableName = ctx.tableName().getText();
        List<String> columns = new ArrayList<>();
        List<AggregateCall> aggregates = new ArrayList<>();
        List<WindowFunctionCall> windowFunctions = new ArrayList<>();
        List<FunctionCallItem> functionCalls = new ArrayList<>();

        if (ctx.selectList().STAR() != null) {
            columns.add("*");
        } else {
            for (StratosSQLParser.SelectItemContext itemCtx : ctx.selectList().selectItem()) {
                String alias = itemCtx.alias() != null ? itemCtx.alias().getText() : null;
                if (itemCtx.windowFunction() != null) {
                    windowFunctions.add(buildWindowFunctionCall(itemCtx.windowFunction(), alias));
                } else if (itemCtx.aggregateFunction() != null) {
                    aggregates.add(buildAggregateCall(itemCtx.aggregateFunction(), alias));
                } else if (itemCtx.functionCall() != null) {
                    functionCalls.add(buildFunctionCallItem(itemCtx.functionCall(), alias));
                } else if (itemCtx.columnName() != null) {
                    // No alias support for plain columns yet (a separate, smaller gap than
                    // aggregate aliasing) - the requested column text is used as-is, same as before.
                    columns.add(itemCtx.columnName().getText());
                } else {
                    // The `expression (AS alias)?` selectItem alternative - not really meaningful
                    // for this engine's simple SELECT list (an expression like "age > 30" isn't a
                    // projectable column), kept only because the grammar allows it. Fall back to
                    // raw text rather than silently dropping it.
                    columns.add(itemCtx.getText());
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
                groupBy.add(colCtx.getText());
            }
        }

        String havingClause = ctx.havingClause() != null ? ctx.havingClause().getText() : null;
        WhereExpr where = buildWhereExpr(ctx.expression());
        String limit = ctx.limitValue() != null ? ctx.limitValue().getText() : null;

        return new SelectStatement(tableName, columns, where, null, limit, joins, aggregates, groupBy, havingClause, windowFunctions, functionCalls);
    }

    private FunctionCallItem buildFunctionCallItem(StratosSQLParser.FunctionCallContext ctx, String alias) {
        String functionName = ctx.functionName().getText();
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
                partitionBy.add(colCtx.getText());
            }
        }

        List<WindowOrderItem> orderBy = new ArrayList<>();
        if (ctx.orderList() != null) {
            for (StratosSQLParser.OrderItemContext itemCtx : ctx.orderList().orderItem()) {
                boolean descending = itemCtx.DESC() != null;
                orderBy.add(new WindowOrderItem(itemCtx.columnName().getText(), descending));
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

        String argument = ctx.STAR() != null ? "*" : ctx.columnName().getText();
        return new AggregateCall(function, argument, alias);
    }

    private JoinClause buildJoinClause(StratosSQLParser.JoinClauseContext ctx) {
        String tableName = ctx.tableName().getText();
        List<StratosSQLParser.ColumnNameContext> columns = ctx.columnName();
        String leftColumn = columns.get(0).getText();
        String rightColumn = columns.get(1).getText();
        return new JoinClause(tableName, leftColumn, rightColumn);
    }

    private DeleteStatement buildDelete(StratosSQLParser.DeleteContext ctx) {
        String tableName = ctx.tableName().getText();
        WhereExpr where = buildWhereExpr(ctx.expression());
        return new DeleteStatement(tableName, where);
    }

    private DropTableStatement buildDropTable(StratosSQLParser.DropTableContext ctx) {
        String tableName = ctx.tableName().getText();
        return new DropTableStatement(tableName);
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
}