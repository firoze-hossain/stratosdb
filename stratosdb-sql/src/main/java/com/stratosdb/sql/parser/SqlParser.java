package com.stratosdb.sql.parser;

import com.stratosdb.sql.ast.*;
import org.antlr.v4.runtime.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SqlParser {
    private static final Logger LOG = LoggerFactory.getLogger(SqlParser.class);

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
        } else if (ctx.showTables() != null) {
            return new ShowTablesStatement();
        } else if (ctx.showStats() != null) {
            return new ShowStatsStatement();
        } else if (ctx.explain() != null) {
            return new ExplainStatement(buildSelect(ctx.explain().select()));
        } else if (ctx.analyze() != null) {
            return new AnalyzeStatement(ctx.analyze().tableName().getText());
        } else if (ctx.vacuum() != null) {
            return new VacuumStatement(ctx.vacuum().tableName().getText());
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
            SelectStatement cteQuery = buildSelect(cteCtx.select(0));
            SelectStatement outerQuery = buildSelect(cteCtx.select(1));
            return new CteSelectStatement(cteName, cteQuery, outerQuery);
        }
        throw new IllegalArgumentException("Unsupported SQL statement");
    }

    private CreateIndexStatement buildCreateIndex(StratosSQLParser.CreateIndexContext ctx) {
        String indexName = ctx.indexName().getText();
        String tableName = ctx.tableName().getText();
        String columnName = ctx.columnName().getText();
        CreateIndexStatement.IndexType indexType = ctx.HASH() != null
            ? CreateIndexStatement.IndexType.HASH
            : CreateIndexStatement.IndexType.BTREE; // default, matching Postgres's own convention
        return new CreateIndexStatement(indexName, tableName, columnName, indexType);
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
            columns.add(new ColumnDefinition(name, type, false, null));
        }

        return new CreateTableStatement(tableName, columns);
    }

    private InsertStatement buildInsert(StratosSQLParser.InsertContext ctx) {
        String tableName = ctx.tableName().getText();
        List<String> values = new ArrayList<>();

        if (ctx.valueList() != null) {
            for (StratosSQLParser.LiteralContext litCtx : ctx.valueList().literal()) {
                values.add(litCtx.getText());
            }
        }

        return new InsertStatement(tableName, values);
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
        for (StratosSQLParser.LiteralContext lit : ctx.literal()) {
            values.add(lit.getText());
        }
        return values;
    }

    private SelectStatement buildSelect(StratosSQLParser.SelectContext ctx) {
        String tableName = ctx.tableName().getText();
        List<String> columns = new ArrayList<>();
        List<AggregateCall> aggregates = new ArrayList<>();

        if (ctx.selectList().STAR() != null) {
            columns.add("*");
        } else {
            for (StratosSQLParser.SelectItemContext itemCtx : ctx.selectList().selectItem()) {
                String alias = itemCtx.alias() != null ? itemCtx.alias().getText() : null;
                if (itemCtx.aggregateFunction() != null) {
                    aggregates.add(buildAggregateCall(itemCtx.aggregateFunction(), alias));
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

        return new SelectStatement(tableName, columns, where, null, limit, joins, aggregates, groupBy, havingClause);
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
}