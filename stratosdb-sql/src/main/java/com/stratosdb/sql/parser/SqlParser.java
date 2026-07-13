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
        } else if (ctx.insert() != null) {
            return buildInsert(ctx.insert());
        } else if (ctx.select() != null) {
            return buildSelect(ctx.select());
        } else if (ctx.delete() != null) {
            return buildDelete(ctx.delete());
        } else if (ctx.dropTable() != null) {
            return buildDropTable(ctx.dropTable());
        } else if (ctx.showTables() != null) {
            return new ShowTablesStatement();
        }
        throw new IllegalArgumentException("Unsupported SQL statement");
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

    private SelectStatement buildSelect(StratosSQLParser.SelectContext ctx) {
        String tableName = ctx.tableName().getText();
        List<String> columns = new ArrayList<>();

        // Check if it's SELECT *
        if (ctx.selectList().STAR() != null) {
            columns.add("*");
        } else {
            // Get all children and extract column names
            // Alternative approach: get the raw text and parse
            String selectText = ctx.selectList().getText();
            if (selectText != null && !selectText.isEmpty()) {
                // Remove parentheses and split by comma
                String[] parts = selectText.split(",");
                for (String part : parts) {
                    columns.add(part.trim());
                }
            }
        }

        String whereClause = ctx.expression() != null ? ctx.expression().getText() : null;
        String limit = ctx.limitValue() != null ? ctx.limitValue().getText() : null;

        return new SelectStatement(tableName, columns, whereClause, null, limit);
    }

    private DeleteStatement buildDelete(StratosSQLParser.DeleteContext ctx) {
        String tableName = ctx.tableName().getText();
        String whereClause = ctx.expression() != null ? ctx.expression().getText() : null;
        return new DeleteStatement(tableName, whereClause);
    }

    private DropTableStatement buildDropTable(StratosSQLParser.DropTableContext ctx) {
        String tableName = ctx.tableName().getText();
        return new DropTableStatement(tableName);
    }
}