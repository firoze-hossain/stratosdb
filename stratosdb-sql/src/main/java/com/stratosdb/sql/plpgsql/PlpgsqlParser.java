package com.stratosdb.sql.plpgsql;

import com.stratosdb.sql.parser.StratosSQLLexer;
import com.stratosdb.sql.parser.StratosSQLParser;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses a procedural ("LANGUAGE plpgsql") function/procedure body's own
 * raw text (the dollar-quoted string's own contents, $$ delimiters already
 * stripped - see CreateFunctionStatement's/CreateProcedureStatement's own
 * javadoc) into a real PlpgsqlBlock AST, using this SAME engine's own
 * StratosSQLLexer/StratosSQLParser classes - a genuinely separate, second
 * parse entry point (plpgsqlBlock(), not sqlStatement()) on the identical
 * lexer/token vocabulary, not a second, separate parser implementation.
 */
public class PlpgsqlParser {

    public PlpgsqlBlock parse(String body) {
        CharStream charStream = CharStreams.fromString(body);
        StratosSQLLexer lexer = new StratosSQLLexer(charStream);
        lexer.removeErrorListeners();
        lexer.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new RuntimeException("Syntax error in procedural body at line " + line + ":" + charPositionInLine + " - " + msg);
            }
        });
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        StratosSQLParser parser = new StratosSQLParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new RuntimeException("Syntax error in procedural body at line " + line + ":" + charPositionInLine + " - " + msg);
            }
        });

        StratosSQLParser.PlpgsqlBlockContext ctx = parser.plpgsqlBlock();
        return buildBlock(ctx, charStream);
    }

    private PlpgsqlBlock buildBlock(StratosSQLParser.PlpgsqlBlockContext ctx, CharStream charStream) {
        List<PlpgsqlBlock.PlpgsqlVarDecl> declarations = new ArrayList<>();
        if (ctx.plpgsqlDeclareSection() != null) {
            for (StratosSQLParser.PlpgsqlVarDeclContext declCtx : ctx.plpgsqlDeclareSection().plpgsqlVarDecl()) {
                String name = declCtx.IDENTIFIER().getText();
                String dataType = declCtx.dataType().getText();
                PlpgsqlExpr initial = declCtx.plpgsqlExpr() != null ? buildExpr(declCtx.plpgsqlExpr(), charStream) : null;
                declarations.add(new PlpgsqlBlock.PlpgsqlVarDecl(name, dataType, initial));
            }
        }
        List<PlpgsqlStmt> statements = buildStatements(ctx.plpgsqlStatement(), charStream);
        return new PlpgsqlBlock(declarations, statements);
    }

    private List<PlpgsqlStmt> buildStatements(List<StratosSQLParser.PlpgsqlStatementContext> stmtCtxList, CharStream charStream) {
        List<PlpgsqlStmt> statements = new ArrayList<>();
        for (StratosSQLParser.PlpgsqlStatementContext stmtCtx : stmtCtxList) {
            statements.add(buildStatement(stmtCtx, charStream));
        }
        return statements;
    }

    private PlpgsqlStmt buildStatement(StratosSQLParser.PlpgsqlStatementContext ctx, CharStream charStream) {
        if (ctx.plpgsqlAssignment() != null) {
            var a = ctx.plpgsqlAssignment();
            return new PlpgsqlStmt.Assignment(a.IDENTIFIER().getText(), buildExpr(a.plpgsqlExpr(), charStream));
        }
        if (ctx.plpgsqlIfStatement() != null) {
            var i = ctx.plpgsqlIfStatement();
            List<PlpgsqlStmt.IfBranch> branches = new ArrayList<>();
            // The IF branch itself is plpgsqlExpr(0)/plpgsqlStatement grouping 0;
            // each ELSIF is a subsequent plpgsqlExpr/statement grouping. ANTLR4's
            // own generated context exposes all plpgsqlExpr() calls (IF + every
            // ELSIF) as one flat, ordered list, and all plpgsqlStatement() calls
            // similarly - so this reconstructs each branch's own statements by
            // real position, using each ELSIF's own real statement COUNT (not
            // just splitting evenly) to find each branch's own real boundary.
            List<StratosSQLParser.PlpgsqlExprContext> allConditions = i.plpgsqlExpr();
            List<StratosSQLParser.PlpgsqlStatementContext> allStmts = i.plpgsqlStatement();
            int[] branchBoundaries = computeIfBranchBoundaries(i, allStmts);
            int stmtIndex = 0;
            for (int b = 0; b < allConditions.size(); b++) {
                int count = branchBoundaries[b];
                List<PlpgsqlStmt> body = buildStatements(allStmts.subList(stmtIndex, stmtIndex + count), charStream);
                branches.add(new PlpgsqlStmt.IfBranch(buildExpr(allConditions.get(b), charStream), body));
                stmtIndex += count;
            }
            List<PlpgsqlStmt> elseBody = buildStatements(allStmts.subList(stmtIndex, allStmts.size()), charStream);
            return new PlpgsqlStmt.If(branches, elseBody);
        }
        if (ctx.plpgsqlWhileStatement() != null) {
            var w = ctx.plpgsqlWhileStatement();
            return new PlpgsqlStmt.While(buildExpr(w.plpgsqlExpr(), charStream), buildStatements(w.plpgsqlStatement(), charStream));
        }
        if (ctx.plpgsqlForRangeStatement() != null) {
            var f = ctx.plpgsqlForRangeStatement();
            return new PlpgsqlStmt.ForRange(f.IDENTIFIER().getText(), buildExpr(f.plpgsqlExpr(0), charStream),
                buildExpr(f.plpgsqlExpr(1), charStream), buildStatements(f.plpgsqlStatement(), charStream));
        }
        if (ctx.plpgsqlLoopStatement() != null) {
            var l = ctx.plpgsqlLoopStatement();
            return new PlpgsqlStmt.Loop(buildStatements(l.plpgsqlStatement(), charStream));
        }
        if (ctx.plpgsqlExitStatement() != null) {
            var e = ctx.plpgsqlExitStatement();
            return new PlpgsqlStmt.Exit(e.plpgsqlExpr() != null ? buildExpr(e.plpgsqlExpr(), charStream) : null);
        }
        if (ctx.plpgsqlContinueStatement() != null) {
            var c = ctx.plpgsqlContinueStatement();
            return new PlpgsqlStmt.Continue(c.plpgsqlExpr() != null ? buildExpr(c.plpgsqlExpr(), charStream) : null);
        }
        if (ctx.plpgsqlReturnStatement() != null) {
            var r = ctx.plpgsqlReturnStatement();
            return new PlpgsqlStmt.Return(r.plpgsqlExpr() != null ? buildExpr(r.plpgsqlExpr(), charStream) : null);
        }
        if (ctx.plpgsqlRaiseStatement() != null) {
            var raise = ctx.plpgsqlRaiseStatement();
            String level = raise.PLSQL_EXCEPTION() != null ? "EXCEPTION" : raise.PLSQL_WARNING() != null ? "WARNING" : "NOTICE";
            String message = unquote(raise.STRING_LITERAL().getText());
            return new PlpgsqlStmt.Raise(level, message);
        }
        // plpgsqlEmbeddedSql: everything else, captured as its own real, exact
        // source text (via the char stream's own interval, NOT getText() token
        // concatenation, which would lose all original whitespace).
        var sql = ctx.plpgsqlEmbeddedSql();
        int start = sql.plpgsqlToken(0).getStart().getStartIndex();
        int stop = sql.plpgsqlToken(sql.plpgsqlToken().size() - 1).getStop().getStopIndex();
        String sqlText = charStream.getText(Interval.of(start, stop));
        return new PlpgsqlStmt.EmbeddedSql(sqlText);
    }

    /**
     * ANTLR4's own generated IfStatementContext flattens every ELSIF's own
     * condition/body into the SAME plpgsqlExpr()/plpgsqlStatement() lists the
     * IF branch itself uses - there is no separate, per-ELSIF sub-context to
     * walk. Real branch boundaries are instead recovered by walking each
     * ELSIF/ELSE/END token's own real source position and counting how many
     * of the flattened statement contexts fall strictly before it.
     */
    private int[] computeIfBranchBoundaries(StratosSQLParser.PlpgsqlIfStatementContext ifCtx, List<StratosSQLParser.PlpgsqlStatementContext> allStmts) {
        List<Integer> boundaryStarts = new ArrayList<>();
        for (var elsif : ifCtx.PLSQL_ELSIF()) {
            boundaryStarts.add(elsif.getSymbol().getStartIndex());
        }
        if (ifCtx.PLSQL_ELSE() != null) {
            boundaryStarts.add(ifCtx.PLSQL_ELSE().getSymbol().getStartIndex());
        } else {
            boundaryStarts.add(ifCtx.PLSQL_END().getSymbol().getStartIndex());
        }
        int numBranches = ifCtx.plpgsqlExpr().size();
        int[] counts = new int[numBranches];
        int stmtCursor = 0;
        for (int b = 0; b < numBranches; b++) {
            int boundary = boundaryStarts.get(b);
            int count = 0;
            while (stmtCursor + count < allStmts.size() && allStmts.get(stmtCursor + count).getStart().getStartIndex() < boundary) {
                count++;
            }
            counts[b] = count;
            stmtCursor += count;
        }
        return counts;
    }

    private PlpgsqlExpr buildExpr(StratosSQLParser.PlpgsqlExprContext ctx, CharStream charStream) {
        if (ctx instanceof StratosSQLParser.PlpgsqlMulDivContext c) {
            return new PlpgsqlExpr.Binary(c.op.getText(), buildExpr(c.plpgsqlExpr(0), charStream), buildExpr(c.plpgsqlExpr(1), charStream));
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlAddSubContext c) {
            return new PlpgsqlExpr.Binary(c.op.getText(), buildExpr(c.plpgsqlExpr(0), charStream), buildExpr(c.plpgsqlExpr(1), charStream));
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlCompareContext c) {
            return new PlpgsqlExpr.Binary(c.op.getText(), buildExpr(c.plpgsqlExpr(0), charStream), buildExpr(c.plpgsqlExpr(1), charStream));
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlNotContext c) {
            return new PlpgsqlExpr.Unary("NOT", buildExpr(c.plpgsqlExpr(), charStream));
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlAndContext c) {
            return new PlpgsqlExpr.Binary("AND", buildExpr(c.plpgsqlExpr(0), charStream), buildExpr(c.plpgsqlExpr(1), charStream));
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlOrContext c) {
            return new PlpgsqlExpr.Binary("OR", buildExpr(c.plpgsqlExpr(0), charStream), buildExpr(c.plpgsqlExpr(1), charStream));
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlNegateContext c) {
            return new PlpgsqlExpr.Unary("NEG", buildExpr(c.plpgsqlExpr(), charStream));
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlParenContext c) {
            return buildExpr(c.plpgsqlExpr(), charStream);
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlFunctionCallContext c) {
            List<PlpgsqlExpr> args = new ArrayList<>();
            for (var argCtx : c.plpgsqlExpr()) {
                args.add(buildExpr(argCtx, charStream));
            }
            return new PlpgsqlExpr.FunctionCall(c.IDENTIFIER().getText(), args);
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlVariableContext c) {
            return new PlpgsqlExpr.Variable(c.IDENTIFIER().getText());
        }
        if (ctx instanceof StratosSQLParser.PlpgsqlLiteralExprContext c) {
            return new PlpgsqlExpr.Literal(parseLiteral(c.literal().getText()));
        }
        throw new IllegalStateException("Unrecognized plpgsql expression: " + ctx.getText());
    }

    private static String unquote(String stringLiteral) {
        String inner = stringLiteral.substring(1, stringLiteral.length() - 1);
        return inner.replace("''", "'");
    }

    /** A small, deliberately duplicated copy of ExecutorEngine's own parseLiteral - kept separate rather than widening that method's own visibility, since this package has no other real dependency on ExecutorEngine at all. */
    private static Object parseLiteral(String value) {
        if (value.startsWith("'") && value.endsWith("'")) {
            return unquote(value);
        }
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        }
        if (value.equalsIgnoreCase("null")) {
            return null;
        }
        try {
            if (value.contains(".")) {
                return Double.parseDouble(value);
            }
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return value;
        }
    }
}
