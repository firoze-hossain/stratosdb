package com.stratosdb.sql.parser;

import com.stratosdb.sql.ast.AggregateCall;
import com.stratosdb.sql.ast.SelectStatement;
import com.stratosdb.sql.ast.WhereExpr;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ArbitraryHavingParserTest {

    private final SqlParser parser = new SqlParser();

    @Test
    @DisplayName("Parse HAVING with single aggregate comparison (COUNT(*) > 1)")
    void testSingleAggregateHaving() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 1;");
        assertNotNull(stmt);
        assertNotNull(stmt.havingExpr());
        assertInstanceOf(WhereExpr.Comparison.class, stmt.havingExpr());

        WhereExpr.Comparison cmp = (WhereExpr.Comparison) stmt.havingExpr();
        assertEquals("COUNT(*)", cmp.column());
        assertEquals(">", cmp.operator());
        assertEquals("1", cmp.literal());
    }

    @Test
    @DisplayName("Parse HAVING with compound AND condition (HAVING COUNT(*) > 1 AND AVG(price) < 50)")
    void testCompoundAndHaving() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category HAVING COUNT(*) > 1 AND AVG(price) < 50;");
        assertNotNull(stmt);
        assertNotNull(stmt.havingExpr());
        assertInstanceOf(WhereExpr.And.class, stmt.havingExpr());

        WhereExpr.And and = (WhereExpr.And) stmt.havingExpr();
        assertInstanceOf(WhereExpr.Comparison.class, and.left());
        assertInstanceOf(WhereExpr.Comparison.class, and.right());

        WhereExpr.Comparison left = (WhereExpr.Comparison) and.left();
        assertEquals("COUNT(*)", left.column());
        assertEquals(">", left.operator());
        assertEquals("1", left.literal());

        WhereExpr.Comparison right = (WhereExpr.Comparison) and.right();
        assertEquals("AVG(price)", right.column());
        assertEquals("<", right.operator());
        assertEquals("50", right.literal());
    }

    @Test
    @DisplayName("Parse HAVING with aggregates not in SELECT list")
    void testHavingAggregatesCollectedWhenNotInSelect() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT category FROM products GROUP BY category HAVING COUNT(*) > 1 AND AVG(price) < 50;");
        assertNotNull(stmt);
        assertEquals(0, stmt.aggregates().size(), "SELECT list contains no aggregates");
        assertEquals(2, stmt.havingAggregates().size(), "HAVING aggregates should be collected");
        assertEquals(2, stmt.allAggregates().size(), "allAggregates includes both SELECT and HAVING aggregates");

        boolean hasCount = stmt.allAggregates().stream().anyMatch(a -> a.canonicalForm().equals("COUNT(*)"));
        boolean hasAvg = stmt.allAggregates().stream().anyMatch(a -> a.canonicalForm().equals("AVG(price)"));
        assertTrue(hasCount);
        assertTrue(hasAvg);
    }

    @Test
    @DisplayName("Parse HAVING with OR and NOT conditions")
    void testHavingOrAndNot() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT dept FROM emp GROUP BY dept HAVING (COUNT(*) >= 5 OR SUM(salary) > 50000) AND NOT MIN(salary) < 1000;");
        assertNotNull(stmt);
        assertNotNull(stmt.havingExpr());
        assertInstanceOf(WhereExpr.And.class, stmt.havingExpr());

        WhereExpr.And and = (WhereExpr.And) stmt.havingExpr();
        assertInstanceOf(WhereExpr.Or.class, and.left());
        assertInstanceOf(WhereExpr.Not.class, and.right());

        WhereExpr.Or or = (WhereExpr.Or) and.left();
        WhereExpr.Comparison countCmp = (WhereExpr.Comparison) or.left();
        assertEquals("COUNT(*)", countCmp.column());
        assertEquals(">=", countCmp.operator());
        assertEquals("5", countCmp.literal());

        WhereExpr.Comparison sumCmp = (WhereExpr.Comparison) or.right();
        assertEquals("SUM(salary)", sumCmp.column());
        assertEquals(">", sumCmp.operator());
        assertEquals("50000", sumCmp.literal());

        WhereExpr.Not not = (WhereExpr.Not) and.right();
        WhereExpr.Comparison minCmp = (WhereExpr.Comparison) not.inner();
        assertEquals("MIN(salary)", minCmp.column());
        assertEquals("<", minCmp.operator());
        assertEquals("1000", minCmp.literal());
    }

    @Test
    @DisplayName("Parse HAVING comparing aggregate to aggregate (MAX(val) > MIN(val))")
    void testHavingAggregateToAggregateComparison() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT team FROM scores GROUP BY team HAVING MAX(score) > MIN(score);");
        assertNotNull(stmt);
        assertNotNull(stmt.havingExpr());
        assertInstanceOf(WhereExpr.ColumnComparison.class, stmt.havingExpr());

        WhereExpr.ColumnComparison cmp = (WhereExpr.ColumnComparison) stmt.havingExpr();
        assertEquals("MAX(score)", cmp.leftColumn());
        assertEquals(">", cmp.operator());
        assertEquals("MIN(score)", cmp.rightColumn());
    }

    @Test
    @DisplayName("Parse HAVING referencing group column and aggregate")
    void testHavingColumnAndAggregate() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING dept = 'Engineering' AND COUNT(*) > 10;");
        assertNotNull(stmt);
        assertNotNull(stmt.havingExpr());
        assertInstanceOf(WhereExpr.And.class, stmt.havingExpr());

        WhereExpr.And and = (WhereExpr.And) stmt.havingExpr();
        WhereExpr.Comparison colCmp = (WhereExpr.Comparison) and.left();
        assertEquals("dept", colCmp.column());
        assertEquals("=", colCmp.operator());
        assertEquals("'Engineering'", colCmp.literal());

        WhereExpr.Comparison aggCmp = (WhereExpr.Comparison) and.right();
        assertEquals("COUNT(*)", aggCmp.column());
        assertEquals(">", aggCmp.operator());
        assertEquals("10", aggCmp.literal());
    }

    @Test
    @DisplayName("Parse HAVING with inverted literal comparison (50 > AVG(price))")
    void testHavingInvertedLiteralComparison() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT dept FROM emp GROUP BY dept HAVING 50 > AVG(price);");
        assertNotNull(stmt);
        assertNotNull(stmt.havingExpr());
        assertInstanceOf(WhereExpr.Comparison.class, stmt.havingExpr());

        WhereExpr.Comparison cmp = (WhereExpr.Comparison) stmt.havingExpr();
        assertEquals("AVG(price)", cmp.column());
        assertEquals("<", cmp.operator());
        assertEquals("50", cmp.literal());
    }
}
