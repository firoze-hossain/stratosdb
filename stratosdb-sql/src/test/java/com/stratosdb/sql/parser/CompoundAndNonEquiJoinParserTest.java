package com.stratosdb.sql.parser;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.ast.SelectStatement;
import com.stratosdb.sql.ast.WhereExpr;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CompoundAndNonEquiJoinParserTest {

    private final SqlParser parser = new SqlParser();

    @Test
    @DisplayName("Parse single equi-join condition")
    void testSingleEquiJoinParsing() {
        SelectStatement stmt = (SelectStatement) parser.parse("SELECT * FROM users u JOIN orders o ON u.id = o.user_id;");
        assertNotNull(stmt);
        assertEquals(1, stmt.joins().size());

        JoinClause join = stmt.joins().get(0);
        assertEquals("orders", join.tableName());
        assertEquals("o", join.tableAlias());
        assertEquals("u.id", join.leftColumn());
        assertEquals("o.user_id", join.rightColumn());
        assertEquals(1, join.leftColumns().size());
        assertEquals(1, join.rightColumns().size());
        assertFalse(join.hasCompoundPredicates());
        assertFalse(join.hasNonEquiPredicates());
    }

    @Test
    @DisplayName("Parse compound multi-column equi-join (ON a.id = b.id AND a.status = b.status)")
    void testCompoundEquiJoinParsing() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT * FROM accounts a JOIN balances b ON a.id = b.id AND a.status = b.status;");
        assertNotNull(stmt);
        assertEquals(1, stmt.joins().size());

        JoinClause join = stmt.joins().get(0);
        assertEquals("balances", join.tableName());
        assertEquals("b", join.tableAlias());
        assertEquals(2, join.leftColumns().size());
        assertEquals(2, join.rightColumns().size());
        assertTrue(join.leftColumns().contains("a.id"));
        assertTrue(join.leftColumns().contains("a.status"));
        assertTrue(join.rightColumns().contains("b.id"));
        assertTrue(join.rightColumns().contains("b.status"));
        assertTrue(join.hasCompoundPredicates());
        assertFalse(join.hasNonEquiPredicates());
        assertInstanceOf(WhereExpr.And.class, join.onExpression());
    }

    @Test
    @DisplayName("Parse 3-column composite key join")
    void testThreeColumnCompositeKeyJoin() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT * FROM t1 JOIN t2 ON t1.c1 = t2.c1 AND t1.c2 = t2.c2 AND t1.c3 = t2.c3;");
        assertNotNull(stmt);
        assertEquals(1, stmt.joins().size());

        JoinClause join = stmt.joins().get(0);
        assertEquals(3, join.leftColumns().size());
        assertEquals(3, join.rightColumns().size());
        assertEquals("t1.c1", join.leftColumns().get(0));
        assertEquals("t2.c1", join.rightColumns().get(0));
        assertEquals("t1.c2", join.leftColumns().get(1));
        assertEquals("t2.c2", join.rightColumns().get(1));
        assertEquals("t1.c3", join.leftColumns().get(2));
        assertEquals("t2.c3", join.rightColumns().get(2));
        assertTrue(join.hasCompoundPredicates());
        assertFalse(join.hasNonEquiPredicates());
    }

    @Test
    @DisplayName("Parse compound join with non-equi comparison (ON a.id = b.id AND a.amount >= b.min_amount)")
    void testCompoundJoinWithNonEquiPredicate() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT * FROM accounts a JOIN tiers b ON a.id = b.id AND a.amount >= b.min_amount;");
        assertNotNull(stmt);
        assertEquals(1, stmt.joins().size());

        JoinClause join = stmt.joins().get(0);
        assertEquals(1, join.leftColumns().size());
        assertEquals(1, join.rightColumns().size());
        assertEquals("a.id", join.leftColumn());
        assertEquals("b.id", join.rightColumn());
        assertTrue(join.hasCompoundPredicates());
        assertTrue(join.hasNonEquiPredicates());
        assertNotNull(join.residualFilter());
        assertInstanceOf(WhereExpr.ColumnComparison.class, join.residualFilter());
        WhereExpr.ColumnComparison cmp = (WhereExpr.ColumnComparison) join.residualFilter();
        assertEquals(">=", cmp.operator());
        assertEquals("a.amount", cmp.leftColumn());
        assertEquals("b.min_amount", cmp.rightColumn());
    }

    @Test
    @DisplayName("Parse pure non-equi join (ON a.val > b.val)")
    void testPureNonEquiJoinParsing() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT * FROM left_table l JOIN right_table r ON l.val > r.val;");
        assertNotNull(stmt);
        assertEquals(1, stmt.joins().size());

        JoinClause join = stmt.joins().get(0);
        assertTrue(join.leftColumns().isEmpty());
        assertTrue(join.rightColumns().isEmpty());
        assertNotNull(join.onExpression());
        assertNotNull(join.residualFilter());
        assertTrue(join.hasNonEquiPredicates());
        assertInstanceOf(WhereExpr.ColumnComparison.class, join.residualFilter());
        assertEquals(">", ((WhereExpr.ColumnComparison) join.residualFilter()).operator());
    }

    @Test
    @DisplayName("Parse parenthesized compound join condition")
    void testParenthesizedCompoundJoin() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT * FROM t1 JOIN t2 ON (t1.x = t2.x AND t1.y = t2.y);");
        assertNotNull(stmt);
        assertEquals(1, stmt.joins().size());

        JoinClause join = stmt.joins().get(0);
        assertEquals(2, join.leftColumns().size());
        assertEquals(2, join.rightColumns().size());
        assertEquals("t1.x", join.leftColumns().get(0));
        assertEquals("t2.x", join.rightColumns().get(0));
    }

    @Test
    @DisplayName("Parse CROSS JOIN without ON clause")
    void testCrossJoinWithoutOnClause() {
        SelectStatement stmt = (SelectStatement) parser.parse("SELECT * FROM t1 CROSS JOIN t2;");
        assertNotNull(stmt);
        assertEquals(1, stmt.joins().size());

        JoinClause join = stmt.joins().get(0);
        assertEquals(JoinClause.JoinType.CROSS, join.joinType());
        assertNull(join.onExpression());
        assertTrue(join.leftColumns().isEmpty());
        assertTrue(join.rightColumns().isEmpty());
    }

    @Test
    @DisplayName("Parse join with inverted operand order (ON b.id = a.id AND b.status = a.status)")
    void testRightTableOnLeftOfEqualsSign() {
        SelectStatement stmt = (SelectStatement) parser.parse(
            "SELECT * FROM users u JOIN orders o ON o.user_id = u.id AND o.status = u.status;");
        assertNotNull(stmt);
        assertEquals(1, stmt.joins().size());

        JoinClause join = stmt.joins().get(0);
        // Correctly maps joined table (o) to rightColumns and outer table (u) to leftColumns
        assertEquals("u.id", join.leftColumns().get(0));
        assertEquals("o.user_id", join.rightColumns().get(0));
        assertEquals("u.status", join.leftColumns().get(1));
        assertEquals("o.status", join.rightColumns().get(1));
    }
}
