package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.sql.execution.RecursiveCteOperator;
import com.stratosdb.sql.execution.TupleSlot;
import com.stratosdb.sql.executor.QueryResult;
import com.stratosdb.storage.page.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Sprint 07 Week 14: Recursive CTEs (WITH RECURSIVE).
 * Verifies non-recursive term evaluation, worktable queue, fixed-point termination,
 * UNION deduplication, CYCLE clause path detection, and hierarchical/graph queries.
 */
public class RecursiveCteEngineTest {

    @TempDir
    Path tempDir;

    private StratosDB db;

    @BeforeEach
    void setUp() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataDirectory(tempDir.toString());
        config.setBufferPoolSize(64);
        db = new StratosDB(config);
    }

    @AfterEach
    void tearDown() {
        if (db != null) {
            db.shutdown();
        }
    }

    @Test
    void testHierarchicalOrgChartMultiLevel() {
        db.execute("CREATE TABLE org (id INT, name VARCHAR, mgr_id INT)");
        db.execute("INSERT INTO org VALUES (1, 'CEO', 0)");
        db.execute("INSERT INTO org VALUES (2, 'VP_Eng', 1)");
        db.execute("INSERT INTO org VALUES (3, 'VP_Sales', 1)");
        db.execute("INSERT INTO org VALUES (4, 'Dir_DB', 2)");
        db.execute("INSERT INTO org VALUES (5, 'Mgr_SQL', 4)");
        db.execute("INSERT INTO org VALUES (6, 'IC_1', 5)");
        db.execute("INSERT INTO org VALUES (7, 'IC_2', 5)");

        QueryResult res = db.execute(
            "WITH RECURSIVE tree AS ("
            + "SELECT id, name, mgr_id FROM org WHERE id = 2 "
            + "UNION ALL "
            + "SELECT org.id, org.name, org.mgr_id FROM org JOIN tree ON org.mgr_id = tree.id"
            + ") SELECT id, name FROM tree ORDER BY id ASC");

        assertTrue(res.isSuccess(), res.getError());
        // Subtree under VP_Eng (id=2): {2, 4, 5, 6, 7}
        assertEquals(5, res.getRows().size());
        assertEquals(List.of(2, 4, 5, 6, 7),
            res.getRows().stream().map(r -> (Integer) r.getValue("id")).toList());
    }

    @Test
    void testGraphReachabilityWithUnionDeduplicationOnCyclicGraph() {
        db.execute("CREATE TABLE edges (src INT, dst INT)");
        db.execute("INSERT INTO edges VALUES (1, 2)");
        db.execute("INSERT INTO edges VALUES (2, 3)");
        db.execute("INSERT INTO edges VALUES (3, 1)"); // cycle 1 -> 2 -> 3 -> 1
        db.execute("INSERT INTO edges VALUES (2, 4)");
        db.execute("INSERT INTO edges VALUES (4, 5)");
        db.execute("INSERT INTO edges VALUES (5, 2)"); // cycle 2 -> 4 -> 5 -> 2

        QueryResult res = db.execute(
            "WITH RECURSIVE reach AS ("
            + "SELECT dst FROM edges WHERE src = 1 "
            + "UNION "
            + "SELECT edges.dst FROM edges JOIN reach ON edges.src = reach.dst"
            + ") SELECT dst FROM reach ORDER BY dst ASC");

        assertTrue(res.isSuccess(), res.getError());
        assertEquals(List.of(1, 2, 3, 4, 5),
            res.getRows().stream().map(r -> (Integer) r.getValue("dst")).toList());
    }

    @Test
    void testGraphPathTraversalWithCycleClauseDetection() {
        db.execute("CREATE TABLE graph (node_from INT, node_to INT)");
        db.execute("INSERT INTO graph VALUES (1, 2)");
        db.execute("INSERT INTO graph VALUES (2, 3)");
        db.execute("INSERT INTO graph VALUES (3, 1)"); // cycle back to 1
        db.execute("INSERT INTO graph VALUES (2, 4)"); // acyclic branch to 4

        QueryResult res = db.execute(
            "WITH RECURSIVE paths AS ("
            + "SELECT node_from, node_to FROM graph WHERE node_from = 1 "
            + "UNION ALL "
            + "SELECT graph.node_from, graph.node_to FROM graph JOIN paths ON graph.node_from = paths.node_to"
            + ") CYCLE node_to SET is_cycle USING path "
            + "SELECT node_from, node_to, is_cycle, path FROM paths");

        assertTrue(res.isSuccess(), res.getError());
        // Paths from 1: (1,2) [false, "2"], (2,3) [false, "2->3"], (2,4) [false, "2->4"], (3,1) [false, "2->3->1"], (1,2) [true, "2->3->1->2"]
        boolean foundCycle = false;
        Set<String> pathsSeen = new HashSet<>();
        for (Tuple r : res.getRows()) {
            Boolean isCycle = (Boolean) r.getValue("is_cycle");
            String path = (String) r.getValue("path");
            assertNotNull(isCycle);
            assertNotNull(path);
            pathsSeen.add(path);
            if (Boolean.TRUE.equals(isCycle)) {
                foundCycle = true;
                assertEquals("2->3->1->2", path);
            }
        }
        assertTrue(foundCycle, "CYCLE clause must detect cycle back to node 2");
        assertTrue(pathsSeen.contains("2->4"));
    }

    @Test
    void testDiamondDagGraphDoesNotFalsePositiveCycle() {
        db.execute("CREATE TABLE dag (src INT, dst INT)");
        db.execute("INSERT INTO dag VALUES (1, 2)");
        db.execute("INSERT INTO dag VALUES (1, 3)");
        db.execute("INSERT INTO dag VALUES (2, 4)");
        db.execute("INSERT INTO dag VALUES (3, 4)");

        QueryResult res = db.execute(
            "WITH RECURSIVE paths AS ("
            + "SELECT src, dst FROM dag WHERE src = 1 "
            + "UNION ALL "
            + "SELECT dag.src, dag.dst FROM dag JOIN paths ON dag.src = paths.dst"
            + ") CYCLE dst SET is_cycle USING path "
            + "SELECT dst, is_cycle, path FROM paths");

        assertTrue(res.isSuccess(), res.getError());
        assertEquals(4, res.getRows().size());
        for (Tuple r : res.getRows()) {
            assertEquals(Boolean.FALSE, r.getValue("is_cycle"),
                "Diamond DAG has no cycle, path=" + r.getValue("path"));
        }
    }

    @Test
    void testDirectRecursiveCteOperatorWorktableQueue() {
        List<String> cols = List.of("n");
        RecursiveCteOperator op = new RecursiveCteOperator(
            "counter",
            () -> List.of(new Tuple(List.of(1), cols)),
            worktable -> {
                List<Tuple> next = new ArrayList<>();
                for (Tuple t : worktable) {
                    int val = ((Number) t.getValue(0)).intValue();
                    if (val < 5) {
                        next.add(new Tuple(List.of(val + 1), cols));
                    }
                }
                return next;
            },
            true, List.of(), null, null, 100
        );

        try (op) {
            op.open();
            List<Integer> vals = new ArrayList<>();
            TupleSlot slot;
            while ((slot = op.next()) != null && !slot.isEmpty()) {
                vals.add(((Number) slot.getTuple().getValue(0)).intValue());
            }
            assertEquals(List.of(1, 2, 3, 4, 5), vals);
        }
    }

    @Test
    void testConcurrentRecursiveCtesUnderVirtualThreads() throws Exception {
        db.execute("CREATE TABLE g (u INT, v INT)");
        db.execute("INSERT INTO g VALUES (1, 2)");
        db.execute("INSERT INTO g VALUES (2, 3)");
        db.execute("INSERT INTO g VALUES (3, 1)");

        try (var exec = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Integer>> futures = new ArrayList<>();
            for (int t = 0; t < 16; t++) {
                futures.add(exec.submit(() -> {
                    QueryResult qr = db.execute(
                        "WITH RECURSIVE r AS ("
                        + "SELECT v FROM g WHERE u = 1 "
                        + "UNION "
                        + "SELECT g.v FROM g JOIN r ON g.u = r.v"
                        + ") SELECT v FROM r");
                    assertTrue(qr.isSuccess(), qr.getError());
                    return qr.getRows().size();
                }));
            }
            for (Future<Integer> f : futures) {
                assertEquals(3, f.get());
            }
        }
    }
}
