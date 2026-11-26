package com.stratosdb.sql.optimizer;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.ast.SelectStatement;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.sql.execution.JoinUtils;
import com.stratosdb.sql.statistics.ColumnStatistics;
import com.stratosdb.sql.statistics.TableStatistics;

import java.util.*;

/**
 * System-R style dynamic programming query optimizer for multi-table join reordering
 * and access path selection (IndexScan vs. SeqScan).
 *
 * References:
 * - Selinger et al., "Access Path Selection in a Relational Database Management System", SIGMOD 1979.
 */
public class SystemROptimizer {

    public static final int MAX_DP_TABLES = 10;

    public enum JoinTreeShape {
        BUSHY,
        LEFT_DEEP
    }

    public record IndexInfo(String indexName, String tableName, String columnName, boolean isUnique, boolean isBTree, boolean isHash) {}

    @FunctionalInterface
    public interface IndexResolver {
        IndexInfo findIndex(String tableName, String columnName, boolean equalityOnly);
    }

    private final CostModel costModel;

    public SystemROptimizer() {
        this(new CostModel());
    }

    public SystemROptimizer(CostModel costModel) {
        this.costModel = costModel != null ? costModel : new CostModel();
    }

    public CostModel getCostModel() {
        return costModel;
    }

    /**
     * Determines whether cost-based join optimization can be performed for this query.
     * Returns true if all involved tables have persistent table statistics,
     * the relation count is within {@link #MAX_DP_TABLES} (10), and all joins are INNER/CROSS.
     */
    public static boolean canOptimize(SelectStatement stmt, Map<String, TableStatistics> statistics) {
        if (stmt == null || statistics == null || statistics.isEmpty()) {
            return false;
        }
        if (stmt.isDerivedTable() || stmt.tableName() == null) {
            return false;
        }
        if (statistics.get(stmt.tableName()) == null) {
            return false;
        }
        int tableCount = 1;
        if (stmt.joins() != null) {
            for (JoinClause join : stmt.joins()) {
                if (join.isSubquery() || join.tableName() == null) {
                    return false;
                }
                if (join.joinType() != null
                        && join.joinType() != JoinClause.JoinType.INNER
                        && join.joinType() != JoinClause.JoinType.CROSS) {
                    return false;
                }
                if (statistics.get(join.tableName()) == null) {
                    return false;
                }
                tableCount++;
            }
        }
        return tableCount <= MAX_DP_TABLES;
    }

    public record Relation(
        int id,
        String tableName,
        String alias,
        List<String> rawColumns,
        TableStatistics stats,
        WhereExpr singleTableFilter
    ) {
        public String effectiveName() {
            return alias != null ? alias : tableName;
        }
    }

    /**
     * Optimizes a multi-table query using System-R dynamic programming join enumeration
     * (supporting bushy and left-deep join trees up to 10 tables).
     */
    public PlanNode optimize(SelectStatement stmt,
                             Map<String, TableStatistics> statistics,
                             Map<String, List<String>> tableColumns,
                             IndexResolver indexResolver) {
        return optimize(stmt, statistics, tableColumns, indexResolver, JoinTreeShape.BUSHY);
    }

    /**
     * Optimizes a multi-table query using System-R dynamic programming join enumeration
     * with the specified {@link JoinTreeShape} ({@code BUSHY} or {@code LEFT_DEEP}).
     */
    public PlanNode optimize(SelectStatement stmt,
                             Map<String, TableStatistics> statistics,
                             Map<String, List<String>> tableColumns,
                             IndexResolver indexResolver,
                             JoinTreeShape shape) {
        if (!canOptimize(stmt, statistics)) {
            return null;
        }

        List<Relation> relations = buildRelations(stmt, statistics, tableColumns);
        int k = relations.size();
        if (k == 0 || k > MAX_DP_TABLES) return null;

        // 2. Extract join predicates (both JOIN ON clauses and WHERE equi-joins)
        List<JoinPredicate> joinPredicates = extractJoinPredicates(stmt, relations);
        int[] adjMask = buildAdjacencyMasks(k, joinPredicates, relations);

        // 3. Size 1: Base Relation Access Path Selection (SeqScan vs IndexScan)
        PlanNode[] memo = new PlanNode[1 << k];
        for (int i = 0; i < k; i++) {
            memo[1 << i] = planBaseRelation(relations.get(i), indexResolver);
        }
        if (k == 1) {
            return memo[1];
        }

        // Precompute masks grouped by popcount for clean O(3^k) bottom-up DP
        List<List<Integer>> masksBySize = new ArrayList<>(k + 1);
        for (int s = 0; s <= k; s++) {
            masksBySize.add(new ArrayList<>());
        }
        int totalMasks = 1 << k;
        for (int mask = 1; mask < totalMasks; mask++) {
            masksBySize.get(Integer.bitCount(mask)).add(mask);
        }

        // 4. Size 2 to K: O(3^k) System-R Dynamic Programming Join Enumeration
        for (int size = 2; size <= k; size++) {
            for (int mask : masksBySize.get(size)) {
                boolean maskHasConnectedPair = hasAnyConnectedPartition(mask, adjMask);
                PlanNode bestPlanForMask = null;

                // Iterate all proper non-empty subsets s1 of mask in O(3^k) total
                for (int s1 = (mask - 1) & mask; s1 > 0; s1 = (s1 - 1) & mask) {
                    int s2 = mask ^ s1;
                    if (s1 > s2) continue; // Symmetric avoidance

                    if (shape == JoinTreeShape.LEFT_DEEP
                            && Integer.bitCount(s1) > 1
                            && Integer.bitCount(s2) > 1) {
                        continue;
                    }

                    PlanNode p1 = memo[s1];
                    PlanNode p2 = memo[s2];
                    if (p1 == null || p2 == null) continue;

                    boolean connected = areSubsetsConnected(s1, s2, adjMask);
                    if (!connected && maskHasConnectedPair) {
                        continue; // Avoid Cartesian products when a connected join exists
                    }

                    List<JoinPredicate> connecting = connected
                        ? findConnectingPredicates(s1, s2, joinPredicates, relations)
                        : List.of();

                    PlanNode candidate = evaluateBestJoinBetween(p1, p2, connecting, relations, statistics);
                    if (candidate != null && (bestPlanForMask == null || candidate.getCost() < bestPlanForMask.getCost())) {
                        bestPlanForMask = candidate;
                    }
                }

                if (bestPlanForMask != null) {
                    memo[mask] = bestPlanForMask;
                }
            }
        }

        return memo[totalMasks - 1];
    }

    /**
     * Estimates the cost of a naive left-deep join plan following the exact FROM/JOIN
     * order written in the SQL query (for optimizer cost-reduction verification).
     */
    public PlanNode buildNaiveLeftDeepPlan(SelectStatement stmt,
                                           Map<String, TableStatistics> statistics,
                                           Map<String, List<String>> tableColumns,
                                           IndexResolver indexResolver) {
        if (!canOptimize(stmt, statistics)) {
            return null;
        }
        List<Relation> relations = buildRelations(stmt, statistics, tableColumns);
        if (relations.isEmpty()) return null;
        List<JoinPredicate> joinPredicates = extractJoinPredicates(stmt, relations);

        PlanNode current = planBaseRelation(relations.get(0), indexResolver);
        int currentMask = 1;
        for (int i = 1; i < relations.size(); i++) {
            PlanNode nextRel = planBaseRelation(relations.get(i), indexResolver);
            int nextMask = 1 << i;
            List<JoinPredicate> connecting = findConnectingPredicates(currentMask, nextMask, joinPredicates, relations);
            current = buildLeftDeepHashJoin(current, nextRel, connecting, relations, statistics);
            currentMask |= nextMask;
        }
        return current;
    }

    private PlanNode buildLeftDeepHashJoin(PlanNode left, PlanNode right,
                                           List<JoinPredicate> connecting,
                                           List<Relation> relations,
                                           Map<String, TableStatistics> statistics) {
        double selectivity = calculateCombinedSelectivity(connecting, relations, statistics);
        double outRows = Math.max(1.0, Math.round(left.getEstimatedRows() * right.getEstimatedRows() * selectivity));
        List<String> combinedCols = JoinUtils.combineColumns(left.getOutputColumns(), right.getOutputColumns());
        List<String> leftCols = new ArrayList<>();
        List<String> rightCols = new ArrayList<>();
        populateConnectingColumns(connecting, left, right, leftCols, rightCols);

        double cost = costModel.costHashJoin(
            left.getCost(), left.getEstimatedRows(),
            right.getCost(), right.getEstimatedRows(), outRows, true);
        return new JoinPlanNode(left, right, JoinPlanNode.JoinAlgorithm.HASH, JoinClause.JoinType.INNER,
            leftCols, rightCols, null, true, cost, outRows, combinedCols);
    }

    private PlanNode evaluateBestJoinBetween(PlanNode p1, PlanNode p2,
                                             List<JoinPredicate> connecting,
                                             List<Relation> relations,
                                             Map<String, TableStatistics> statistics) {
        double selectivity = calculateCombinedSelectivity(connecting, relations, statistics);
        double outRows = Math.max(1.0, Math.round(p1.getEstimatedRows() * p2.getEstimatedRows() * selectivity));
        List<String> combinedCols = JoinUtils.combineColumns(p1.getOutputColumns(), p2.getOutputColumns());

        List<String> p1Cols = new ArrayList<>();
        List<String> p2Cols = new ArrayList<>();
        populateConnectingColumns(connecting, p1, p2, p1Cols, p2Cols);

        PlanNode best = null;

        // Option A: Hash Join (Build on P2, Probe on P1)
        double costA = costModel.costHashJoin(p1.getCost(), p1.getEstimatedRows(), p2.getCost(), p2.getEstimatedRows(), outRows, true);
        best = new JoinPlanNode(p1, p2, JoinPlanNode.JoinAlgorithm.HASH, JoinClause.JoinType.INNER,
            p1Cols, p2Cols, null, true, costA, outRows, combinedCols);

        // Option B: Hash Join (Build on P1, Probe on P2)
        double costB = costModel.costHashJoin(p2.getCost(), p2.getEstimatedRows(), p1.getCost(), p1.getEstimatedRows(), outRows, true);
        if (costB < best.getCost()) {
            best = new JoinPlanNode(p2, p1, JoinPlanNode.JoinAlgorithm.HASH, JoinClause.JoinType.INNER,
                p2Cols, p1Cols, null, true, costB, outRows, combinedCols);
        }

        // Option C: Merge Join (only when at least one equi-join key exists)
        if (!p1Cols.isEmpty()) {
            double costMerge = costModel.costMergeJoin(p1.getCost(), p1.getEstimatedRows(), p2.getCost(), p2.getEstimatedRows(), outRows, false, false);
            if (costMerge < best.getCost()) {
                best = new JoinPlanNode(p1, p2, JoinPlanNode.JoinAlgorithm.MERGE, JoinClause.JoinType.INNER,
                    p1Cols, p2Cols, null, false, costMerge, outRows, combinedCols);
            }
        }

        // Option D: Nested Loop Join
        double costNL = costModel.costNestedLoopJoin(p1.getCost(), p1.getEstimatedRows(), p2.getCost(), p2.getEstimatedRows(), outRows);
        if (costNL < best.getCost()) {
            best = new JoinPlanNode(p1, p2, JoinPlanNode.JoinAlgorithm.NESTED_LOOP, JoinClause.JoinType.INNER,
                p1Cols, p2Cols, null, false, costNL, outRows, combinedCols);
        }

        return best;
    }

    private List<Relation> buildRelations(SelectStatement stmt,
                                          Map<String, TableStatistics> statistics,
                                          Map<String, List<String>> tableColumns) {
        List<Relation> relations = new ArrayList<>();
        String fromTable = stmt.tableName();
        String fromAlias = stmt.tableAlias();
        List<String> fromCols = tableColumns != null ? tableColumns.getOrDefault(fromTable, List.of()) : List.of();
        TableStatistics fromStats = statistics.get(fromTable);

        Map<String, List<WhereExpr>> filtersByTable = extractSingleTableFilters(stmt.where(), stmt);
        relations.add(new Relation(0, fromTable, fromAlias, fromCols, fromStats,
            combineFilters(filtersByTable.get((fromAlias != null ? fromAlias : fromTable).toLowerCase()))));

        int relIdx = 1;
        if (stmt.joins() != null) {
            for (JoinClause join : stmt.joins()) {
                String jTable = join.tableName();
                String jAlias = join.tableAlias();
                List<String> jCols = tableColumns != null ? tableColumns.getOrDefault(jTable, List.of()) : List.of();
                TableStatistics jStats = statistics.get(jTable);
                String effKey = (jAlias != null ? jAlias : jTable).toLowerCase();
                relations.add(new Relation(relIdx++, jTable, jAlias, jCols, jStats,
                    combineFilters(filtersByTable.get(effKey))));
            }
        }
        return relations;
    }

    private PlanNode planBaseRelation(Relation rel, IndexResolver indexResolver) {
        TableStatistics stats = rel.stats();
        String effName = rel.effectiveName();
        List<String> qualCols = JoinUtils.qualifyColumnsWithAlias(rel.rawColumns(), effName);

        double seqCost = costModel.costSeqScan(stats);
        long totalRows = stats.rowCount();
        double estRows = totalRows;

        WhereExpr filter = rel.singleTableFilter();
        if (filter == null) {
            return new ScanPlanNode(rel.tableName(), rel.alias(), false, null, null, null, null, null, null, seqCost, estRows, qualCols);
        }

        // Check if filter contains a single-column comparison on an indexed attribute
        WhereExpr.Comparison cmp = extractSimpleComparison(filter);
        if (cmp != null && indexResolver != null) {
            String colName = unqualifyColName(cmp.column());
            boolean isEquality = cmp.operator().equals("=");
            IndexInfo idx = indexResolver.findIndex(rel.tableName(), colName, isEquality);
            if (idx != null) {
                try {
                    long val = Long.parseLong(cmp.literal());
                    long lo, hi;
                    switch (cmp.operator()) {
                        case "=" -> { lo = val; hi = val; }
                        case ">" -> { lo = val + 1; hi = Long.MAX_VALUE; }
                        case ">=" -> { lo = val; hi = Long.MAX_VALUE; }
                        case "<" -> { lo = Long.MIN_VALUE; hi = val - 1; }
                        case "<=" -> { lo = Long.MIN_VALUE; hi = val; }
                        default -> { lo = Long.MIN_VALUE; hi = Long.MAX_VALUE; }
                    }

                    ColumnStatistics colStats = stats.columnStats().get(colName);
                    double idxCost = costModel.costIndexScan(stats, colName, cmp.operator(), lo, hi, colStats);
                    double sel = colStats != null ? colStats.estimateSelectivity(cmp.operator(), lo, hi) : 0.1;
                    double idxEstRows = Math.max(1, Math.round(totalRows * sel));

                    if (idxCost < seqCost) {
                        return new ScanPlanNode(rel.tableName(), rel.alias(), true, idx.indexName(), colName,
                            cmp.operator(), lo, hi, filter, idxCost, idxEstRows, qualCols);
                    }
                } catch (NumberFormatException ignored) {}
            }
        }

        // SeqScan with filter evaluation CPU cost
        double filterCpu = totalRows * costModel.getCpuOperatorCost();
        double sel = 0.5; // default fallback if selectivity unknown
        if (cmp != null) {
            String col = unqualifyColName(cmp.column());
            ColumnStatistics cs = stats.columnStats().get(col);
            if (cs != null) {
                try {
                    long v = Long.parseLong(cmp.literal());
                    sel = cs.estimateSelectivity(cmp.operator(), v, v);
                } catch (Exception ignored) {}
            }
        }
        estRows = Math.max(1, Math.round(totalRows * sel));
        return new ScanPlanNode(rel.tableName(), rel.alias(), false, null, null, null, null, null, filter, seqCost + filterCpu, estRows, qualCols);
    }

    private static double calculateJoinSelectivity(JoinPredicate pred,
                                                   List<Relation> relations,
                                                   Map<String, TableStatistics> statistics) {
        if (pred == null || pred.getLeftTable() == null || pred.getRightTable() == null) {
            return 0.1; // default Cartesian or cross selectivity
        }

        TableStatistics leftStats = findStats(pred.getLeftTable(), relations, statistics);
        TableStatistics rightStats = findStats(pred.getRightTable(), relations, statistics);

        double dLeft = 1.0;
        double dRight = 1.0;

        if (leftStats != null) {
            ColumnStatistics cs = leftStats.columnStats().get(unqualifyColName(pred.getLeftColumn()));
            if (cs != null) dLeft = Math.max(1.0, cs.distinctCount());
        }
        if (rightStats != null) {
            ColumnStatistics cs = rightStats.columnStats().get(unqualifyColName(pred.getRightColumn()));
            if (cs != null) dRight = Math.max(1.0, cs.distinctCount());
        }

        return 1.0 / Math.max(1.0, Math.max(dLeft, dRight));
    }

    private static TableStatistics findStats(String tableNameOrAlias,
                                             List<Relation> relations,
                                             Map<String, TableStatistics> statistics) {
        for (Relation r : relations) {
            if (tableNameOrAlias.equalsIgnoreCase(r.effectiveName()) || tableNameOrAlias.equalsIgnoreCase(r.tableName())) {
                return r.stats();
            }
        }
        return statistics.get(tableNameOrAlias);
    }

    private static String resolveColRef(JoinPredicate pred, PlanNode node) {
        if (pred == null) return null;
        for (String t : node.getCoveredTables()) {
            if (t.equalsIgnoreCase(pred.getLeftTable())) {
                return pred.getQualifiedColumnForTable(pred.getLeftTable());
            }
            if (t.equalsIgnoreCase(pred.getRightTable())) {
                return pred.getQualifiedColumnForTable(pred.getRightTable());
            }
        }
        return pred.getLeftColumn();
    }

    private static List<JoinPredicate> findConnectingPredicates(int s1, int s2,
                                                               List<JoinPredicate> joinPredicates,
                                                               List<Relation> relations) {
        List<JoinPredicate> connecting = new ArrayList<>();
        for (JoinPredicate jp : joinPredicates) {
            boolean inS1 = false;
            boolean inS2 = false;

            for (int i = 0; i < relations.size(); i++) {
                Relation r = relations.get(i);
                boolean matchesLeft = r.effectiveName().equalsIgnoreCase(jp.getLeftTable()) || r.tableName().equalsIgnoreCase(jp.getLeftTable());
                boolean matchesRight = r.effectiveName().equalsIgnoreCase(jp.getRightTable()) || r.tableName().equalsIgnoreCase(jp.getRightTable());

                if ((s1 & (1 << i)) != 0) {
                    if (matchesLeft || matchesRight) inS1 = true;
                }
                if ((s2 & (1 << i)) != 0) {
                    if (matchesLeft || matchesRight) inS2 = true;
                }
            }

            if (inS1 && inS2) {
                connecting.add(jp);
            }
        }
        return connecting;
    }

    private static double calculateCombinedSelectivity(List<JoinPredicate> connecting,
                                                       List<Relation> relations,
                                                       Map<String, TableStatistics> statistics) {
        if (connecting == null || connecting.isEmpty()) {
            return 1.0; // Cross join / Cartesian product
        }
        double sel = 1.0;
        for (JoinPredicate pred : connecting) {
            sel *= calculateJoinSelectivity(pred, relations, statistics);
        }
        return Math.max(1e-9, Math.min(1.0, sel));
    }

    private static void populateConnectingColumns(List<JoinPredicate> connecting,
                                                  PlanNode p1,
                                                  PlanNode p2,
                                                  List<String> p1Cols,
                                                  List<String> p2Cols) {
        if (connecting == null) return;
        for (JoinPredicate pred : connecting) {
            String c1 = resolveColRef(pred, p1);
            String c2 = resolveColRef(pred, p2);
            if (c1 != null && c2 != null) {
                p1Cols.add(c1);
                p2Cols.add(c2);
            }
        }
    }

    private static int[] buildAdjacencyMasks(int k, List<JoinPredicate> joinPredicates, List<Relation> relations) {
        int[] adj = new int[k];
        for (JoinPredicate jp : joinPredicates) {
            int l = findRelationIndex(jp.getLeftTable(), relations);
            int r = findRelationIndex(jp.getRightTable(), relations);
            if (l >= 0 && r >= 0 && l != r) {
                adj[l] |= (1 << r);
                adj[r] |= (1 << l);
            }
        }
        return adj;
    }

    private static int findRelationIndex(String tableNameOrAlias, List<Relation> relations) {
        if (tableNameOrAlias == null) return -1;
        for (int i = 0; i < relations.size(); i++) {
            Relation r = relations.get(i);
            if (tableNameOrAlias.equalsIgnoreCase(r.effectiveName()) || tableNameOrAlias.equalsIgnoreCase(r.tableName())) {
                return i;
            }
        }
        return -1;
    }

    private static boolean areSubsetsConnected(int s1, int s2, int[] adjMask) {
        int m = s1;
        while (m != 0) {
            int lsb = Integer.numberOfTrailingZeros(m);
            if ((adjMask[lsb] & s2) != 0) {
                return true;
            }
            m &= m - 1;
        }
        return false;
    }

    private static boolean hasAnyConnectedPartition(int mask, int[] adjMask) {
        int m = mask;
        while (m != 0) {
            int lsb = Integer.numberOfTrailingZeros(m);
            int rest = mask ^ (1 << lsb);
            if ((adjMask[lsb] & rest) != 0) {
                return true;
            }
            m &= m - 1;
        }
        return false;
    }

    private static List<JoinPredicate> extractJoinPredicates(SelectStatement stmt, List<Relation> relations) {
        List<JoinPredicate> list = new ArrayList<>();
        if (stmt.joins() != null) {
            for (JoinClause jc : stmt.joins()) {
                List<String> lCols = jc.leftColumns();
                List<String> rCols = jc.rightColumns();
                if (lCols != null && rCols != null && !lCols.isEmpty() && lCols.size() == rCols.size()) {
                    for (int i = 0; i < lCols.size(); i++) {
                        addEquiPredicate(lCols.get(i), rCols.get(i), jc, relations, list);
                    }
                } else if (jc.leftColumn() != null && jc.rightColumn() != null) {
                    addEquiPredicate(jc.leftColumn(), jc.rightColumn(), jc, relations, list);
                }
            }
        }
        if (stmt.where() != null) {
            List<WhereExpr> conjuncts = new ArrayList<>();
            splitAnd(stmt.where(), conjuncts);
            for (WhereExpr c : conjuncts) {
                if (c instanceof WhereExpr.ColumnComparison cc && "=".equals(cc.operator())) {
                    addEquiPredicate(cc.leftColumn(), cc.rightColumn(), null, relations, list);
                }
            }
        }
        return list;
    }

    private static void addEquiPredicate(String leftRaw, String rightRaw, JoinClause jc,
                                         List<Relation> relations, List<JoinPredicate> list) {
        String leftTbl = extractTable(leftRaw, relations);
        String leftCol = unqualifyColName(leftRaw);
        String rightTbl = extractTable(rightRaw, relations);
        String rightCol = unqualifyColName(rightRaw);

        if (leftTbl == null && jc != null && jc.tableName() != null) {
            rightTbl = jc.effectiveTargetName();
        }
        if (leftTbl != null && rightTbl != null && !leftTbl.equalsIgnoreCase(rightTbl)) {
            list.add(new JoinPredicate(leftTbl, leftCol, rightTbl, rightCol,
                jc != null ? jc.joinType() : JoinClause.JoinType.INNER));
        }
    }

    private static String extractTable(String colRef, List<Relation> relations) {
        if (colRef != null && colRef.contains(".")) {
            return colRef.substring(0, colRef.lastIndexOf('.'));
        }
        // Try finding which relation owns this column
        if (colRef != null) {
            for (Relation r : relations) {
                for (String c : r.rawColumns()) {
                    if (c.equalsIgnoreCase(colRef)) {
                        return r.effectiveName();
                    }
                }
            }
        }
        return null;
    }

    private static String unqualifyColName(String col) {
        if (col == null) return null;
        return col.contains(".") ? col.substring(col.lastIndexOf('.') + 1) : col;
    }

    private static Map<String, List<WhereExpr>> extractSingleTableFilters(WhereExpr where, SelectStatement stmt) {
        Map<String, List<WhereExpr>> map = new HashMap<>();
        if (where == null) return map;

        List<WhereExpr> conjuncts = new ArrayList<>();
        splitAnd(where, conjuncts);

        for (WhereExpr c : conjuncts) {
            Set<String> referencedTables = findReferencedTables(c);
            if (referencedTables.size() == 1) {
                String tbl = referencedTables.iterator().next().toLowerCase();
                map.computeIfAbsent(tbl, k -> new ArrayList<>()).add(c);
            }
        }
        return map;
    }

    private static void splitAnd(WhereExpr expr, List<WhereExpr> list) {
        if (expr instanceof WhereExpr.And and) {
            splitAnd(and.left(), list);
            splitAnd(and.right(), list);
        } else if (expr != null) {
            list.add(expr);
        }
    }

    private static Set<String> findReferencedTables(WhereExpr expr) {
        Set<String> tables = new HashSet<>();
        collectReferencedTables(expr, tables);
        return tables;
    }

    private static void collectReferencedTables(WhereExpr expr, Set<String> tables) {
        if (expr == null) return;
        switch (expr) {
            case WhereExpr.Comparison cmp -> addTablePrefix(cmp.column(), tables);
            case WhereExpr.Like like -> addTablePrefix(like.column(), tables);
            case WhereExpr.Contains c -> addTablePrefix(c.column(), tables);
            case WhereExpr.InList in -> addTablePrefix(in.column(), tables);
            case WhereExpr.ColumnComparison cc -> {
                addTablePrefix(cc.leftColumn(), tables);
                addTablePrefix(cc.rightColumn(), tables);
            }
            case WhereExpr.And and -> {
                collectReferencedTables(and.left(), tables);
                collectReferencedTables(and.right(), tables);
            }
            case WhereExpr.Or or -> {
                collectReferencedTables(or.left(), tables);
                collectReferencedTables(or.right(), tables);
            }
            case WhereExpr.Not not -> collectReferencedTables(not.inner(), tables);
            default -> {}
        }
    }

    private static void addTablePrefix(String col, Set<String> tables) {
        if (col != null && col.contains(".")) {
            tables.add(col.substring(0, col.lastIndexOf('.')));
        }
    }

    private static WhereExpr combineFilters(List<WhereExpr> list) {
        if (list == null || list.isEmpty()) return null;
        WhereExpr res = list.get(0);
        for (int i = 1; i < list.size(); i++) {
            res = new WhereExpr.And(res, list.get(i));
        }
        return res;
    }

    private static WhereExpr.Comparison extractSimpleComparison(WhereExpr where) {
        if (where instanceof WhereExpr.Comparison cmp) {
            return cmp;
        }
        if (where instanceof WhereExpr.And and) {
            WhereExpr.Comparison left = extractSimpleComparison(and.left());
            return left != null ? left : extractSimpleComparison(and.right());
        }
        return null;
    }
}
