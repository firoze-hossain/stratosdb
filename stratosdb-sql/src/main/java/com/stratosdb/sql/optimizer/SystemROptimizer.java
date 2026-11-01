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
     * Returns true if all involved tables have persistent table statistics.
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
        if (stmt.joins() != null) {
            for (JoinClause join : stmt.joins()) {
                if (join.isSubquery() || join.tableName() == null) {
                    return false;
                }
                if (statistics.get(join.tableName()) == null) {
                    return false;
                }
            }
        }
        return true;
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
     * Optimizes a multi-table query using System-R dynamic programming join enumeration.
     */
    public PlanNode optimize(SelectStatement stmt,
                             Map<String, TableStatistics> statistics,
                             Map<String, List<String>> tableColumns,
                             IndexResolver indexResolver) {
        if (!canOptimize(stmt, statistics)) {
            return null;
        }

        // 1. Identify all relations in the query (FROM + JOIN targets)
        List<Relation> relations = new ArrayList<>();
        String fromTable = stmt.tableName();
        String fromAlias = stmt.tableAlias();
        List<String> fromCols = tableColumns.getOrDefault(fromTable, List.of());
        TableStatistics fromStats = statistics.get(fromTable);

        // Separate single-table WHERE filters vs multi-table join filters
        Map<String, List<WhereExpr>> filtersByTable = extractSingleTableFilters(stmt.where(), stmt);

        relations.add(new Relation(0, fromTable, fromAlias, fromCols, fromStats,
            combineFilters(filtersByTable.get(fromAlias != null ? fromAlias.toLowerCase() : fromTable.toLowerCase()))));

        int relIdx = 1;
        if (stmt.joins() != null) {
            for (JoinClause join : stmt.joins()) {
                String jTable = join.tableName();
                String jAlias = join.tableAlias();
                List<String> jCols = tableColumns.getOrDefault(jTable, List.of());
                TableStatistics jStats = statistics.get(jTable);
                String effKey = (jAlias != null ? jAlias : jTable).toLowerCase();
                relations.add(new Relation(relIdx++, jTable, jAlias, jCols, jStats, combineFilters(filtersByTable.get(effKey))));
            }
        }

        int k = relations.size();
        if (k == 0) return null;

        // 2. Extract join predicates
        List<JoinPredicate> joinPredicates = extractJoinPredicates(stmt, relations);

        // 3. Size 1: Base Relation Access Path Selection (SeqScan vs IndexScan)
        Map<Integer, PlanNode> memo = new HashMap<>();
        for (int i = 0; i < k; i++) {
            Relation rel = relations.get(i);
            PlanNode scanPlan = planBaseRelation(rel, indexResolver);
            memo.put(1 << i, scanPlan);
        }

        // 4. Size 2 to K: Dynamic Programming Join Enumeration
        for (int size = 2; size <= k; size++) {
            for (int mask = 1; mask < (1 << k); mask++) {
                if (Integer.bitCount(mask) != size) continue;

                PlanNode bestPlanForMask = null;

                // Enumerate non-empty disjoint partitions (S1, S2)
                for (int s1 = 1; s1 < mask; s1++) {
                    if ((mask & s1) != s1) continue;
                    int s2 = mask ^ s1;
                    if (s1 > s2) continue; // Symmetric avoidance

                    PlanNode p1 = memo.get(s1);
                    PlanNode p2 = memo.get(s2);
                    if (p1 == null || p2 == null) continue;

                    List<JoinPredicate> connecting = findConnectingPredicates(s1, s2, joinPredicates, relations);
                    if (connecting.isEmpty()) {
                        // Skip Cartesian product if a connected partition exists for this mask
                        if (hasConnectedPartition(mask, joinPredicates, relations)) {
                            continue;
                        }
                    }

                    List<JoinPredicate> predicatesToTry = connecting.isEmpty()
                        ? List.of(new JoinPredicate(null, null, null, null, JoinClause.JoinType.INNER))
                        : connecting;

                    for (JoinPredicate pred : predicatesToTry) {
                        double selectivity = calculateJoinSelectivity(pred, relations, statistics);
                        double outRows = Math.max(1.0, Math.round(p1.getEstimatedRows() * p2.getEstimatedRows() * selectivity));
                        List<String> combinedCols = JoinUtils.combineColumns(p1.getOutputColumns(), p2.getOutputColumns());

                        String leftCol = resolveColRef(pred, p1);
                        String rightCol = resolveColRef(pred, p2);

                        // Option A: Hash Join (Build on P2, Probe on P1)
                        double costA = costModel.costHashJoin(p1.getCost(), p1.getEstimatedRows(), p2.getCost(), p2.getEstimatedRows(), outRows, true);
                        if (bestPlanForMask == null || costA < bestPlanForMask.getCost()) {
                            bestPlanForMask = new JoinPlanNode(p1, p2, JoinPlanNode.JoinAlgorithm.HASH, pred.getJoinType(),
                                leftCol, rightCol, true, costA, outRows, combinedCols);
                        }

                        // Option B: Hash Join (Build on P1, Probe on P2)
                        double costB = costModel.costHashJoin(p2.getCost(), p2.getEstimatedRows(), p1.getCost(), p1.getEstimatedRows(), outRows, true);
                        if (costB < bestPlanForMask.getCost()) {
                            bestPlanForMask = new JoinPlanNode(p2, p1, JoinPlanNode.JoinAlgorithm.HASH, pred.getJoinType(),
                                rightCol, leftCol, true, costB, outRows, combinedCols);
                        }

                        // Option C: Merge Join
                        double costMerge = costModel.costMergeJoin(p1.getCost(), p1.getEstimatedRows(), p2.getCost(), p2.getEstimatedRows(), outRows, false, false);
                        if (costMerge < bestPlanForMask.getCost()) {
                            bestPlanForMask = new JoinPlanNode(p1, p2, JoinPlanNode.JoinAlgorithm.MERGE, pred.getJoinType(),
                                leftCol, rightCol, false, costMerge, outRows, combinedCols);
                        }

                        // Option D: Nested Loop Join
                        double costNL = costModel.costNestedLoopJoin(p1.getCost(), p1.getEstimatedRows(), p2.getCost(), p2.getEstimatedRows(), outRows);
                        if (costNL < bestPlanForMask.getCost()) {
                            bestPlanForMask = new JoinPlanNode(p1, p2, JoinPlanNode.JoinAlgorithm.NESTED_LOOP, pred.getJoinType(),
                                leftCol, rightCol, false, costNL, outRows, combinedCols);
                        }
                    }
                }

                if (bestPlanForMask != null) {
                    memo.put(mask, bestPlanForMask);
                }
            }
        }

        return memo.get((1 << k) - 1);
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

    private static boolean hasConnectedPartition(int mask,
                                                 List<JoinPredicate> joinPredicates,
                                                 List<Relation> relations) {
        for (int s1 = 1; s1 < mask; s1++) {
            if ((mask & s1) != s1) continue;
            int s2 = mask ^ s1;
            if (!findConnectingPredicates(s1, s2, joinPredicates, relations).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static List<JoinPredicate> extractJoinPredicates(SelectStatement stmt, List<Relation> relations) {
        List<JoinPredicate> list = new ArrayList<>();
        if (stmt.joins() != null) {
            for (JoinClause jc : stmt.joins()) {
                String leftRaw = jc.leftColumn();
                String rightRaw = jc.rightColumn();

                String leftTbl = extractTable(leftRaw, relations);
                String leftCol = unqualifyColName(leftRaw);
                String rightTbl = extractTable(rightRaw, relations);
                String rightCol = unqualifyColName(rightRaw);

                if (leftTbl == null && jc.tableName() != null) {
                    rightTbl = jc.effectiveTargetName();
                }

                list.add(new JoinPredicate(leftTbl, leftCol, rightTbl, rightCol, jc.joinType()));
            }
        }
        return list;
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
        if (expr instanceof WhereExpr.Comparison cmp) {
            if (cmp.column() != null && cmp.column().contains(".")) {
                tables.add(cmp.column().substring(0, cmp.column().lastIndexOf('.')));
            }
        }
        return tables;
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
