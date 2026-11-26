package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.storage.page.Tuple;

import java.util.*;
import java.util.function.BiPredicate;

/**
 * Physical operator that implements hash join with an in-memory hash table on the build side
 * and a streaming probe side.
 * Supports multi-column composite equi-keys, residual non-equi filter predicates,
 * pure non-equi joins, and INNER, LEFT, RIGHT, FULL, CROSS, SEMI, and ANTI outer joins.
 * Memory scales strictly with the build table size; the probe table streams in O(1) memory.
 */
public class HashJoinOperator implements PhysicalOperator {
    private final PhysicalOperator left;
    private final PhysicalOperator right;
    private final List<String> leftColumns;
    private final List<String> rightColumns;
    private final WhereExpr nonEquiFilter;
    private final BiPredicate<Tuple, Tuple> nonEquiCondition;
    private final JoinClause.JoinType joinType;
    private final List<String> leftColumnsList;
    private final List<String> rightColumnsList;
    private final List<String> outputColumns;

    // Build side is right by default (for INNER, LEFT, FULL, CROSS, SEMI, ANTI); for RIGHT join, build side is left
    private final boolean buildOnLeft;
    private final PhysicalOperator buildSide;
    private final PhysicalOperator probeSide;
    private final List<String> buildColumns;
    private final List<String> probeColumns;

    private Map<JoinUtils.CompositeKey, List<Tuple>> hashTable = null;
    private List<Tuple> allBuildTuples = null;
    private List<Tuple> nullKeyBuildTuples = null;
    private Set<Tuple> matchedBuildTuples = null;
    private Iterator<Tuple> unmatchedBuildIterator = null;

    private Tuple currentProbeTuple = null;
    private List<Tuple> currentMatches = null;
    private int currentMatchIndex = 0;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public HashJoinOperator(PhysicalOperator left,
                            PhysicalOperator right,
                            List<String> leftColumns,
                            List<String> rightColumns,
                            WhereExpr nonEquiFilter,
                            BiPredicate<Tuple, Tuple> nonEquiCondition,
                            JoinClause.JoinType joinType) {
        this.left = left;
        this.right = right;
        this.leftColumns = leftColumns != null ? List.copyOf(leftColumns) : List.of();
        this.rightColumns = rightColumns != null ? List.copyOf(rightColumns) : List.of();
        this.nonEquiFilter = nonEquiFilter;
        this.nonEquiCondition = nonEquiCondition;
        this.joinType = joinType != null ? joinType : JoinClause.JoinType.INNER;
        this.leftColumnsList = left != null ? left.getOutputColumns() : List.of();
        this.rightColumnsList = right != null ? right.getOutputColumns() : List.of();
        if (this.joinType == JoinClause.JoinType.SEMI || this.joinType == JoinClause.JoinType.ANTI) {
            this.outputColumns = List.copyOf(this.leftColumnsList);
        } else {
            this.outputColumns = JoinUtils.combineColumns(leftColumnsList, rightColumnsList);
        }

        // For RIGHT JOIN, build side is left (so right streams as probe)
        // For LEFT, FULL, INNER, CROSS, SEMI, and ANTI joins, build side is right (so left streams as probe)
        this.buildOnLeft = (this.joinType == JoinClause.JoinType.RIGHT);
        this.buildSide = buildOnLeft ? left : right;
        this.probeSide = buildOnLeft ? right : left;
        this.buildColumns = buildOnLeft ? this.leftColumns : this.rightColumns;
        this.probeColumns = buildOnLeft ? this.rightColumns : this.leftColumns;
    }

    public HashJoinOperator(PhysicalOperator left,
                            PhysicalOperator right,
                            List<String> leftColumns,
                            List<String> rightColumns,
                            WhereExpr nonEquiFilter,
                            JoinClause.JoinType joinType) {
        this(left, right, leftColumns, rightColumns, nonEquiFilter, null, joinType);
    }

    public HashJoinOperator(PhysicalOperator left,
                            PhysicalOperator right,
                            List<String> leftColumns,
                            List<String> rightColumns,
                            BiPredicate<Tuple, Tuple> nonEquiCondition,
                            JoinClause.JoinType joinType) {
        this(left, right, leftColumns, rightColumns, null, nonEquiCondition, joinType);
    }

    public HashJoinOperator(PhysicalOperator left,
                            PhysicalOperator right,
                            List<String> leftColumns,
                            List<String> rightColumns,
                            JoinClause.JoinType joinType) {
        this(left, right, leftColumns, rightColumns, null, null, joinType);
    }

    public HashJoinOperator(PhysicalOperator left,
                            PhysicalOperator right,
                            String leftColumn,
                            String rightColumn,
                            JoinClause.JoinType joinType) {
        this(left, right,
             leftColumn != null ? List.of(leftColumn) : List.of(),
             rightColumn != null ? List.of(rightColumn) : List.of(),
             null, null, joinType);
    }

    public HashJoinOperator(PhysicalOperator left,
                            PhysicalOperator right,
                            String leftColumn,
                            String rightColumn,
                            WhereExpr nonEquiFilter,
                            JoinClause.JoinType joinType) {
        this(left, right,
             leftColumn != null ? List.of(leftColumn) : List.of(),
             rightColumn != null ? List.of(rightColumn) : List.of(),
             nonEquiFilter, null, joinType);
    }

    @Override
    public void open() {
        close();
        hashTable = new HashMap<>();
        allBuildTuples = new ArrayList<>();
        if (joinType == JoinClause.JoinType.FULL) {
            matchedBuildTuples = Collections.newSetFromMap(new IdentityHashMap<>());
            nullKeyBuildTuples = new ArrayList<>();
        }

        // 1. Build phase: populate in-memory hash table from build side
        if (buildSide != null) {
            buildSide.open();
            while (true) {
                TupleSlot s = buildSide.next();
                if (s == null || s.isEmpty()) break;
                Tuple tuple = s.getTuple();
                allBuildTuples.add(tuple);

                if (!buildColumns.isEmpty()) {
                    JoinUtils.CompositeKey key = JoinUtils.extractCompositeKey(tuple, buildColumns);
                    if (!key.hasNull()) {
                        hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
                    } else if (nullKeyBuildTuples != null) {
                        nullKeyBuildTuples.add(tuple);
                    }
                }
            }
            buildSide.close();
        }

        // 2. Open probe side
        if (probeSide != null) {
            probeSide.open();
        }

        currentProbeTuple = null;
        currentMatches = null;
        currentMatchIndex = 0;
        unmatchedBuildIterator = null;
        open = true;
    }

    private boolean testCondition(Tuple leftTuple, Tuple rightTuple) {
        if (nonEquiFilter != null) {
            if (!JoinUtils.evaluateJoinCondition(leftTuple, rightTuple, nonEquiFilter)) {
                return false;
            }
        }
        if (nonEquiCondition != null) {
            if (!nonEquiCondition.test(leftTuple, rightTuple)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public TupleSlot next() {
        if (!open) {
            slot.clear();
            return slot;
        }

        while (true) {
            // If actively yielding matches for current probe tuple
            if (currentMatches != null && currentMatchIndex < currentMatches.size()) {
                Tuple buildTuple = currentMatches.get(currentMatchIndex++);
                if (matchedBuildTuples != null) {
                    matchedBuildTuples.add(buildTuple);
                }
                Tuple merged = buildOnLeft
                    ? JoinUtils.merge(buildTuple, currentProbeTuple)
                    : JoinUtils.merge(currentProbeTuple, buildTuple);
                slot.set(merged);
                return slot;
            }

            // Advance probe side
            TupleSlot probeSlot = probeSide != null ? probeSide.next() : null;
            if (probeSlot == null || probeSlot.isEmpty()) {
                // Probe side exhausted
                break;
            }

            currentProbeTuple = probeSlot.getTuple();

            // Case A: No equi-columns (pure non-equi join or Cartesian product)
            if (buildColumns.isEmpty() || probeColumns.isEmpty()) {
                List<Tuple> matchingCandidates = new ArrayList<>();
                for (Tuple bTuple : allBuildTuples) {
                    Tuple l = buildOnLeft ? bTuple : currentProbeTuple;
                    Tuple r = buildOnLeft ? currentProbeTuple : bTuple;
                    if (testCondition(l, r)) {
                        matchingCandidates.add(bTuple);
                    }
                }

                if (joinType == JoinClause.JoinType.SEMI) {
                    if (!matchingCandidates.isEmpty()) {
                        slot.set(currentProbeTuple);
                        return slot;
                    }
                    continue;
                }

                if (joinType == JoinClause.JoinType.ANTI) {
                    if (matchingCandidates.isEmpty()) {
                        slot.set(currentProbeTuple);
                        return slot;
                    }
                    continue;
                }

                if (!matchingCandidates.isEmpty()) {
                    currentMatches = matchingCandidates;
                    currentMatchIndex = 0;
                    continue;
                } else {
                    if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                        slot.set(JoinUtils.mergeWithNullRight(currentProbeTuple, rightColumnsList));
                        return slot;
                    } else if (joinType == JoinClause.JoinType.RIGHT) {
                        slot.set(JoinUtils.mergeWithNullLeft(leftColumnsList, currentProbeTuple));
                        return slot;
                    }
                    continue;
                }
            }

            // Case B: Equi-join (with possible compound keys and non-equi residual filters)
            JoinUtils.CompositeKey probeKey = JoinUtils.extractCompositeKey(currentProbeTuple, probeColumns);

            if (probeKey.hasNull()) {
                // NULL never matches in equi-join
                if (joinType == JoinClause.JoinType.ANTI) {
                    slot.set(currentProbeTuple);
                    return slot;
                }
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    slot.set(JoinUtils.mergeWithNullRight(currentProbeTuple, rightColumnsList));
                    return slot;
                } else if (joinType == JoinClause.JoinType.RIGHT) {
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumnsList, currentProbeTuple));
                    return slot;
                }
                continue;
            }

            List<Tuple> rawCandidates = hashTable.get(probeKey);
            List<Tuple> matchingCandidates = null;
            if (rawCandidates != null && !rawCandidates.isEmpty()) {
                matchingCandidates = new ArrayList<>();
                for (Tuple bTuple : rawCandidates) {
                    Tuple l = buildOnLeft ? bTuple : currentProbeTuple;
                    Tuple r = buildOnLeft ? currentProbeTuple : bTuple;
                    if (testCondition(l, r)) {
                        matchingCandidates.add(bTuple);
                    }
                }
            }

            if (joinType == JoinClause.JoinType.SEMI) {
                if (matchingCandidates != null && !matchingCandidates.isEmpty()) {
                    slot.set(currentProbeTuple);
                    return slot;
                }
                continue;
            }

            if (joinType == JoinClause.JoinType.ANTI) {
                if (matchingCandidates == null || matchingCandidates.isEmpty()) {
                    slot.set(currentProbeTuple);
                    return slot;
                }
                continue;
            }

            if (matchingCandidates != null && !matchingCandidates.isEmpty()) {
                currentMatches = matchingCandidates;
                currentMatchIndex = 0;
                // Loop will pick up currentMatches at top and emit first match
            } else {
                currentMatches = null;
                // No match for this probe tuple
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    slot.set(JoinUtils.mergeWithNullRight(currentProbeTuple, rightColumnsList));
                    return slot;
                } else if (joinType == JoinClause.JoinType.RIGHT) {
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumnsList, currentProbeTuple));
                    return slot;
                }
            }
        }

        // Probe side completed - for FULL OUTER JOIN, emit remaining unmatched build side tuples
        if (joinType == JoinClause.JoinType.FULL) {
            if (unmatchedBuildIterator == null) {
                List<Tuple> allUnmatched = new ArrayList<>();
                if (allBuildTuples != null) {
                    for (Tuple t : allBuildTuples) {
                        if (matchedBuildTuples == null || !matchedBuildTuples.contains(t)) {
                            allUnmatched.add(t);
                        }
                    }
                }
                unmatchedBuildIterator = allUnmatched.iterator();
            }

            if (unmatchedBuildIterator != null && unmatchedBuildIterator.hasNext()) {
                Tuple unmatchedBuild = unmatchedBuildIterator.next();
                // Build side was right
                slot.set(JoinUtils.mergeWithNullLeft(leftColumnsList, unmatchedBuild));
                return slot;
            }
        }

        slot.clear();
        return slot;
    }

    @Override
    public void close() {
        if (probeSide != null) {
            try { probeSide.close(); } catch (Exception ignored) {}
        }
        if (buildSide != null) {
            try { buildSide.close(); } catch (Exception ignored) {}
        }
        if (hashTable != null) {
            hashTable.clear();
            hashTable = null;
        }
        if (allBuildTuples != null) {
            allBuildTuples.clear();
            allBuildTuples = null;
        }
        if (matchedBuildTuples != null) {
            matchedBuildTuples.clear();
            matchedBuildTuples = null;
        }
        if (nullKeyBuildTuples != null) {
            nullKeyBuildTuples.clear();
            nullKeyBuildTuples = null;
        }
        unmatchedBuildIterator = null;
        currentProbeTuple = null;
        currentMatches = null;
        slot.clear();
        open = false;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }

    public JoinClause.JoinType getJoinType() {
        return joinType;
    }

    public PhysicalOperator getLeft() {
        return left;
    }

    public PhysicalOperator getRight() {
        return right;
    }

    public String getLeftColumn() {
        return !leftColumns.isEmpty() ? leftColumns.get(0) : null;
    }

    public String getRightColumn() {
        return !rightColumns.isEmpty() ? rightColumns.get(0) : null;
    }

    public List<String> getLeftColumns() {
        return leftColumns;
    }

    public List<String> getRightColumns() {
        return rightColumns;
    }

    public WhereExpr getNonEquiFilter() {
        return nonEquiFilter;
    }
}
