package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.storage.page.Tuple;

import java.util.*;

/**
 * Physical operator that implements hash join with an in-memory hash table on the build side
 * and a streaming probe side.
 * Supports INNER, LEFT, RIGHT, and FULL outer joins.
 * Memory scales strictly with the build table size; the probe table streams in O(1) memory.
 */
public class HashJoinOperator implements PhysicalOperator {
    private final PhysicalOperator left;
    private final PhysicalOperator right;
    private final String leftColumn;
    private final String rightColumn;
    private final JoinClause.JoinType joinType;
    private final List<String> leftColumns;
    private final List<String> rightColumns;
    private final List<String> outputColumns;

    // Build side is right by default (for INNER, LEFT, FULL); for RIGHT join, build side is left
    private final boolean buildOnLeft;
    private final PhysicalOperator buildSide;
    private final PhysicalOperator probeSide;
    private final String buildColumn;
    private final String probeColumn;

    private Map<Object, List<Tuple>> hashTable = null;
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
                            String leftColumn,
                            String rightColumn,
                            JoinClause.JoinType joinType) {
        this.left = left;
        this.right = right;
        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.joinType = joinType != null ? joinType : JoinClause.JoinType.INNER;
        this.leftColumns = left != null ? left.getOutputColumns() : List.of();
        this.rightColumns = right != null ? right.getOutputColumns() : List.of();
        this.outputColumns = JoinUtils.combineColumns(leftColumns, rightColumns);

        // For RIGHT JOIN, build side is left (so right streams as probe)
        // For LEFT, FULL, and INNER joins, build side is right (so left streams as probe)
        this.buildOnLeft = (this.joinType == JoinClause.JoinType.RIGHT);
        this.buildSide = buildOnLeft ? left : right;
        this.probeSide = buildOnLeft ? right : left;
        this.buildColumn = buildOnLeft ? leftColumn : rightColumn;
        this.probeColumn = buildOnLeft ? rightColumn : leftColumn;
    }

    @Override
    public void open() {
        close();
        hashTable = new HashMap<>();
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
                Object key = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(tuple, buildColumn));
                if (key != null) {
                    hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(tuple);
                } else if (nullKeyBuildTuples != null) {
                    nullKeyBuildTuples.add(tuple);
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
            Object probeKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(currentProbeTuple, probeColumn));

            if (probeKey == null) {
                // NULL never matches in equi-join
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    // Probe is left
                    slot.set(JoinUtils.mergeWithNullRight(currentProbeTuple, rightColumns));
                    return slot;
                } else if (joinType == JoinClause.JoinType.RIGHT) {
                    // Probe is right
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumns, currentProbeTuple));
                    return slot;
                }
                // For INNER join, skip null probe key
                continue;
            }

            List<Tuple> matches = hashTable.get(probeKey);
            if (matches != null && !matches.isEmpty()) {
                currentMatches = matches;
                currentMatchIndex = 0;
                // Loop will pick up currentMatches at top and emit first match
            } else {
                currentMatches = null;
                // No match for this probe tuple
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    slot.set(JoinUtils.mergeWithNullRight(currentProbeTuple, rightColumns));
                    return slot;
                } else if (joinType == JoinClause.JoinType.RIGHT) {
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumns, currentProbeTuple));
                    return slot;
                }
            }
        }

        // Probe side completed - for FULL OUTER JOIN, emit remaining unmatched build side tuples
        if (joinType == JoinClause.JoinType.FULL) {
            if (unmatchedBuildIterator == null && hashTable != null) {
                List<Tuple> allUnmatched = new ArrayList<>();
                for (List<Tuple> list : hashTable.values()) {
                    for (Tuple t : list) {
                        if (matchedBuildTuples == null || !matchedBuildTuples.contains(t)) {
                            allUnmatched.add(t);
                        }
                    }
                }
                if (nullKeyBuildTuples != null) {
                    allUnmatched.addAll(nullKeyBuildTuples);
                }
                unmatchedBuildIterator = allUnmatched.iterator();
            }

            if (unmatchedBuildIterator != null && unmatchedBuildIterator.hasNext()) {
                Tuple unmatchedBuild = unmatchedBuildIterator.next();
                // Build side was right
                slot.set(JoinUtils.mergeWithNullLeft(leftColumns, unmatchedBuild));
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
        return leftColumn;
    }

    public String getRightColumn() {
        return rightColumn;
    }
}
