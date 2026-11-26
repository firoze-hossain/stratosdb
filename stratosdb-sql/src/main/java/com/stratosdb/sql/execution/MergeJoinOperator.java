package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.storage.page.Tuple;

import java.util.*;
import java.util.function.BiPredicate;

/**
 * Physical operator that implements sort-merge join over presorted streams.
 * Supports multi-column composite equi-keys, residual non-equi filter predicates,
 * and INNER, LEFT, RIGHT, FULL, SEMI, and ANTI outer joins in O(1) memory for distinct keys.
 */
public class MergeJoinOperator implements PhysicalOperator {
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

    private Tuple currentLeft = null;
    private Tuple currentRight = null;
    private final List<Tuple> rightRun = new ArrayList<>();
    private final List<Tuple> matchedPairsInRun = new ArrayList<>();
    private int matchedPairsIndex = 0;
    private final Set<Tuple> matchedRightTuplesInRun = Collections.newSetFromMap(new IdentityHashMap<>());
    private JoinUtils.CompositeKey currentRunKey = null;
    private boolean inRun = false;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    // Fallback for pure non-equi merge join
    private List<Tuple> allRightTuples = null;
    private int allRightIndex = 0;
    private boolean currentLeftMatchedInNonEqui = false;
    private Set<Tuple> matchedRightTuplesNonEqui = null;
    private Iterator<Tuple> unmatchedRightNonEquiIter = null;

    public MergeJoinOperator(PhysicalOperator left,
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
            this.outputColumns = List.copyOf(leftColumnsList);
        } else {
            this.outputColumns = JoinUtils.combineColumns(leftColumnsList, rightColumnsList);
        }
    }

    public MergeJoinOperator(PhysicalOperator left,
                             PhysicalOperator right,
                             List<String> leftColumns,
                             List<String> rightColumns,
                             WhereExpr nonEquiFilter,
                             JoinClause.JoinType joinType) {
        this(left, right, leftColumns, rightColumns, nonEquiFilter, null, joinType);
    }

    public MergeJoinOperator(PhysicalOperator left,
                             PhysicalOperator right,
                             List<String> leftColumns,
                             List<String> rightColumns,
                             BiPredicate<Tuple, Tuple> nonEquiCondition,
                             JoinClause.JoinType joinType) {
        this(left, right, leftColumns, rightColumns, null, nonEquiCondition, joinType);
    }

    public MergeJoinOperator(PhysicalOperator left,
                             PhysicalOperator right,
                             List<String> leftColumns,
                             List<String> rightColumns,
                             JoinClause.JoinType joinType) {
        this(left, right, leftColumns, rightColumns, null, null, joinType);
    }

    public MergeJoinOperator(PhysicalOperator left,
                             PhysicalOperator right,
                             String leftColumn,
                             String rightColumn,
                             JoinClause.JoinType joinType) {
        this(left, right,
             leftColumn != null ? List.of(leftColumn) : List.of(),
             rightColumn != null ? List.of(rightColumn) : List.of(),
             null, null, joinType);
    }

    public MergeJoinOperator(PhysicalOperator left,
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
        if (left != null) left.open();
        if (right != null) right.open();

        if (leftColumns.isEmpty() || rightColumns.isEmpty()) {
            // Pure non-equi fallback: buffer right side
            allRightTuples = new ArrayList<>();
            if (right != null) {
                while (true) {
                    TupleSlot s = right.next();
                    if (s == null || s.isEmpty()) break;
                    allRightTuples.add(s.getTuple());
                }
            }
            if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
                matchedRightTuplesNonEqui = Collections.newSetFromMap(new IdentityHashMap<>());
            }
            currentLeft = advance(left);
            allRightIndex = 0;
            currentLeftMatchedInNonEqui = false;
        } else {
            currentLeft = advance(left);
            currentRight = advance(right);
            rightRun.clear();
            matchedPairsInRun.clear();
            matchedPairsIndex = 0;
            matchedRightTuplesInRun.clear();
            currentRunKey = null;
            inRun = false;
        }
        open = true;
    }

    private Tuple advance(PhysicalOperator op) {
        if (op == null) return null;
        TupleSlot s = op.next();
        return (s != null && !s.isEmpty()) ? s.getTuple() : null;
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

        // Handle pure non-equi merge join fallback
        if (leftColumns.isEmpty() || rightColumns.isEmpty()) {
            return nextPureNonEqui();
        }

        while (true) {
            if (inRun) {
                if (matchedPairsIndex < matchedPairsInRun.size()) {
                    slot.set(matchedPairsInRun.get(matchedPairsIndex++));
                    return slot;
                } else {
                    // Completed pairs for current left tuple; advance to next left tuple
                    currentLeft = advance(left);
                    if (currentLeft != null) {
                        JoinUtils.CompositeKey nextLKey = JoinUtils.extractCompositeKey(currentLeft, leftColumns);
                        if (!nextLKey.hasNull() && nextLKey.equals(currentRunKey)) {
                            // Another left tuple with identical run key!
                            prepareRunMatchesForCurrentLeft();
                            continue;
                        }
                    }

                    // Run completed across all matching left tuples!
                    matchedPairsInRun.clear();
                    matchedPairsIndex = 0;

                    // For RIGHT and FULL outer joins, emit any right tuples from rightRun that never matched
                    if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
                        for (Tuple r : rightRun) {
                            if (!matchedRightTuplesInRun.contains(r)) {
                                matchedPairsInRun.add(JoinUtils.mergeWithNullLeft(leftColumnsList, r));
                            }
                        }
                    }

                    rightRun.clear();
                    matchedRightTuplesInRun.clear();
                    currentRunKey = null;

                    if (!matchedPairsInRun.isEmpty()) {
                        // Keep inRun = true so any remaining unmatched right tuples are emitted
                        slot.set(matchedPairsInRun.get(matchedPairsIndex++));
                        return slot;
                    }
                    inRun = false;
                }
            }

            // Normal lockstep merge advance
            if (currentLeft == null && currentRight == null) {
                slot.clear();
                return slot;
            }

            if (currentLeft == null) {
                // Left exhausted
                if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
                    Tuple r = currentRight;
                    currentRight = advance(right);
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumnsList, r));
                    return slot;
                } else {
                    slot.clear();
                    return slot;
                }
            }

            if (currentRight == null) {
                // Right exhausted
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    Tuple l = currentLeft;
                    currentLeft = advance(left);
                    slot.set(JoinUtils.mergeWithNullRight(l, rightColumnsList));
                    return slot;
                } else if (joinType == JoinClause.JoinType.ANTI) {
                    Tuple l = currentLeft;
                    currentLeft = advance(left);
                    slot.set(l);
                    return slot;
                } else {
                    slot.clear();
                    return slot;
                }
            }

            // Both currentLeft and currentRight are present
            JoinUtils.CompositeKey lKey = JoinUtils.extractCompositeKey(currentLeft, leftColumns);
            JoinUtils.CompositeKey rKey = JoinUtils.extractCompositeKey(currentRight, rightColumns);

            if (lKey.hasNull()) {
                Tuple l = currentLeft;
                currentLeft = advance(left);
                if (joinType == JoinClause.JoinType.ANTI) {
                    slot.set(l);
                    return slot;
                }
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    slot.set(JoinUtils.mergeWithNullRight(l, rightColumnsList));
                    return slot;
                }
                continue;
            }

            if (rKey.hasNull()) {
                Tuple r = currentRight;
                currentRight = advance(right);
                if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumnsList, r));
                    return slot;
                }
                continue;
            }

            int cmp = JoinUtils.compareCompositeKeys(currentLeft, leftColumns, currentRight, rightColumns);
            if (cmp < 0) {
                Tuple l = currentLeft;
                currentLeft = advance(left);
                if (joinType == JoinClause.JoinType.ANTI) {
                    slot.set(l);
                    return slot;
                }
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    slot.set(JoinUtils.mergeWithNullRight(l, rightColumnsList));
                    return slot;
                }
                continue;
            } else if (cmp > 0) {
                Tuple r = currentRight;
                currentRight = advance(right);
                if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumnsList, r));
                    return slot;
                }
                continue;
            } else {
                // Match found! Build rightRun of all right tuples matching current key
                currentRunKey = rKey;
                rightRun.clear();
                matchedRightTuplesInRun.clear();
                rightRun.add(currentRight);

                while (true) {
                    Tuple nextR = advance(right);
                    if (nextR == null) {
                        currentRight = null;
                        break;
                    }
                    JoinUtils.CompositeKey nextKey = JoinUtils.extractCompositeKey(nextR, rightColumns);
                    if (!nextKey.hasNull() && nextKey.equals(currentRunKey)) {
                        rightRun.add(nextR);
                    } else {
                        currentRight = nextR;
                        break;
                    }
                }

                inRun = true;
                prepareRunMatchesForCurrentLeft();
                // Loop continues and yields from matchedPairsInRun at top
            }
        }
    }

    private void prepareRunMatchesForCurrentLeft() {
        matchedPairsInRun.clear();
        matchedPairsIndex = 0;

        boolean anyMatched = false;
        for (Tuple r : rightRun) {
            if (testCondition(currentLeft, r)) {
                anyMatched = true;
                matchedRightTuplesInRun.add(r);
                if (joinType != JoinClause.JoinType.SEMI && joinType != JoinClause.JoinType.ANTI) {
                    matchedPairsInRun.add(JoinUtils.merge(currentLeft, r));
                }
            }
        }

        if (joinType == JoinClause.JoinType.SEMI) {
            if (anyMatched) {
                matchedPairsInRun.add(currentLeft);
            }
        } else if (joinType == JoinClause.JoinType.ANTI) {
            if (!anyMatched) {
                matchedPairsInRun.add(currentLeft);
            }
        } else if (!anyMatched && (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL)) {
            matchedPairsInRun.add(JoinUtils.mergeWithNullRight(currentLeft, rightColumnsList));
        }
    }

    private TupleSlot nextPureNonEqui() {
        while (currentLeft != null) {
            if (joinType == JoinClause.JoinType.SEMI) {
                boolean matched = false;
                while (allRightIndex < allRightTuples.size()) {
                    Tuple r = allRightTuples.get(allRightIndex++);
                    if (testCondition(currentLeft, r)) {
                        matched = true;
                        break;
                    }
                }
                Tuple emit = currentLeft;
                currentLeft = advance(left);
                allRightIndex = 0;
                if (matched) {
                    slot.set(emit);
                    return slot;
                }
                continue;
            }

            if (joinType == JoinClause.JoinType.ANTI) {
                boolean matched = false;
                while (allRightIndex < allRightTuples.size()) {
                    Tuple r = allRightTuples.get(allRightIndex++);
                    if (testCondition(currentLeft, r)) {
                        matched = true;
                        break;
                    }
                }
                Tuple emit = currentLeft;
                currentLeft = advance(left);
                allRightIndex = 0;
                if (!matched) {
                    slot.set(emit);
                    return slot;
                }
                continue;
            }

            while (allRightIndex < allRightTuples.size()) {
                Tuple r = allRightTuples.get(allRightIndex++);
                if (testCondition(currentLeft, r)) {
                    currentLeftMatchedInNonEqui = true;
                    if (matchedRightTuplesNonEqui != null) {
                        matchedRightTuplesNonEqui.add(r);
                    }
                    slot.set(JoinUtils.merge(currentLeft, r));
                    return slot;
                }
            }

            Tuple finishedLeft = currentLeft;
            boolean hadMatch = currentLeftMatchedInNonEqui;

            currentLeft = advance(left);
            allRightIndex = 0;
            currentLeftMatchedInNonEqui = false;

            if (!hadMatch && (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL)) {
                slot.set(JoinUtils.mergeWithNullRight(finishedLeft, rightColumnsList));
                return slot;
            }
        }

        // Emit unmatched right tuples for RIGHT or FULL outer join
        if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
            if (unmatchedRightNonEquiIter == null && allRightTuples != null) {
                List<Tuple> un = new ArrayList<>();
                for (Tuple r : allRightTuples) {
                    if (matchedRightTuplesNonEqui == null || !matchedRightTuplesNonEqui.contains(r)) {
                        un.add(r);
                    }
                }
                unmatchedRightNonEquiIter = un.iterator();
            }
            if (unmatchedRightNonEquiIter != null && unmatchedRightNonEquiIter.hasNext()) {
                slot.set(JoinUtils.mergeWithNullLeft(leftColumnsList, unmatchedRightNonEquiIter.next()));
                return slot;
            }
        }

        slot.clear();
        return slot;
    }

    @Override
    public void close() {
        if (left != null) {
            try { left.close(); } catch (Exception ignored) {}
        }
        if (right != null) {
            try { right.close(); } catch (Exception ignored) {}
        }
        currentLeft = null;
        currentRight = null;
        rightRun.clear();
        matchedPairsInRun.clear();
        matchedRightTuplesInRun.clear();
        currentRunKey = null;
        inRun = false;
        if (allRightTuples != null) {
            allRightTuples.clear();
            allRightTuples = null;
        }
        if (matchedRightTuplesNonEqui != null) {
            matchedRightTuplesNonEqui.clear();
            matchedRightTuplesNonEqui = null;
        }
        unmatchedRightNonEquiIter = null;
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
