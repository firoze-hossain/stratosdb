package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * Physical operator that implements nested loop joins for INNER, LEFT, RIGHT, and FULL outer joins.
 * Outer stream is evaluated tuple-by-tuple; inner tuples are matched against the join predicate.
 */
public class NestedLoopJoinOperator implements PhysicalOperator {
    private final PhysicalOperator left;
    private final PhysicalOperator right;
    private final BiPredicate<Tuple, Tuple> condition;
    private final JoinClause.JoinType joinType;
    private final List<String> leftColumns;
    private final List<String> rightColumns;
    private final List<String> outputColumns;

    private List<Tuple> innerRows = null;
    private boolean[] matchedInner = null;
    private Tuple currentLeftTuple = null;
    private int currentInnerIndex = 0;
    private boolean leftMatched = false;
    private int unmatchedInnerEmitIndex = 0;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public NestedLoopJoinOperator(PhysicalOperator left,
                                  PhysicalOperator right,
                                  String leftColumn,
                                  String rightColumn,
                                  JoinClause.JoinType joinType) {
        this(left, right, (l, r) -> {
            Object lVal = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(l, leftColumn));
            Object rVal = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(r, rightColumn));
            return lVal != null && rVal != null && JoinUtils.compareKeys(lVal, rVal) == 0;
        }, joinType);
    }

    public NestedLoopJoinOperator(PhysicalOperator left,
                                  PhysicalOperator right,
                                  BiPredicate<Tuple, Tuple> condition,
                                  JoinClause.JoinType joinType) {
        this.left = left;
        this.right = right;
        this.condition = condition;
        this.joinType = joinType != null ? joinType : JoinClause.JoinType.INNER;
        this.leftColumns = left != null ? left.getOutputColumns() : List.of();
        this.rightColumns = right != null ? right.getOutputColumns() : List.of();
        this.outputColumns = JoinUtils.combineColumns(leftColumns, rightColumns);
    }

    @Override
    public void open() {
        close();
        if (left != null) left.open();
        if (right != null) {
            right.open();
            innerRows = new ArrayList<>();
            while (true) {
                TupleSlot innerSlot = right.next();
                if (innerSlot == null || innerSlot.isEmpty()) break;
                innerRows.add(innerSlot.getTuple());
            }
            right.close();
            matchedInner = new boolean[innerRows.size()];
        } else {
            innerRows = List.of();
            matchedInner = new boolean[0];
        }

        currentLeftTuple = advanceLeft();
        currentInnerIndex = 0;
        leftMatched = false;
        unmatchedInnerEmitIndex = 0;
        open = true;
    }

    private Tuple advanceLeft() {
        if (left == null) return null;
        TupleSlot s = left.next();
        return (s != null && !s.isEmpty()) ? s.getTuple() : null;
    }

    @Override
    public TupleSlot next() {
        if (!open) {
            slot.clear();
            return slot;
        }

        while (currentLeftTuple != null) {
            while (currentInnerIndex < innerRows.size()) {
                Tuple innerTuple = innerRows.get(currentInnerIndex);
                int innerIdx = currentInnerIndex++;
                if (condition == null || condition.test(currentLeftTuple, innerTuple)) {
                    leftMatched = true;
                    if (matchedInner != null && innerIdx < matchedInner.length) {
                        matchedInner[innerIdx] = true;
                    }
                    slot.set(JoinUtils.merge(currentLeftTuple, innerTuple));
                    return slot;
                }
            }

            // Inner finished for current left tuple
            Tuple justFinishedLeft = currentLeftTuple;
            boolean hadMatch = leftMatched;

            // Advance to next left tuple
            currentLeftTuple = advanceLeft();
            currentInnerIndex = 0;
            leftMatched = false;

            if (!hadMatch && (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL)) {
                slot.set(JoinUtils.mergeWithNullRight(justFinishedLeft, rightColumns));
                return slot;
            }
        }

        // Left side exhausted - for RIGHT and FULL outer joins, emit unmatched inner rows
        if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
            while (unmatchedInnerEmitIndex < innerRows.size()) {
                int idx = unmatchedInnerEmitIndex++;
                if (matchedInner != null && !matchedInner[idx]) {
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumns, innerRows.get(idx)));
                    return slot;
                }
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
        innerRows = null;
        matchedInner = null;
        currentLeftTuple = null;
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
}
