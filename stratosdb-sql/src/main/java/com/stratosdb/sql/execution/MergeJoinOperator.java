package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.JoinClause;
import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical operator that implements sort-merge join over presorted streams.
 * Streams both left and right inputs in lockstep, buffering only equal-key runs.
 * Supports INNER, LEFT, RIGHT, and FULL outer joins in O(1) memory for distinct keys.
 */
public class MergeJoinOperator implements PhysicalOperator {
    private final PhysicalOperator left;
    private final PhysicalOperator right;
    private final String leftColumn;
    private final String rightColumn;
    private final JoinClause.JoinType joinType;
    private final List<String> leftColumns;
    private final List<String> rightColumns;
    private final List<String> outputColumns;

    private Tuple currentLeft = null;
    private Tuple currentRight = null;
    private final List<Tuple> rightRun = new ArrayList<>();
    private int rightRunIndex = 0;
    private Object currentRunKey = null;
    private boolean inRun = false;
    private final TupleSlot slot = new TupleSlot();
    private boolean open = false;

    public MergeJoinOperator(PhysicalOperator left,
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
        if (this.joinType == JoinClause.JoinType.SEMI || this.joinType == JoinClause.JoinType.ANTI) {
            this.outputColumns = List.copyOf(leftColumns);
        } else {
            this.outputColumns = JoinUtils.combineColumns(leftColumns, rightColumns);
        }
    }

    @Override
    public void open() {
        close();
        if (left != null) left.open();
        if (right != null) right.open();

        currentLeft = advance(left);
        currentRight = advance(right);
        rightRun.clear();
        rightRunIndex = 0;
        currentRunKey = null;
        inRun = false;
        open = true;
    }

    private Tuple advance(PhysicalOperator op) {
        if (op == null) return null;
        TupleSlot s = op.next();
        return (s != null && !s.isEmpty()) ? s.getTuple() : null;
    }

    @Override
    public TupleSlot next() {
        if (!open) {
            slot.clear();
            return slot;
        }

        if (joinType == JoinClause.JoinType.SEMI) {
            while (currentLeft != null && currentRight != null) {
                Object leftKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(currentLeft, leftColumn));
                Object rightKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(currentRight, rightColumn));
                if (leftKey == null) {
                    currentLeft = advance(left);
                    continue;
                }
                if (rightKey == null) {
                    currentRight = advance(right);
                    continue;
                }
                int cmp = JoinUtils.compareKeys(leftKey, rightKey);
                if (cmp == 0) {
                    Tuple emit = currentLeft;
                    currentLeft = advance(left);
                    slot.set(emit);
                    return slot;
                } else if (cmp < 0) {
                    currentLeft = advance(left);
                } else {
                    currentRight = advance(right);
                }
            }
            slot.clear();
            return slot;
        }

        if (joinType == JoinClause.JoinType.ANTI) {
            while (currentLeft != null) {
                if (currentRight == null) {
                    Tuple emit = currentLeft;
                    currentLeft = advance(left);
                    slot.set(emit);
                    return slot;
                }
                Object leftKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(currentLeft, leftColumn));
                Object rightKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(currentRight, rightColumn));
                if (leftKey == null) {
                    Tuple emit = currentLeft;
                    currentLeft = advance(left);
                    slot.set(emit);
                    return slot;
                }
                if (rightKey == null) {
                    currentRight = advance(right);
                    continue;
                }
                int cmp = JoinUtils.compareKeys(leftKey, rightKey);
                if (cmp < 0) {
                    Tuple emit = currentLeft;
                    currentLeft = advance(left);
                    slot.set(emit);
                    return slot;
                } else if (cmp == 0) {
                    currentLeft = advance(left);
                } else {
                    currentRight = advance(right);
                }
            }
            slot.clear();
            return slot;
        }

        while (true) {
            if (inRun) {
                if (rightRunIndex < rightRun.size()) {
                    Tuple r = rightRun.get(rightRunIndex++);
                    slot.set(JoinUtils.merge(currentLeft, r));
                    return slot;
                } else {
                    // Exhausted rightRun for currentLeft - advance left to check for another matching left tuple
                    currentLeft = advance(left);
                    if (currentLeft != null) {
                        Object nextLKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(currentLeft, leftColumn));
                        if (nextLKey != null && JoinUtils.compareKeys(nextLKey, currentRunKey) == 0) {
                            rightRunIndex = 0;
                            // Continue loop to emit matches for this next left tuple
                            continue;
                        }
                    }
                    // Run completed for all matching left tuples
                    inRun = false;
                    rightRun.clear();
                    currentRunKey = null;
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
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumns, r));
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
                    slot.set(JoinUtils.mergeWithNullRight(l, rightColumns));
                    return slot;
                } else {
                    slot.clear();
                    return slot;
                }
            }

            // Both currentLeft and currentRight are present
            Object lKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(currentLeft, leftColumn));
            Object rKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(currentRight, rightColumn));

            if (lKey == null) {
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    Tuple l = currentLeft;
                    currentLeft = advance(left);
                    slot.set(JoinUtils.mergeWithNullRight(l, rightColumns));
                    return slot;
                } else {
                    currentLeft = advance(left);
                    continue;
                }
            }

            if (rKey == null) {
                if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
                    Tuple r = currentRight;
                    currentRight = advance(right);
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumns, r));
                    return slot;
                } else {
                    currentRight = advance(right);
                    continue;
                }
            }

            int cmp = JoinUtils.compareKeys(lKey, rKey);
            if (cmp < 0) {
                if (joinType == JoinClause.JoinType.LEFT || joinType == JoinClause.JoinType.FULL) {
                    Tuple l = currentLeft;
                    currentLeft = advance(left);
                    slot.set(JoinUtils.mergeWithNullRight(l, rightColumns));
                    return slot;
                } else {
                    currentLeft = advance(left);
                    continue;
                }
            } else if (cmp > 0) {
                if (joinType == JoinClause.JoinType.RIGHT || joinType == JoinClause.JoinType.FULL) {
                    Tuple r = currentRight;
                    currentRight = advance(right);
                    slot.set(JoinUtils.mergeWithNullLeft(leftColumns, r));
                    return slot;
                } else {
                    currentRight = advance(right);
                    continue;
                }
            } else {
                // Match found! Build rightRun of all right tuples matching current key
                currentRunKey = rKey;
                rightRun.clear();
                rightRun.add(currentRight);

                while (true) {
                    Tuple nextR = advance(right);
                    if (nextR == null) {
                        currentRight = null;
                        break;
                    }
                    Object nextKey = JoinUtils.normalizeJoinKey(JoinUtils.findColumnValue(nextR, rightColumn));
                    if (nextKey != null && JoinUtils.compareKeys(nextKey, currentRunKey) == 0) {
                        rightRun.add(nextR);
                    } else {
                        currentRight = nextR;
                        break;
                    }
                }

                inRun = true;
                rightRunIndex = 0;
                // Loop continues and hits if (inRun) at top, emitting first match
            }
        }
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
        currentRunKey = null;
        inRun = false;
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
