package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.AggregateCall;
import com.stratosdb.sql.ast.WhereExpr;
import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Volcano physical operator for computing aggregates over pre-sorted inputs in O(1) memory.
 * Emits a completed group immediately whenever the GROUP BY key changes, avoiding
 * materialization of all groups into an in-memory hash table.
 */
public class StreamAggregateOperator implements PhysicalOperator {

    private final PhysicalOperator child;
    private final List<String> groupBy;
    private final List<AggregateCall> aggregates;
    private final String havingClause;
    private final WhereExpr havingExpr;
    private final List<AggregateCall> allAggregates;
    private final List<String> outputColumns;

    private List<Object> currentGroupKey;
    private GroupAccumulator currentAccumulator;
    private TupleSlot pendingChildSlot;
    private boolean childEof;
    private boolean hasSeenRows;
    private boolean emptyGroupEmitted;

    public StreamAggregateOperator(PhysicalOperator child, List<String> groupBy, List<AggregateCall> aggregates, String havingClause) {
        this(child, groupBy, aggregates, havingClause, null, null);
    }

    public StreamAggregateOperator(PhysicalOperator child, List<String> groupBy, List<AggregateCall> aggregates,
                                   String havingClause, WhereExpr havingExpr) {
        this(child, groupBy, aggregates, havingClause, havingExpr, null);
    }

    public StreamAggregateOperator(PhysicalOperator child, List<String> groupBy, List<AggregateCall> aggregates,
                                   String havingClause, WhereExpr havingExpr, List<AggregateCall> havingAggregates) {
        this.child = child;
        this.groupBy = groupBy != null ? new ArrayList<>(groupBy) : List.of();
        this.aggregates = aggregates != null ? new ArrayList<>(aggregates) : List.of();
        this.havingClause = havingClause;
        this.havingExpr = havingExpr;
        this.outputColumns = buildOutputColumns();

        List<AggregateCall> all = new ArrayList<>(this.aggregates);
        if (havingAggregates != null) {
            for (AggregateCall ha : havingAggregates) {
                if (all.stream().noneMatch(a -> a.canonicalForm().equalsIgnoreCase(ha.canonicalForm()))) {
                    all.add(ha);
                }
            }
        }
        this.allAggregates = all;
    }

    private List<String> buildOutputColumns() {
        List<String> cols = new ArrayList<>();
        cols.addAll(groupBy);
        for (AggregateCall agg : aggregates) {
            cols.add(agg.displayName());
        }
        if (cols.isEmpty() && child != null) {
            return child.getOutputColumns();
        }
        return cols;
    }

    @Override
    public void open() {
        child.open();
        currentGroupKey = null;
        currentAccumulator = null;
        hasSeenRows = false;
        emptyGroupEmitted = false;
        pendingChildSlot = child.next();
        childEof = (pendingChildSlot == null || pendingChildSlot.isEmpty());
    }

    @Override
    public TupleSlot next() {
        while (true) {
            if (childEof) {
                // If we have an active in-flight group, finalize and emit it
                if (currentAccumulator != null) {
                    Tuple out = buildOutputTuple(currentGroupKey, currentAccumulator);
                    GroupAccumulator finalizedAcc = currentAccumulator;
                    currentAccumulator = null;
                    if (passesHaving(out, finalizedAcc)) {
                        return TupleSlot.of(out);
                    }
                    continue;
                }

                // If input stream was completely empty and no GROUP BY was specified, emit single default row
                if (!hasSeenRows && !emptyGroupEmitted && groupBy.isEmpty()) {
                    emptyGroupEmitted = true;
                    GroupAccumulator emptyAcc = new GroupAccumulator(allAggregates);
                    Tuple out = buildOutputTuple(List.of(), emptyAcc);
                    if (passesHaving(out, emptyAcc)) {
                        return TupleSlot.of(out);
                    }
                }

                return TupleSlot.empty();
            }

            Tuple row = pendingChildSlot.getTuple();
            hasSeenRows = true;
            List<Object> rowKey = extractGroupKey(row);

            if (currentAccumulator == null) {
                // First group initialized
                currentGroupKey = rowKey;
                currentAccumulator = new GroupAccumulator(allAggregates);
                currentAccumulator.update(row);
                advanceChild();
            } else if (keysEqual(currentGroupKey, rowKey)) {
                // Same group continuation
                currentAccumulator.update(row);
                advanceChild();
            } else {
                // Group boundary encountered! Finalize previous group and initialize new group
                Tuple out = buildOutputTuple(currentGroupKey, currentAccumulator);
                GroupAccumulator finalizedAcc = currentAccumulator;
                currentGroupKey = rowKey;
                currentAccumulator = new GroupAccumulator(allAggregates);
                currentAccumulator.update(row);
                advanceChild();

                if (passesHaving(out, finalizedAcc)) {
                    return TupleSlot.of(out);
                }
                // If HAVING rejected it, continue loop to produce next group
            }
        }
    }

    private void advanceChild() {
        pendingChildSlot = child.next();
        if (pendingChildSlot == null || pendingChildSlot.isEmpty()) {
            childEof = true;
        }
    }

    private List<Object> extractGroupKey(Tuple tuple) {
        if (groupBy.isEmpty()) {
            return List.of();
        }
        List<Object> key = new ArrayList<>(groupBy.size());
        for (String col : groupBy) {
            key.add(JoinUtils.findColumnValue(tuple, col));
        }
        return key;
    }

    private boolean keysEqual(List<Object> k1, List<Object> k2) {
        if (k1 == null && k2 == null) return true;
        if (k1 == null || k2 == null) return false;
        if (k1.size() != k2.size()) return false;
        for (int i = 0; i < k1.size(); i++) {
            if (!Objects.equals(k1.get(i), k2.get(i))) {
                return false;
            }
        }
        return true;
    }

    private Tuple buildOutputTuple(List<Object> key, GroupAccumulator acc) {
        Tuple out = new Tuple();
        for (int i = 0; i < groupBy.size(); i++) {
            Object val = (key != null && i < key.size()) ? key.get(i) : null;
            out.addValue(groupBy.get(i), val);
        }
        for (int i = 0; i < aggregates.size(); i++) {
            AggregateCall agg = aggregates.get(i);
            int idx = allAggregates.indexOf(agg);
            out.addValue(agg.displayName(), idx >= 0 ? acc.getValue(idx) : acc.getValue(i));
        }
        return out;
    }

    private boolean passesHaving(Tuple outTuple, GroupAccumulator acc) {
        if (havingExpr == null && (havingClause == null || havingClause.isBlank())) {
            return true;
        }
        Map<String, Object> aggValues = new HashMap<>();
        for (int i = 0; i < allAggregates.size(); i++) {
            AggregateCall agg = allAggregates.get(i);
            Object val = acc.getValue(i);
            aggValues.put(agg.canonicalForm(), val);
            aggValues.put(agg.displayName(), val);
            aggValues.put(agg.function() + "(" + agg.argument() + ")", val);
        }
        if (havingExpr != null) {
            return SortAndAggregateUtils.evaluateHaving(havingExpr, aggValues, outTuple);
        }
        return SortAndAggregateUtils.matchesHaving(havingClause, aggValues);
    }

    @Override
    public void close() {
        child.close();
        currentAccumulator = null;
        currentGroupKey = null;
        pendingChildSlot = null;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }

    @Override
    public PhysicalOperator getChild() {
        return child;
    }
}
