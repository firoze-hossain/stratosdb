package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.AggregateCall;
import com.stratosdb.storage.page.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Volcano physical operator that evaluates GROUP BY and aggregate functions (COUNT, SUM, AVG, MIN, MAX)
 * along with HAVING filter predicates using an in-memory hash table of lightweight streaming accumulators.
 */
public class HashAggregateOperator implements PhysicalOperator {

    private final PhysicalOperator child;
    private final List<String> groupBy;
    private final List<AggregateCall> aggregates;
    private final String havingClause;
    private final List<String> outputColumns;

    private List<Tuple> resultRows;
    private java.util.Iterator<Tuple> resultIterator;

    public HashAggregateOperator(PhysicalOperator child, List<String> groupBy, List<AggregateCall> aggregates, String havingClause) {
        this.child = child;
        this.groupBy = groupBy != null ? new ArrayList<>(groupBy) : List.of();
        this.aggregates = aggregates != null ? new ArrayList<>(aggregates) : List.of();
        this.havingClause = havingClause;
        this.outputColumns = buildOutputColumns();
        this.resultRows = new ArrayList<>();
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
        resultRows.clear();

        Map<List<Object>, GroupAccumulator> groups = new LinkedHashMap<>();
        while (true) {
            TupleSlot slot = child.next();
            if (slot == null || slot.isEmpty()) {
                break;
            }
            Tuple tuple = slot.getTuple();
            List<Object> key = extractGroupKey(tuple);
            GroupAccumulator acc = groups.computeIfAbsent(key, k -> new GroupAccumulator(aggregates));
            acc.update(tuple);
        }

        // Implicit group for empty table when no GROUP BY clause is specified
        if (groups.isEmpty() && groupBy.isEmpty()) {
            groups.put(List.of(), new GroupAccumulator(aggregates));
        }

        for (Map.Entry<List<Object>, GroupAccumulator> entry : groups.entrySet()) {
            List<Object> groupKey = entry.getKey();
            GroupAccumulator acc = entry.getValue();

            Tuple out = new Tuple();
            for (int i = 0; i < groupBy.size(); i++) {
                out.addValue(groupBy.get(i), groupKey.get(i));
            }

            Map<String, Object> aggValues = new HashMap<>();
            for (int i = 0; i < aggregates.size(); i++) {
                AggregateCall agg = aggregates.get(i);
                Object val = acc.getValue(i);
                out.addValue(agg.displayName(), val);
                aggValues.put(agg.canonicalForm(), val);
            }

            if (havingClause != null && !SortAndAggregateUtils.matchesHaving(havingClause, aggValues)) {
                continue;
            }

            resultRows.add(out);
        }

        resultIterator = resultRows.iterator();
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

    @Override
    public TupleSlot next() {
        if (resultIterator != null && resultIterator.hasNext()) {
            return TupleSlot.of(resultIterator.next());
        }
        return TupleSlot.empty();
    }

    @Override
    public void close() {
        child.close();
        if (resultRows != null) {
            resultRows.clear();
        }
        resultIterator = null;
    }

    @Override
    public List<String> getOutputColumns() {
        return outputColumns;
    }
}
