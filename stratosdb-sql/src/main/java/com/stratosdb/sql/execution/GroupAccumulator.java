package com.stratosdb.sql.execution;

import com.stratosdb.sql.ast.AggregateCall;
import com.stratosdb.storage.page.Tuple;

import java.util.List;

/**
 * Stateful group accumulator that tracks running aggregates (COUNT, SUM, AVG, MIN, MAX)
 * for a single GROUP BY key without retaining rows in memory.
 */
public class GroupAccumulator {

    private final List<AggregateCall> aggregates;
    private final AccumulatorState[] states;

    public GroupAccumulator(List<AggregateCall> aggregates) {
        this.aggregates = aggregates != null ? aggregates : List.of();
        this.states = new AccumulatorState[this.aggregates.size()];
        for (int i = 0; i < this.states.length; i++) {
            this.states[i] = createAccumulatorState(this.aggregates.get(i));
        }
    }

    public void update(Tuple tuple) {
        for (AccumulatorState state : states) {
            state.update(tuple);
        }
    }

    public Object getValue(int index) {
        if (index < 0 || index >= states.length) return null;
        return states[index].result();
    }

    public List<AggregateCall> getAggregates() {
        return aggregates;
    }

    private static AccumulatorState createAccumulatorState(AggregateCall agg) {
        String func = agg.function().toUpperCase();
        String arg = agg.argument();
        return switch (func) {
            case "COUNT" -> new CountAccumulator(arg);
            case "SUM" -> new SumAccumulator(arg);
            case "AVG" -> new AvgAccumulator(arg);
            case "MIN" -> new MinAccumulator(arg);
            case "MAX" -> new MaxAccumulator(arg);
            default -> throw new IllegalArgumentException("Unsupported aggregate function: " + func);
        };
    }

    interface AccumulatorState {
        void update(Tuple tuple);
        Object result();
    }

    private static class CountAccumulator implements AccumulatorState {
        private final String argument;
        private int count = 0;

        CountAccumulator(String argument) {
            this.argument = argument;
        }

        @Override
        public void update(Tuple tuple) {
            if ("*".equals(argument)) {
                count++;
            } else {
                Object val = JoinUtils.findColumnValue(tuple, argument);
                if (val != null) {
                    count++;
                }
            }
        }

        @Override
        public Object result() {
            return count;
        }
    }

    private static class SumAccumulator implements AccumulatorState {
        private final String argument;
        private double sum = 0.0;
        private boolean hasValue = false;
        private boolean allIntegral = true;

        SumAccumulator(String argument) {
            this.argument = argument;
        }

        @Override
        public void update(Tuple tuple) {
            Object val = JoinUtils.findColumnValue(tuple, argument);
            if (val instanceof Integer i) {
                sum += i;
                hasValue = true;
            } else if (val instanceof Long l) {
                sum += l;
                hasValue = true;
            } else if (val instanceof Double d) {
                sum += d;
                hasValue = true;
                allIntegral = false;
            } else if (val instanceof Number n) {
                sum += n.doubleValue();
                hasValue = true;
                allIntegral = false;
            }
        }

        @Override
        public Object result() {
            if (!hasValue) return null;
            return allIntegral ? (Object) (long) sum : (Object) sum;
        }
    }

    private static class AvgAccumulator implements AccumulatorState {
        private final String argument;
        private double sum = 0.0;
        private int count = 0;

        AvgAccumulator(String argument) {
            this.argument = argument;
        }

        @Override
        public void update(Tuple tuple) {
            Object val = JoinUtils.findColumnValue(tuple, argument);
            if (val instanceof Number n) {
                sum += n.doubleValue();
                count++;
            }
        }

        @Override
        public Object result() {
            return count > 0 ? (Double) (sum / count) : null;
        }
    }

    private static class MinAccumulator implements AccumulatorState {
        private final String argument;
        private Object min = null;

        MinAccumulator(String argument) {
            this.argument = argument;
        }

        @Override
        public void update(Tuple tuple) {
            Object val = JoinUtils.findColumnValue(tuple, argument);
            if (val != null && (min == null || SortAndAggregateUtils.compareForMinMax(val, min) < 0)) {
                min = val;
            }
        }

        @Override
        public Object result() {
            return min;
        }
    }

    private static class MaxAccumulator implements AccumulatorState {
        private final String argument;
        private Object max = null;

        MaxAccumulator(String argument) {
            this.argument = argument;
        }

        @Override
        public void update(Tuple tuple) {
            Object val = JoinUtils.findColumnValue(tuple, argument);
            if (val != null && (max == null || SortAndAggregateUtils.compareForMinMax(val, max) > 0)) {
                max = val;
            }
        }

        @Override
        public Object result() {
            return max;
        }
    }
}
