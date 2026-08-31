package com.stratosdb.storage.page;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a database row
 */
public class Tuple {
    private final List<Object> values;
    private final List<String> columnNames;
    
    public Tuple() {
        this.values = new ArrayList<>();
        this.columnNames = new ArrayList<>();
    }
    
    public Tuple(List<Object> values, List<String> columnNames) {
        this.values = values != null ? values : new ArrayList<>();
        this.columnNames = columnNames != null ? columnNames : new ArrayList<>();
    }
    
    public void addValue(String columnName, Object value) {
        this.columnNames.add(columnName);
        this.values.add(value);
    }
    
    public Object getValue(int index) {
        return index >= 0 && index < values.size() ? values.get(index) : null;
    }
    
    public Object getValue(String columnName) {
        int index = columnNames.indexOf(columnName);
        return index >= 0 ? values.get(index) : null;
    }
    
    public List<Object> getValues() { return values; }
    public List<String> getColumnNames() { return columnNames; }
    public int size() { return values.size(); }
    
    /**
     * Serialize tuple to byte array.
     *
     * Uses a dynamically-growing buffer (ByteArrayOutputStream), not a
     * fixed-size one - a real, separate pre-existing bug found while
     * adding array support: the previous implementation allocated exactly
     * 1024 bytes regardless of actual row size, throwing
     * BufferOverflowException the moment any row's total serialized size
     * exceeded that (a real risk this project's own larger test data -
     * e.g. BRIN's 500-character padding strings - happened not to trigger
     * only because those tests used few enough columns to stay under the
     * limit by luck, not by design). Arrays specifically make this a
     * near-certainty rather than an edge case: even a modestly-sized
     * array column pushes a row over 1024 bytes easily.
     */
    public byte[] serialize() {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteStream);
        try {
            out.writeInt(values.size());

            for (String name : columnNames) {
                byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
                out.writeInt(nameBytes.length);
                out.write(nameBytes);
            }

            for (Object value : values) {
                writeValue(out, value);
            }
        } catch (IOException e) {
            // ByteArrayOutputStream/DataOutputStream never actually throw IOException
            // in practice (no real I/O involved) - this exists only to satisfy the
            // checked exception, not because it's expected to ever happen.
            throw new UncheckedIOException(e);
        }
        return byteStream.toByteArray();
    }

    /**
     * Writes one value's type tag and payload. Recursive for arrays (type
     * tag 6), since an array's elements can themselves be any of the
     * other supported scalar types - reusing this exact method for each
     * element keeps a single, unified encoding rather than a special
     * parallel one just for array contents.
     */
    private static void writeValue(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else if (value instanceof Integer) {
            out.writeInt(1);
            out.writeInt((Integer) value);
        } else if (value instanceof String) {
            byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
            out.writeInt(2);
            out.writeInt(strBytes.length);
            out.write(strBytes);
        } else if (value instanceof Long) {
            out.writeInt(3);
            out.writeLong((Long) value);
        } else if (value instanceof Boolean) {
            out.writeInt(4);
            out.writeByte((Boolean) value ? 1 : 0);
        } else if (value instanceof Double) {
            out.writeInt(5);
            out.writeDouble((Double) value);
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            out.writeInt(6);
            out.writeInt(list.size());
            for (Object element : list) {
                writeValue(out, element);
            }
        } else if (value instanceof java.util.Map) {
            java.util.Map<?, ?> map = (java.util.Map<?, ?>) value;
            out.writeInt(7);
            out.writeInt(map.size());
            for (java.util.Map.Entry<?, ?> entry : map.entrySet()) {
                byte[] keyBytes = entry.getKey().toString().getBytes(StandardCharsets.UTF_8);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                writeValue(out, entry.getValue());
            }
        } else if (value instanceof RangeValue range) {
            // A real INT4RANGE/DATERANGE value (see RangeValue's own javadoc) -
            // its own lower/upper bounds are always themselves either an
            // Integer or a String (this engine's own established convention
            // for a date, see RangeValue.parseBound), both already handled by
            // this same writeValue - reused recursively here rather than
            // duplicating their own encoding a second time. Each bound is
            // preceded by a real presence flag, since either side may
            // genuinely be null (an unbounded end - see RangeValue's own
            // javadoc), which the recursive writeValue call itself already
            // handles correctly via its own type tag -1 for a null value.
            out.writeInt(8);
            writeValue(out, range.lower());
            writeValue(out, range.upper());
            out.writeByte(range.lowerInclusive() ? 1 : 0);
            out.writeByte(range.upperInclusive() ? 1 : 0);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + value.getClass());
        }
    }
    
    /**
     * Deserialize from byte array
     */
    public static Tuple deserialize(byte[] data) {
        Tuple tuple = new Tuple();
        try {
            DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(data));

            int count = in.readInt();

            for (int i = 0; i < count; i++) {
                int nameLen = in.readInt();
                byte[] nameBytes = new byte[nameLen];
                in.readFully(nameBytes);
                tuple.columnNames.add(new String(nameBytes, StandardCharsets.UTF_8));
            }

            for (int i = 0; i < count; i++) {
                tuple.values.add(readValue(in));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return tuple;
    }

    /** Reads one value's type tag and payload - the exact mirror of writeValue, including the recursive case for arrays. */
    private static Object readValue(DataInputStream in) throws IOException {
        int type = in.readInt();
        switch (type) {
            case 1:
                return in.readInt();
            case 2: {
                int len = in.readInt();
                byte[] strBytes = new byte[len];
                in.readFully(strBytes);
                return new String(strBytes, StandardCharsets.UTF_8);
            }
            case 3:
                return in.readLong();
            case 4:
                return in.readByte() == 1;
            case 5:
                return in.readDouble();
            case 6: {
                int size = in.readInt();
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++) {
                    list.add(readValue(in));
                }
                return list;
            }
            case 7: {
                int size = in.readInt();
                java.util.Map<String, Object> map = new java.util.LinkedHashMap<>(); // LinkedHashMap: preserves key insertion order, matching JsonParser's own choice
                for (int i = 0; i < size; i++) {
                    int keyLen = in.readInt();
                    byte[] keyBytes = new byte[keyLen];
                    in.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);
                    map.put(key, readValue(in));
                }
                return map;
            }
            case 8: {
                Object lower = readValue(in);
                Object upper = readValue(in);
                boolean lowerInclusive = in.readByte() == 1;
                boolean upperInclusive = in.readByte() == 1;
                return new RangeValue(lower, upper, lowerInclusive, upperInclusive);
            }
            case -1:
                return null;
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(columnNames.get(i)).append("=").append(values.get(i));
        }
        sb.append("]");
        return sb.toString();
    }
}