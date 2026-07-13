package com.stratosdb.storage.page;

import java.nio.ByteBuffer;
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
     * Serialize tuple to byte array
     */
    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        
        // Write number of columns
        buffer.putInt(values.size());
        
        // Write column names
        for (String name : columnNames) {
            byte[] nameBytes = name.getBytes();
            buffer.putInt(nameBytes.length);
            buffer.put(nameBytes);
        }
        
        // Write each value
        for (Object value : values) {
            if (value == null) {
                buffer.putInt(-1);
            } else if (value instanceof Integer) {
                buffer.putInt(1); // type
                buffer.putInt((Integer) value);
            } else if (value instanceof String) {
                String str = (String) value;
                byte[] strBytes = str.getBytes();
                buffer.putInt(2); // type
                buffer.putInt(strBytes.length);
                buffer.put(strBytes);
            } else if (value instanceof Long) {
                buffer.putInt(3); // type
                buffer.putLong((Long) value);
            } else if (value instanceof Boolean) {
                buffer.putInt(4); // type
                buffer.put((byte) ((Boolean) value ? 1 : 0));
            } else if (value instanceof Double) {
                buffer.putInt(5); // type
                buffer.putDouble((Double) value);
            } else {
                throw new IllegalArgumentException("Unsupported type: " + value.getClass());
            }
        }
        
        byte[] result = new byte[buffer.position()];
        buffer.position(0);
        buffer.get(result);
        return result;
    }
    
    /**
     * Deserialize from byte array
     */
    public static Tuple deserialize(byte[] data) {
        Tuple tuple = new Tuple();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        int count = buffer.getInt();
        
        // Read column names
        for (int i = 0; i < count; i++) {
            int nameLen = buffer.getInt();
            byte[] nameBytes = new byte[nameLen];
            buffer.get(nameBytes);
            tuple.columnNames.add(new String(nameBytes));
        }
        
        // Read values
        for (int i = 0; i < count; i++) {
            int type = buffer.getInt();
            switch (type) {
                case 1: // Integer
                    tuple.values.add(buffer.getInt());
                    break;
                case 2: // String
                    int len = buffer.getInt();
                    byte[] strBytes = new byte[len];
                    buffer.get(strBytes);
                    tuple.values.add(new String(strBytes));
                    break;
                case 3: // Long
                    tuple.values.add(buffer.getLong());
                    break;
                case 4: // Boolean
                    tuple.values.add(buffer.get() == 1);
                    break;
                case 5: // Double
                    tuple.values.add(buffer.getDouble());
                    break;
                case -1: // NULL
                    tuple.values.add(null);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type: " + type);
            }
        }
        
        return tuple;
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