package com.stratosdb.storage.page;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decomposed binary JSONB storage format with a fixed 12-byte header,
 * sorted key-offset / element-offset tables, and per-node type tags.
 *
 * <p>Binary layout:
 * <pre>
 * Header (12 bytes):
 *   [0..3]  Magic: 'J','S','B','1' (0x4A534231)
 *   [4]     Version: 1
 *   [5]     Flags: 0
 *   [6]     Root type tag
 *   [7]     Reserved: 0
 *   [8..11] Root node offset (int32, big-endian; 12 for root documents)
 *
 * Node encodings:
 *   TAG_NULL   (0x00): no payload
 *   TAG_FALSE  (0x01): no payload
 *   TAG_TRUE   (0x02): no payload
 *   TAG_LONG   (0x03): int64 (8 bytes)
 *   TAG_DOUBLE (0x04): float64 (8 bytes)
 *   TAG_STRING (0x05): int32 byteLen + UTF-8 bytes
 *   TAG_ARRAY  (0x06): int32 count + count * int32 elementOffsets + element nodes
 *   TAG_OBJECT (0x07): int32 count + count * (int32 keyOffset, int32 valOffset)
 *                      sorted lexicographically by UTF-8 key, followed by
 *                      key strings (int32 len + UTF-8) and value nodes.
 * </pre>
 *
 * <p>All offsets are absolute byte positions within the binary buffer, allowing
 * O(log N) binary search on object keys and O(1) array indexing without
 * deserializing sibling keys or values.
 */
public final class BinaryJsonb {
    public static final int MAGIC = 0x4A534231; // "JSB1"
    public static final byte VERSION = 1;
    public static final int HEADER_SIZE = 12;

    public static final byte TAG_NULL = 0x00;
    public static final byte TAG_FALSE = 0x01;
    public static final byte TAG_TRUE = 0x02;
    public static final byte TAG_LONG = 0x03;
    public static final byte TAG_DOUBLE = 0x04;
    public static final byte TAG_STRING = 0x05;
    public static final byte TAG_ARRAY = 0x06;
    public static final byte TAG_OBJECT = 0x07;

    private final byte[] data;
    private final int rootOffset;

    /**
     * Wraps a complete serialized BinaryJsonb buffer, validating its header.
     */
    public BinaryJsonb(byte[] data) {
        if (data == null || data.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid BinaryJsonb buffer: too short");
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        int magic = buf.getInt(0);
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid BinaryJsonb magic: 0x" + Integer.toHexString(magic));
        }
        byte version = buf.get(4);
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported BinaryJsonb version: " + version);
        }
        this.data = data;
        this.rootOffset = buf.getInt(8);
        if (this.rootOffset < HEADER_SIZE || this.rootOffset >= data.length) {
            throw new IllegalArgumentException("Invalid BinaryJsonb root offset: " + this.rootOffset);
        }
    }

    private BinaryJsonb(byte[] data, int rootOffset) {
        this.data = data;
        this.rootOffset = rootOffset;
    }

    /**
     * Encodes a parsed Java JSON structure (Map, List, String, Number, Boolean, null)
     * into a decomposed BinaryJsonb buffer.
     */
    public static BinaryJsonb encode(Object value) {
        if (value instanceof BinaryJsonb existing) {
            return existing.rootOffset == HEADER_SIZE && existing.getRootOffset() == HEADER_SIZE
                ? existing
                : encode(existing.toJavaObject());
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
        DataOutputStream out = new DataOutputStream(baos);
        try {
            // Write placeholder 12-byte header
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(0); // flags
            out.writeByte(tagFor(value));
            out.writeByte(0); // reserved
            out.writeInt(HEADER_SIZE);

            byte[] body = encodeNode(value, HEADER_SIZE);
            out.write(body);
            out.flush();
            return new BinaryJsonb(baos.toByteArray(), HEADER_SIZE);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to encode BinaryJsonb", e);
        }
    }

    private static byte tagFor(Object value) {
        if (value == null) return TAG_NULL;
        if (value instanceof Boolean b) return b ? TAG_TRUE : TAG_FALSE;
        if (value instanceof Integer || value instanceof Long) return TAG_LONG;
        if (value instanceof Double d) {
            if (d == Math.floor(d) && !Double.isInfinite(d) && d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
                return TAG_LONG;
            }
            return TAG_DOUBLE;
        }
        if (value instanceof Number) return TAG_DOUBLE;
        if (value instanceof String) return TAG_STRING;
        if (value instanceof List<?>) return TAG_ARRAY;
        if (value instanceof Map<?, ?>) return TAG_OBJECT;
        throw new IllegalArgumentException("Unsupported JSONB value type: " + value.getClass().getName());
    }

    private static byte[] encodeNode(Object value, int baseOffset) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        byte tag = tagFor(value);
        out.writeByte(tag);

        switch (tag) {
            case TAG_NULL, TAG_FALSE, TAG_TRUE -> {
                // Tag alone encodes the value
            }
            case TAG_LONG -> {
                long l = (value instanceof Number n) ? n.longValue() : 0L;
                out.writeLong(l);
            }
            case TAG_DOUBLE -> {
                double d = ((Number) value).doubleValue();
                out.writeDouble(d);
            }
            case TAG_STRING -> {
                byte[] utf8 = ((String) value).getBytes(StandardCharsets.UTF_8);
                out.writeInt(utf8.length);
                out.write(utf8);
            }
            case TAG_ARRAY -> {
                List<?> list = (List<?>) value;
                int count = list.size();
                out.writeInt(count);
                int tableStart = baseOffset + 1 + 4;
                int cursor = tableStart + count * 4;
                List<byte[]> encodedElements = new ArrayList<>(count);
                int[] elemOffsets = new int[count];
                for (int i = 0; i < count; i++) {
                    elemOffsets[i] = cursor;
                    byte[] elemBytes = encodeNode(list.get(i), cursor);
                    encodedElements.add(elemBytes);
                    cursor += elemBytes.length;
                }
                for (int off : elemOffsets) {
                    out.writeInt(off);
                }
                for (byte[] elemBytes : encodedElements) {
                    out.write(elemBytes);
                }
            }
            case TAG_OBJECT -> {
                Map<?, ?> map = (Map<?, ?>) value;
                record KeyVal(String key, byte[] keyUtf8, Object val) {}
                List<KeyVal> entries = new ArrayList<>(map.size());
                for (Map.Entry<?, ?> e : map.entrySet()) {
                    String k = e.getKey().toString();
                    entries.add(new KeyVal(k, k.getBytes(StandardCharsets.UTF_8), e.getValue()));
                }
                entries.sort(Comparator.comparing(kv -> kv.key, String::compareTo));

                int count = entries.size();
                out.writeInt(count);
                int tableStart = baseOffset + 1 + 4;
                int cursor = tableStart + count * 8;

                int[] keyOffsets = new int[count];
                int[] valOffsets = new int[count];
                List<byte[]> encodedVals = new ArrayList<>(count);

                // First lay out keys, then values
                for (int i = 0; i < count; i++) {
                    keyOffsets[i] = cursor;
                    cursor += 4 + entries.get(i).keyUtf8.length;
                }
                for (int i = 0; i < count; i++) {
                    valOffsets[i] = cursor;
                    byte[] valBytes = encodeNode(entries.get(i).val, cursor);
                    encodedVals.add(valBytes);
                    cursor += valBytes.length;
                }
                for (int i = 0; i < count; i++) {
                    out.writeInt(keyOffsets[i]);
                    out.writeInt(valOffsets[i]);
                }
                for (KeyVal kv : entries) {
                    out.writeInt(kv.keyUtf8.length);
                    out.write(kv.keyUtf8);
                }
                for (byte[] valBytes : encodedVals) {
                    out.write(valBytes);
                }
            }
            default -> throw new IllegalStateException("Unexpected tag: " + tag);
        }

        out.flush();
        return baos.toByteArray();
    }

    public int getMagic() {
        return ByteBuffer.wrap(data).getInt(0);
    }

    public byte getVersion() {
        return data[4];
    }

    public byte getRootTag() {
        return data[this.rootOffset];
    }

    public int getRootOffset() {
        return rootOffset;
    }

    public byte[] rawBytes() {
        return data;
    }

    public boolean isNull() {
        return data[rootOffset] == TAG_NULL;
    }

    public boolean isObject() {
        return data[rootOffset] == TAG_OBJECT;
    }

    public boolean isArray() {
        return data[rootOffset] == TAG_ARRAY;
    }

    /**
     * Checks whether this JSONB object contains the given top-level key
     * using O(log N) binary search over the sorted key-offset table.
     */
    public boolean hasKey(String key) {
        if (key == null || data[rootOffset] != TAG_OBJECT) {
            return false;
        }
        return findObjectKeyOffset(rootOffset, key) >= 0;
    }

    /**
     * Extracts a child node by object key using O(log N) binary search.
     * Returns null if this node is not an object or the key is absent.
     * If the key's value is JSON null, returns a BinaryJsonb node with TAG_NULL.
     */
    public BinaryJsonb get(String key) {
        if (key == null || data[rootOffset] != TAG_OBJECT) {
            return null;
        }
        int valOffset = findObjectKeyOffset(rootOffset, key);
        if (valOffset < 0) {
            return null;
        }
        return new BinaryJsonb(data, valOffset);
    }

    /**
     * Extracts a child node by 0-based array index in O(1) via the element offset table.
     * Returns null if this node is not an array or index is out of bounds.
     */
    public BinaryJsonb get(int index) {
        if (index < 0 || data[rootOffset] != TAG_ARRAY) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(data);
        int count = buf.getInt(rootOffset + 1);
        if (index >= count) {
            return null;
        }
        int elemOffset = buf.getInt(rootOffset + 5 + index * 4);
        return new BinaryJsonb(data, elemOffset);
    }

    private int findObjectKeyOffset(int objOffset, String targetKey) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int count = buf.getInt(objOffset + 1);
        int tableStart = objOffset + 5;
        byte[] targetBytes = targetKey.getBytes(StandardCharsets.UTF_8);

        int lo = 0;
        int hi = count - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int entryPos = tableStart + mid * 8;
            int keyOffset = buf.getInt(entryPos);
            int keyLen = buf.getInt(keyOffset);
            int cmp = compareUtf8(data, keyOffset + 4, keyLen, targetBytes);
            if (cmp < 0) {
                lo = mid + 1;
            } else if (cmp > 0) {
                hi = mid - 1;
            } else {
                return buf.getInt(entryPos + 4);
            }
        }
        return -1;
    }

    private static int compareUtf8(byte[] buf, int offset, int len, byte[] target) {
        String s = new String(buf, offset, len, StandardCharsets.UTF_8);
        String t = new String(target, StandardCharsets.UTF_8);
        return s.compareTo(t);
    }

    /**
     * Navigates a sequence of path steps (String keys or Integer indices)
     * directly over the binary offset tables.
     */
    public BinaryJsonb navigate(List<Object> steps) {
        BinaryJsonb current = this;
        for (Object step : steps) {
            if (current == null) {
                return null;
            }
            if (step instanceof Number n) {
                current = current.get(n.intValue());
            } else {
                String s = step.toString();
                if (current.isArray()) {
                    try {
                        current = current.get(Integer.parseInt(s));
                    } catch (NumberFormatException e) {
                        return null;
                    }
                } else {
                    current = current.get(s);
                }
            }
        }
        return current;
    }

    /**
     * PostgreSQL JSONB containment (@>): returns true if every key/element
     * structure in {@code other} is contained within {@code this}.
     */
    public boolean contains(BinaryJsonb other) {
        if (other == null) {
            return false;
        }
        return nodeContains(this.data, this.rootOffset, other.data, other.rootOffset);
    }

    private static boolean nodeContains(byte[] left, int leftOff, byte[] right, int rightOff) {
        byte lTag = left[leftOff];
        byte rTag = right[rightOff];

        if (lTag == TAG_OBJECT && rTag == TAG_OBJECT) {
            ByteBuffer rBuf = ByteBuffer.wrap(right);
            int rCount = rBuf.getInt(rightOff + 1);
            int rTable = rightOff + 5;
            BinaryJsonb leftObj = new BinaryJsonb(left, leftOff);
            for (int i = 0; i < rCount; i++) {
                int rKeyOff = rBuf.getInt(rTable + i * 8);
                int rKeyLen = rBuf.getInt(rKeyOff);
                String key = new String(right, rKeyOff + 4, rKeyLen, StandardCharsets.UTF_8);
                int rValOff = rBuf.getInt(rTable + i * 8 + 4);
                int lValOff = leftObj.findObjectKeyOffset(leftOff, key);
                if (lValOff < 0 || !nodeContains(left, lValOff, right, rValOff)) {
                    return false;
                }
            }
            return true;
        }

        if (lTag == TAG_ARRAY && rTag == TAG_ARRAY) {
            ByteBuffer lBuf = ByteBuffer.wrap(left);
            ByteBuffer rBuf = ByteBuffer.wrap(right);
            int lCount = lBuf.getInt(leftOff + 1);
            int rCount = rBuf.getInt(rightOff + 1);
            for (int i = 0; i < rCount; i++) {
                int rElemOff = rBuf.getInt(rightOff + 5 + i * 4);
                boolean matched = false;
                for (int j = 0; j < lCount; j++) {
                    int lElemOff = lBuf.getInt(leftOff + 5 + j * 4);
                    if (nodeContains(left, lElemOff, right, rElemOff)) {
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    return false;
                }
            }
            return true;
        }

        if (lTag == TAG_ARRAY) {
            // Scalar containment in array: true if any element contains the scalar
            ByteBuffer lBuf = ByteBuffer.wrap(left);
            int lCount = lBuf.getInt(leftOff + 1);
            for (int j = 0; j < lCount; j++) {
                int lElemOff = lBuf.getInt(leftOff + 5 + j * 4);
                if (nodeContains(left, lElemOff, right, rightOff)) {
                    return true;
                }
            }
            return false;
        }

        // Scalar equality (with Long/Double numeric equivalence)
        if ((lTag == TAG_LONG || lTag == TAG_DOUBLE) && (rTag == TAG_LONG || rTag == TAG_DOUBLE)) {
            double lv = readNumericAsDouble(left, leftOff, lTag);
            double rv = readNumericAsDouble(right, rightOff, rTag);
            return Double.compare(lv, rv) == 0;
        }
        if (lTag != rTag) {
            return false;
        }
        return switch (lTag) {
            case TAG_NULL, TAG_FALSE, TAG_TRUE -> true;
            case TAG_STRING -> {
                ByteBuffer lBuf = ByteBuffer.wrap(left);
                ByteBuffer rBuf = ByteBuffer.wrap(right);
                int lLen = lBuf.getInt(leftOff + 1);
                int rLen = rBuf.getInt(rightOff + 1);
                yield lLen == rLen && Arrays.equals(left, leftOff + 5, leftOff + 5 + lLen, right, rightOff + 5, rightOff + 5 + rLen);
            }
            default -> false;
        };
    }

    private static double readNumericAsDouble(byte[] buf, int off, byte tag) {
        ByteBuffer bb = ByteBuffer.wrap(buf);
        if (tag == TAG_LONG) {
            return (double) bb.getLong(off + 1);
        }
        return bb.getDouble(off + 1);
    }

    /**
     * Collects all GIN index keys for this JSONB document:
     * - "$key:<k>" for every top-level key
     * - "<k>:<scalar>" for top-level scalar/array elements
     * - "<path>:<scalar>" for nested object scalar/array elements
     */
    public void collectGinKeys(List<String> outKeys) {
        collectGinKeysAt(rootOffset, "", outKeys, true);
    }

    private void collectGinKeysAt(int nodeOff, String prefix, List<String> outKeys, boolean isTopLevel) {
        byte tag = data[nodeOff];
        ByteBuffer buf = ByteBuffer.wrap(data);
        if (tag == TAG_OBJECT) {
            int count = buf.getInt(nodeOff + 1);
            int table = nodeOff + 5;
            for (int i = 0; i < count; i++) {
                int keyOff = buf.getInt(table + i * 8);
                int keyLen = buf.getInt(keyOff);
                String key = new String(data, keyOff + 4, keyLen, StandardCharsets.UTF_8);
                int valOff = buf.getInt(table + i * 8 + 4);
                if (isTopLevel) {
                    outKeys.add("$key:" + key);
                }
                String fullPath = prefix.isEmpty() ? key : prefix + "." + key;
                byte valTag = data[valOff];
                if (valTag == TAG_OBJECT) {
                    collectGinKeysAt(valOff, fullPath, outKeys, false);
                } else if (valTag == TAG_ARRAY) {
                    collectGinArrayElements(valOff, fullPath, outKeys);
                } else if (valTag != TAG_NULL) {
                    outKeys.add(fullPath + ":" + scalarAsTextAt(valOff));
                }
            }
        } else if (tag == TAG_ARRAY) {
            collectGinArrayElements(nodeOff, prefix, outKeys);
        }
    }

    private void collectGinArrayElements(int arrOff, String pathPrefix, List<String> outKeys) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int count = buf.getInt(arrOff + 1);
        for (int i = 0; i < count; i++) {
            int elemOff = buf.getInt(arrOff + 5 + i * 4);
            byte elemTag = data[elemOff];
            if (elemTag == TAG_OBJECT) {
                collectGinKeysAt(elemOff, pathPrefix, outKeys, false);
            } else if (elemTag != TAG_NULL && elemTag != TAG_ARRAY) {
                String text = scalarAsTextAt(elemOff);
                if (!pathPrefix.isEmpty()) {
                    outKeys.add(pathPrefix + ":" + text);
                } else {
                    outKeys.add(text);
                }
            }
        }
    }

    /**
     * Renders this node as unquoted text for ->> extraction (null for JSON null).
     */
    public String asText() {
        byte tag = data[rootOffset];
        if (tag == TAG_NULL) {
            return null;
        }
        if (tag == TAG_OBJECT || tag == TAG_ARRAY) {
            return toJsonText();
        }
        return scalarAsTextAt(rootOffset);
    }

    private String scalarAsTextAt(int off) {
        byte tag = data[off];
        ByteBuffer buf = ByteBuffer.wrap(data);
        return switch (tag) {
            case TAG_NULL -> "null";
            case TAG_FALSE -> "false";
            case TAG_TRUE -> "true";
            case TAG_LONG -> String.valueOf(buf.getLong(off + 1));
            case TAG_DOUBLE -> {
                double d = buf.getDouble(off + 1);
                if (d == Math.floor(d) && !Double.isInfinite(d)) {
                    yield String.valueOf((long) d);
                }
                yield String.valueOf(d);
            }
            case TAG_STRING -> {
                int len = buf.getInt(off + 1);
                yield new String(data, off + 5, len, StandardCharsets.UTF_8);
            }
            default -> toJsonTextAt(off);
        };
    }

    /**
     * Decodes this node back into standard Java objects (Map, List, String, Long, Double, Boolean, null).
     */
    public Object toJavaObject() {
        return toJavaObjectAt(rootOffset);
    }

    private Object toJavaObjectAt(int off) {
        byte tag = data[off];
        ByteBuffer buf = ByteBuffer.wrap(data);
        return switch (tag) {
            case TAG_NULL -> null;
            case TAG_FALSE -> Boolean.FALSE;
            case TAG_TRUE -> Boolean.TRUE;
            case TAG_LONG -> buf.getLong(off + 1);
            case TAG_DOUBLE -> buf.getDouble(off + 1);
            case TAG_STRING -> {
                int len = buf.getInt(off + 1);
                yield new String(data, off + 5, len, StandardCharsets.UTF_8);
            }
            case TAG_ARRAY -> {
                int count = buf.getInt(off + 1);
                List<Object> list = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    list.add(toJavaObjectAt(buf.getInt(off + 5 + i * 4)));
                }
                yield list;
            }
            case TAG_OBJECT -> {
                int count = buf.getInt(off + 1);
                Map<String, Object> map = new LinkedHashMap<>(count);
                int table = off + 5;
                for (int i = 0; i < count; i++) {
                    int keyOff = buf.getInt(table + i * 8);
                    int keyLen = buf.getInt(keyOff);
                    String key = new String(data, keyOff + 4, keyLen, StandardCharsets.UTF_8);
                    int valOff = buf.getInt(table + i * 8 + 4);
                    map.put(key, toJavaObjectAt(valOff));
                }
                yield map;
            }
            default -> throw new IllegalStateException("Unknown JSONB tag: " + tag);
        };
    }

    public String toJsonText() {
        return toJsonTextAt(rootOffset);
    }

    private String toJsonTextAt(int off) {
        StringBuilder sb = new StringBuilder();
        writeJsonTextAt(off, sb);
        return sb.toString();
    }

    private void writeJsonTextAt(int off, StringBuilder sb) {
        byte tag = data[off];
        ByteBuffer buf = ByteBuffer.wrap(data);
        switch (tag) {
            case TAG_NULL -> sb.append("null");
            case TAG_FALSE -> sb.append("false");
            case TAG_TRUE -> sb.append("true");
            case TAG_LONG -> sb.append(buf.getLong(off + 1));
            case TAG_DOUBLE -> {
                double d = buf.getDouble(off + 1);
                if (d == Math.floor(d) && !Double.isInfinite(d)) {
                    sb.append((long) d);
                } else {
                    sb.append(d);
                }
            }
            case TAG_STRING -> {
                int len = buf.getInt(off + 1);
                writeEscapedString(new String(data, off + 5, len, StandardCharsets.UTF_8), sb);
            }
            case TAG_ARRAY -> {
                sb.append('[');
                int count = buf.getInt(off + 1);
                for (int i = 0; i < count; i++) {
                    if (i > 0) sb.append(',');
                    writeJsonTextAt(buf.getInt(off + 5 + i * 4), sb);
                }
                sb.append(']');
            }
            case TAG_OBJECT -> {
                sb.append('{');
                int count = buf.getInt(off + 1);
                int table = off + 5;
                for (int i = 0; i < count; i++) {
                    if (i > 0) sb.append(',');
                    int keyOff = buf.getInt(table + i * 8);
                    int keyLen = buf.getInt(keyOff);
                    String key = new String(data, keyOff + 4, keyLen, StandardCharsets.UTF_8);
                    writeEscapedString(key, sb);
                    sb.append(':');
                    writeJsonTextAt(buf.getInt(table + i * 8 + 4), sb);
                }
                sb.append('}');
            }
            default -> throw new IllegalStateException("Unknown JSONB tag: " + tag);
        }
    }

    private static void writeEscapedString(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> sb.append(c);
            }
        }
        sb.append('"');
    }

    @Override
    public String toString() {
        return toJsonText();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BinaryJsonb other)) return false;
        return this.rootOffset == HEADER_SIZE && other.rootOffset == HEADER_SIZE
            ? Arrays.equals(this.data, other.data)
            : this.contains(other) && other.contains(this);
    }

    @Override
    public int hashCode() {
        return rootOffset == HEADER_SIZE ? Arrays.hashCode(data) : toJsonText().hashCode();
    }
}
