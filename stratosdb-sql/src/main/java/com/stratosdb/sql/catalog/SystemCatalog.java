package com.stratosdb.sql.catalog;

import com.stratosdb.storage.buffer.BufferPool;
import com.stratosdb.storage.heap.HeapTable;
import com.stratosdb.storage.page.Tuple;
import com.stratosdb.transaction.Transaction;
import com.stratosdb.transaction.TransactionManager;
import com.stratosdb.transaction.mvcc.MVCCVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Transactional Relational System Catalog.
 *
 * Implements internal relational system tables storing database metadata as real MVCC HeapTables:
 *   - stratos_tables:      table_id, table_name, table_type, owner, ddl
 *   - stratos_columns:     table_name, column_name, data_type, ordinal_position, is_nullable, column_default
 *   - stratos_indexes:     index_name, table_name, column_name, column_name_2, index_type, is_unique
 *   - stratos_types:       type_name, type_category, type_values
 *   - stratos_constraints: constraint_name, table_name, constraint_type, definition
 *
 * DDL operations (CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE INDEX, CREATE TYPE)
 * execute as transactional ACID writes against these system tables. When a transaction
 * rolls back, in-memory schema state and relational system tables roll back to their
 * exact pre-transaction state.
 */
public class SystemCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(SystemCatalog.class);

    public static final String TABLE_TABLES = "stratos_tables";
    public static final String TABLE_COLUMNS = "stratos_columns";
    public static final String TABLE_INDEXES = "stratos_indexes";
    public static final String TABLE_TYPES = "stratos_types";
    public static final String TABLE_CONSTRAINTS = "stratos_constraints";

    public static final List<String> STRATOS_TABLES_COLS = List.of(
        "table_id", "table_name", "table_type", "owner", "ddl"
    );
    public static final Map<String, String> STRATOS_TABLES_TYPES = Map.of(
        "table_id", "INT",
        "table_name", "VARCHAR",
        "table_type", "VARCHAR",
        "owner", "VARCHAR",
        "ddl", "VARCHAR"
    );

    public static final List<String> STRATOS_COLUMNS_COLS = List.of(
        "table_name", "column_name", "data_type", "ordinal_position", "is_nullable", "column_default"
    );
    public static final Map<String, String> STRATOS_COLUMNS_TYPES = Map.of(
        "table_name", "VARCHAR",
        "column_name", "VARCHAR",
        "data_type", "VARCHAR",
        "ordinal_position", "INT",
        "is_nullable", "VARCHAR",
        "column_default", "VARCHAR"
    );

    public static final List<String> STRATOS_INDEXES_COLS = List.of(
        "index_name", "table_name", "column_name", "column_name_2", "index_type", "is_unique"
    );
    public static final Map<String, String> STRATOS_INDEXES_TYPES = Map.of(
        "index_name", "VARCHAR",
        "table_name", "VARCHAR",
        "column_name", "VARCHAR",
        "column_name_2", "VARCHAR",
        "index_type", "VARCHAR",
        "is_unique", "VARCHAR"
    );

    public static final List<String> STRATOS_TYPES_COLS = List.of(
        "type_name", "type_category", "type_values"
    );
    public static final Map<String, String> STRATOS_TYPES_TYPES = Map.of(
        "type_name", "VARCHAR",
        "type_category", "VARCHAR",
        "type_values", "VARCHAR"
    );

    public static final List<String> STRATOS_CONSTRAINTS_COLS = List.of(
        "constraint_name", "table_name", "constraint_type", "definition"
    );
    public static final Map<String, String> STRATOS_CONSTRAINTS_TYPES = Map.of(
        "constraint_name", "VARCHAR",
        "table_name", "VARCHAR",
        "constraint_type", "VARCHAR",
        "definition", "VARCHAR"
    );

    public interface CatalogUndoConsumer {
        void onInsert(String tableName, long pageId, int slot);
        void onDelete(String tableName, long pageId, int slot);
    }

    private final BufferPool bufferPool;
    private final TransactionManager transactionManager;
    private final String dataDirectory;
    private final AtomicInteger nextTableId = new AtomicInteger(1);
    private CatalogUndoConsumer undoConsumer;

    private HeapTable tablesTable;
    private HeapTable columnsTable;
    private HeapTable indexesTable;
    private HeapTable typesTable;
    private HeapTable constraintsTable;

    public SystemCatalog(BufferPool bufferPool, TransactionManager transactionManager, String dataDirectory) {
        this.bufferPool = bufferPool;
        this.transactionManager = transactionManager;
        this.dataDirectory = dataDirectory;
    }

    public void setUndoConsumer(CatalogUndoConsumer undoConsumer) {
        this.undoConsumer = undoConsumer;
    }

    public static boolean isSystemTable(String name) {
        if (name == null) return false;
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.equals(TABLE_TABLES) ||
               lower.equals(TABLE_COLUMNS) ||
               lower.equals(TABLE_INDEXES) ||
               lower.equals(TABLE_TYPES) ||
               lower.equals(TABLE_CONSTRAINTS);
    }

    public HeapTable getTablesTable() { return tablesTable; }
    public HeapTable getColumnsTable() { return columnsTable; }
    public HeapTable getIndexesTable() { return indexesTable; }
    public HeapTable getTypesTable() { return typesTable; }
    public HeapTable getConstraintsTable() { return constraintsTable; }

    /**
     * Initializes the relational system catalog: creates system HeapTables and bootstrap self-describing metadata.
     */
    public void init(Map<String, HeapTable> tables,
                     Map<String, List<String>> tableColumns,
                     Map<String, Map<String, String>> tableColumnTypes,
                     Map<String, Map<String, String>> tableColumnDefaults) {
        this.tablesTable = new HeapTable(TABLE_TABLES, bufferPool);
        this.columnsTable = new HeapTable(TABLE_COLUMNS, bufferPool);
        this.indexesTable = new HeapTable(TABLE_INDEXES, bufferPool);
        this.typesTable = new HeapTable(TABLE_TYPES, bufferPool);
        this.constraintsTable = new HeapTable(TABLE_CONSTRAINTS, bufferPool);

        registerSystemTable(tables, tableColumns, tableColumnTypes, TABLE_TABLES, STRATOS_TABLES_COLS, STRATOS_TABLES_TYPES, tablesTable);
        registerSystemTable(tables, tableColumns, tableColumnTypes, TABLE_COLUMNS, STRATOS_COLUMNS_COLS, STRATOS_COLUMNS_TYPES, columnsTable);
        registerSystemTable(tables, tableColumns, tableColumnTypes, TABLE_INDEXES, STRATOS_INDEXES_COLS, STRATOS_INDEXES_TYPES, indexesTable);
        registerSystemTable(tables, tableColumns, tableColumnTypes, TABLE_TYPES, STRATOS_TYPES_COLS, STRATOS_TYPES_TYPES, typesTable);
        registerSystemTable(tables, tableColumns, tableColumnTypes, TABLE_CONSTRAINTS, STRATOS_CONSTRAINTS_COLS, STRATOS_CONSTRAINTS_TYPES, constraintsTable);

        // Bootstrap self-describing metadata if system tables are brand new
        if (tablesTable.scanPositioned().isEmpty()) {
            bootstrapSelfDescribingMetadata();
        } else {
            // Find max table ID
            int maxId = 5;
            for (HeapTable.PositionedRow row : tablesTable.scanPositioned()) {
                Tuple t = Tuple.deserialize(row.stored(), STRATOS_TABLES_COLS);
                Object val = t.getValue("table_id");
                if (val instanceof Number n) {
                    maxId = Math.max(maxId, n.intValue());
                }
            }
            nextTableId.set(maxId + 1);
        }
    }

    private void registerSystemTable(Map<String, HeapTable> tables,
                                     Map<String, List<String>> tableColumns,
                                     Map<String, Map<String, String>> tableColumnTypes,
                                     String name, List<String> cols, Map<String, String> types, HeapTable table) {
        tables.put(name, table);
        tableColumns.put(name, new ArrayList<>(cols));
        tableColumnTypes.put(name, new HashMap<>(types));
    }

    private void bootstrapSelfDescribingMetadata() {
        long xid = 0L;
        insertTableInternal(1, TABLE_TABLES, "SYSTEM TABLE", "stratos",
            "CREATE TABLE stratos_tables (table_id INT, table_name VARCHAR, table_type VARCHAR, owner VARCHAR, ddl VARCHAR);", xid);
        insertTableInternal(2, TABLE_COLUMNS, "SYSTEM TABLE", "stratos",
            "CREATE TABLE stratos_columns (table_name VARCHAR, column_name VARCHAR, data_type VARCHAR, ordinal_position INT, is_nullable VARCHAR, column_default VARCHAR);", xid);
        insertTableInternal(3, TABLE_INDEXES, "SYSTEM TABLE", "stratos",
            "CREATE TABLE stratos_indexes (index_name VARCHAR, table_name VARCHAR, column_name VARCHAR, column_name_2 VARCHAR, index_type VARCHAR, is_unique VARCHAR);", xid);
        insertTableInternal(4, TABLE_TYPES, "SYSTEM TABLE", "stratos",
            "CREATE TABLE stratos_types (type_name VARCHAR, type_category VARCHAR, type_values VARCHAR);", xid);
        insertTableInternal(5, TABLE_CONSTRAINTS, "SYSTEM TABLE", "stratos",
            "CREATE TABLE stratos_constraints (constraint_name VARCHAR, table_name VARCHAR, constraint_type VARCHAR, definition VARCHAR);", xid);

        insertColumnsForTable(TABLE_TABLES, STRATOS_TABLES_COLS, STRATOS_TABLES_TYPES, xid);
        insertColumnsForTable(TABLE_COLUMNS, STRATOS_COLUMNS_COLS, STRATOS_COLUMNS_TYPES, xid);
        insertColumnsForTable(TABLE_INDEXES, STRATOS_INDEXES_COLS, STRATOS_INDEXES_TYPES, xid);
        insertColumnsForTable(TABLE_TYPES, STRATOS_TYPES_COLS, STRATOS_TYPES_TYPES, xid);
        insertColumnsForTable(TABLE_CONSTRAINTS, STRATOS_CONSTRAINTS_COLS, STRATOS_CONSTRAINTS_TYPES, xid);

        nextTableId.set(6);
    }

    private void insertColumnsForTable(String tableName, List<String> cols, Map<String, String> types, long xid) {
        for (int i = 0; i < cols.size(); i++) {
            String colName = cols.get(i);
            String type = types.getOrDefault(colName, "VARCHAR");
            insertColumn(tableName, colName, type, i + 1, false, null, xid);
        }
    }

    // --- Transactional DDL mutating operations ---

    public int insertTable(String tableName, String tableType, String owner, String ddl, long xid) {
        int id = nextTableId.getAndIncrement();
        insertTableInternal(id, tableName, tableType, owner, ddl, xid);
        return id;
    }

    private void insertTableInternal(int id, String tableName, String tableType, String owner, String ddl, long xid) {
        Tuple tuple = new Tuple();
        tuple.addValue("table_id", id);
        tuple.addValue("table_name", tableName);
        tuple.addValue("table_type", tableType);
        tuple.addValue("owner", owner != null ? owner : "");
        tuple.addValue("ddl", ddl != null ? ddl : "");
        tuple.setXmin(xid);
        tuple.setXmax(0L);
        HeapTable.InsertResult res = tablesTable.insertMvcc(tuple.serialize(), xid);
        if (undoConsumer != null && xid != 0L) {
            undoConsumer.onInsert(TABLE_TABLES, res.pageId, res.slot);
        }
    }

    public void deleteTable(String tableName, long xid) {
        for (HeapTable.PositionedRow row : tablesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_TABLES_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    tablesTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_TABLES, row.pageId(), row.slot());
                    }
                }
            }
        }
    }

    public void updateTableDdl(String tableName, String newDdl, long xid) {
        for (HeapTable.PositionedRow row : tablesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_TABLES_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    tablesTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_TABLES, row.pageId(), row.slot());
                    }

                    int tableId = tuple.getValue("table_id") instanceof Number n ? n.intValue() : nextTableId.getAndIncrement();
                    String tableType = (String) tuple.getValue("table_type");
                    String owner = (String) tuple.getValue("owner");
                    insertTableInternal(tableId, tableName, tableType, owner, newDdl, xid);
                    break;
                }
            }
        }
    }

    public void insertColumn(String tableName, String columnName, String dataType, int ordinal, boolean nullable, String defaultVal, long xid) {
        Tuple tuple = new Tuple();
        tuple.addValue("table_name", tableName);
        tuple.addValue("column_name", columnName);
        tuple.addValue("data_type", dataType);
        tuple.addValue("ordinal_position", ordinal);
        tuple.addValue("is_nullable", nullable ? "YES" : "NO");
        tuple.addValue("column_default", defaultVal != null ? defaultVal : "");
        tuple.setXmin(xid);
        tuple.setXmax(0L);
        HeapTable.InsertResult res = columnsTable.insertMvcc(tuple.serialize(), xid);
        if (undoConsumer != null && xid != 0L) {
            undoConsumer.onInsert(TABLE_COLUMNS, res.pageId, res.slot);
        }
    }

    public void deleteColumns(String tableName, long xid) {
        for (HeapTable.PositionedRow row : columnsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_COLUMNS_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    columnsTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_COLUMNS, row.pageId(), row.slot());
                    }
                }
            }
        }
    }

    public void deleteColumn(String tableName, String columnName, long xid) {
        for (HeapTable.PositionedRow row : columnsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_COLUMNS_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name")) &&
                columnName.equalsIgnoreCase((String) tuple.getValue("column_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    columnsTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_COLUMNS, row.pageId(), row.slot());
                    }
                }
            }
        }
    }

    public void renameColumn(String tableName, String oldName, String newName, long xid) {
        for (HeapTable.PositionedRow row : columnsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_COLUMNS_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name")) &&
                oldName.equalsIgnoreCase((String) tuple.getValue("column_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    columnsTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_COLUMNS, row.pageId(), row.slot());
                    }

                    String dataType = (String) tuple.getValue("data_type");
                    int ordinal = tuple.getValue("ordinal_position") instanceof Number n ? n.intValue() : 1;
                    boolean nullable = "YES".equalsIgnoreCase((String) tuple.getValue("is_nullable"));
                    String defVal = (String) tuple.getValue("column_default");
                    insertColumn(tableName, newName, dataType, ordinal, nullable, defVal, xid);
                }
            }
        }
    }

    public void alterColumnType(String tableName, String columnName, String newType, long xid) {
        for (HeapTable.PositionedRow row : columnsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_COLUMNS_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name")) &&
                columnName.equalsIgnoreCase((String) tuple.getValue("column_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    columnsTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_COLUMNS, row.pageId(), row.slot());
                    }

                    int ordinal = tuple.getValue("ordinal_position") instanceof Number n ? n.intValue() : 1;
                    boolean nullable = "YES".equalsIgnoreCase((String) tuple.getValue("is_nullable"));
                    String defVal = (String) tuple.getValue("column_default");
                    insertColumn(tableName, columnName, newType, ordinal, nullable, defVal, xid);
                }
            }
        }
    }

    public void setColumnDefault(String tableName, String columnName, String newDefault, long xid) {
        for (HeapTable.PositionedRow row : columnsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_COLUMNS_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name")) &&
                columnName.equalsIgnoreCase((String) tuple.getValue("column_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    columnsTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_COLUMNS, row.pageId(), row.slot());
                    }

                    String dataType = (String) tuple.getValue("data_type");
                    int ordinal = tuple.getValue("ordinal_position") instanceof Number n ? n.intValue() : 1;
                    boolean nullable = "YES".equalsIgnoreCase((String) tuple.getValue("is_nullable"));
                    insertColumn(tableName, columnName, dataType, ordinal, nullable, newDefault, xid);
                }
            }
        }
    }

    public void dropColumnDefault(String tableName, String columnName, long xid) {
        setColumnDefault(tableName, columnName, "", xid);
    }

    public void renameTable(String oldName, String newName, long xid) {
        for (HeapTable.PositionedRow row : tablesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_TABLES_COLS);
            if (oldName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    tablesTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_TABLES, row.pageId(), row.slot());
                    }

                    int tableId = tuple.getValue("table_id") instanceof Number n ? n.intValue() : nextTableId.getAndIncrement();
                    String tableType = (String) tuple.getValue("table_type");
                    String owner = (String) tuple.getValue("owner");
                    String ddl = (String) tuple.getValue("ddl");
                    if (ddl != null) {
                        ddl = ddl.replaceFirst("(?i)\\b" + oldName + "\\b", newName);
                    }
                    insertTableInternal(tableId, newName, tableType, owner, ddl, xid);
                }
            }
        }
        for (HeapTable.PositionedRow row : columnsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_COLUMNS_COLS);
            if (oldName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    columnsTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_COLUMNS, row.pageId(), row.slot());
                    }

                    String colName = (String) tuple.getValue("column_name");
                    String dataType = (String) tuple.getValue("data_type");
                    int ordinal = tuple.getValue("ordinal_position") instanceof Number n ? n.intValue() : 1;
                    boolean nullable = "YES".equalsIgnoreCase((String) tuple.getValue("is_nullable"));
                    String defVal = (String) tuple.getValue("column_default");
                    insertColumn(newName, colName, dataType, ordinal, nullable, defVal, xid);
                }
            }
        }
        for (HeapTable.PositionedRow row : indexesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_INDEXES_COLS);
            if (oldName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    indexesTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_INDEXES, row.pageId(), row.slot());
                    }

                    String idxName = (String) tuple.getValue("index_name");
                    String col1 = (String) tuple.getValue("column_name");
                    String col2 = (String) tuple.getValue("column_name_2");
                    String idxType = (String) tuple.getValue("index_type");
                    boolean unique = "YES".equalsIgnoreCase((String) tuple.getValue("is_unique"));
                    insertIndex(idxName, newName, col1, col2, idxType, unique, xid);
                }
            }
        }
        for (HeapTable.PositionedRow row : constraintsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_CONSTRAINTS_COLS);
            if (oldName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    constraintsTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_CONSTRAINTS, row.pageId(), row.slot());
                    }

                    String cName = (String) tuple.getValue("constraint_name");
                    String cType = (String) tuple.getValue("constraint_type");
                    String def = (String) tuple.getValue("definition");
                    insertConstraint(cName, newName, cType, def, xid);
                }
            }
        }
    }

    public void insertIndex(String indexName, String tableName, String col1, String col2, String type, boolean isUnique, long xid) {
        Tuple tuple = new Tuple();
        tuple.addValue("index_name", indexName);
        tuple.addValue("table_name", tableName);
        tuple.addValue("column_name", col1);
        tuple.addValue("column_name_2", col2 != null ? col2 : "");
        tuple.addValue("index_type", type);
        tuple.addValue("is_unique", isUnique ? "YES" : "NO");
        tuple.setXmin(xid);
        tuple.setXmax(0L);
        HeapTable.InsertResult res = indexesTable.insertMvcc(tuple.serialize(), xid);
        if (undoConsumer != null && xid != 0L) {
            undoConsumer.onInsert(TABLE_INDEXES, res.pageId, res.slot);
        }
    }

    public void deleteIndex(String indexName, long xid) {
        for (HeapTable.PositionedRow row : indexesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_INDEXES_COLS);
            if (indexName.equalsIgnoreCase((String) tuple.getValue("index_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    indexesTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_INDEXES, row.pageId(), row.slot());
                    }
                }
            }
        }
    }

    public void deleteIndexesForTable(String tableName, long xid) {
        for (HeapTable.PositionedRow row : indexesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_INDEXES_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    indexesTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_INDEXES, row.pageId(), row.slot());
                    }
                }
            }
        }
    }

    public void insertType(String typeName, String category, String values, long xid) {
        Tuple tuple = new Tuple();
        tuple.addValue("type_name", typeName);
        tuple.addValue("type_category", category);
        tuple.addValue("type_values", values != null ? values : "");
        tuple.setXmin(xid);
        tuple.setXmax(0L);
        HeapTable.InsertResult res = typesTable.insertMvcc(tuple.serialize(), xid);
        if (undoConsumer != null && xid != 0L) {
            undoConsumer.onInsert(TABLE_TYPES, res.pageId, res.slot);
        }
    }

    public void deleteType(String typeName, long xid) {
        for (HeapTable.PositionedRow row : typesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_TYPES_COLS);
            if (typeName.equalsIgnoreCase((String) tuple.getValue("type_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    typesTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_TYPES, row.pageId(), row.slot());
                    }
                }
            }
        }
    }

    public void insertConstraint(String constraintName, String tableName, String type, String definition, long xid) {
        Tuple tuple = new Tuple();
        tuple.addValue("constraint_name", constraintName);
        tuple.addValue("table_name", tableName);
        tuple.addValue("constraint_type", type);
        tuple.addValue("definition", definition != null ? definition : "");
        tuple.setXmin(xid);
        tuple.setXmax(0L);
        HeapTable.InsertResult res = constraintsTable.insertMvcc(tuple.serialize(), xid);
        if (undoConsumer != null && xid != 0L) {
            undoConsumer.onInsert(TABLE_CONSTRAINTS, res.pageId, res.slot);
        }
    }

    public void deleteConstraintsForTable(String tableName, long xid) {
        for (HeapTable.PositionedRow row : constraintsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_CONSTRAINTS_COLS);
            if (tableName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                if (tuple.getXmax() == 0L || (transactionManager != null && !transactionManager.isCommitted(tuple.getXmax()))) {
                    byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), xid);
                    constraintsTable.update(row.pageId(), row.slot(), tombstoned);
                    if (undoConsumer != null && xid != 0L) {
                        undoConsumer.onDelete(TABLE_CONSTRAINTS, row.pageId(), row.slot());
                    }
                }
            }
        }
    }

    public void clearNonSystemTables() {
        for (HeapTable.PositionedRow row : tablesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_TABLES_COLS);
            String name = (String) tuple.getValue("table_name");
            if (!isSystemTable(name)) {
                byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), 1L);
                tablesTable.update(row.pageId(), row.slot(), tombstoned);
            }
        }
        for (HeapTable.PositionedRow row : columnsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_COLUMNS_COLS);
            String name = (String) tuple.getValue("table_name");
            if (!isSystemTable(name)) {
                byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), 1L);
                columnsTable.update(row.pageId(), row.slot(), tombstoned);
            }
        }
        for (HeapTable.PositionedRow row : indexesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_INDEXES_COLS);
            String name = (String) tuple.getValue("table_name");
            if (!isSystemTable(name)) {
                byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), 1L);
                indexesTable.update(row.pageId(), row.slot(), tombstoned);
            }
        }
        for (HeapTable.PositionedRow row : typesTable.scanPositioned()) {
            byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), 1L);
            typesTable.update(row.pageId(), row.slot(), tombstoned);
        }
        for (HeapTable.PositionedRow row : constraintsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_CONSTRAINTS_COLS);
            String name = (String) tuple.getValue("table_name");
            if (!isSystemTable(name)) {
                byte[] tombstoned = MVCCVisibility.withXmax(row.stored(), 1L);
                constraintsTable.update(row.pageId(), row.slot(), tombstoned);
            }
        }
    }

    public String getTableDdl(String tableName) {
        for (HeapTable.PositionedRow row : tablesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_TABLES_COLS);
            if (isRowVisible(tuple) && tableName.equalsIgnoreCase((String) tuple.getValue("table_name"))) {
                return (String) tuple.getValue("ddl");
            }
        }
        return null;
    }

    // --- Loading existing catalog entries from system tables ---

    public void loadExistingCatalog(Consumer<TableMetadataLoaded> onTable,
                                    Consumer<ColumnMetadataLoaded> onColumn,
                                    Consumer<IndexMetadataLoaded> onIndex,
                                    Consumer<TypeMetadataLoaded> onType,
                                    Consumer<ConstraintMetadataLoaded> onConstraint) {
        // 1. Tables
        for (HeapTable.PositionedRow row : tablesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_TABLES_COLS);
            if (isRowVisible(tuple)) {
                String tableName = (String) tuple.getValue("table_name");
                if (isSystemTable(tableName)) continue;
                String tableType = (String) tuple.getValue("table_type");
                String owner = (String) tuple.getValue("owner");
                String ddl = (String) tuple.getValue("ddl");
                onTable.accept(new TableMetadataLoaded(tableName, tableType, owner, ddl));
            }
        }

        // 2. Columns
        List<ColumnMetadataLoaded> cols = new ArrayList<>();
        for (HeapTable.PositionedRow row : columnsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_COLUMNS_COLS);
            if (isRowVisible(tuple)) {
                String tableName = (String) tuple.getValue("table_name");
                if (isSystemTable(tableName)) continue;
                String colName = (String) tuple.getValue("column_name");
                String dataType = (String) tuple.getValue("data_type");
                int ordinal = tuple.getValue("ordinal_position") instanceof Number n ? n.intValue() : 1;
                boolean nullable = "YES".equalsIgnoreCase((String) tuple.getValue("is_nullable"));
                String defVal = (String) tuple.getValue("column_default");
                cols.add(new ColumnMetadataLoaded(tableName, colName, dataType, ordinal, nullable, defVal));
            }
        }
        cols.sort(Comparator.comparingInt(ColumnMetadataLoaded::ordinal));
        for (ColumnMetadataLoaded c : cols) {
            onColumn.accept(c);
        }

        // 3. Indexes
        for (HeapTable.PositionedRow row : indexesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_INDEXES_COLS);
            if (isRowVisible(tuple)) {
                String idxName = (String) tuple.getValue("index_name");
                String tblName = (String) tuple.getValue("table_name");
                String col1 = (String) tuple.getValue("column_name");
                String col2 = (String) tuple.getValue("column_name_2");
                String idxType = (String) tuple.getValue("index_type");
                boolean unique = "YES".equalsIgnoreCase((String) tuple.getValue("is_unique"));
                onIndex.accept(new IndexMetadataLoaded(idxName, tblName, col1, col2, idxType, unique));
            }
        }

        // 4. Types
        for (HeapTable.PositionedRow row : typesTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_TYPES_COLS);
            if (isRowVisible(tuple)) {
                String typeName = (String) tuple.getValue("type_name");
                String category = (String) tuple.getValue("type_category");
                String vals = (String) tuple.getValue("type_values");
                onType.accept(new TypeMetadataLoaded(typeName, category, vals));
            }
        }

        // 5. Constraints
        for (HeapTable.PositionedRow row : constraintsTable.scanPositioned()) {
            Tuple tuple = Tuple.deserialize(row.stored(), STRATOS_CONSTRAINTS_COLS);
            if (isRowVisible(tuple)) {
                String cName = (String) tuple.getValue("constraint_name");
                String tblName = (String) tuple.getValue("table_name");
                String cType = (String) tuple.getValue("constraint_type");
                String def = (String) tuple.getValue("definition");
                onConstraint.accept(new ConstraintMetadataLoaded(cName, tblName, cType, def));
            }
        }
    }

    private boolean isRowVisible(Tuple tuple) {
        long xmin = tuple.getXmin();
        long xmax = tuple.getXmax();
        boolean xminOk = (xmin == 0L) || (transactionManager != null && transactionManager.isCommitted(xmin));
        boolean xmaxOk = (xmax == 0L) || (transactionManager != null && !transactionManager.isCommitted(xmax));
        return xminOk && xmaxOk;
    }

    public record TableMetadataLoaded(String tableName, String tableType, String owner, String ddl) {}
    public record ColumnMetadataLoaded(String tableName, String columnName, String dataType, int ordinal, boolean nullable, String defaultValue) {}
    public record IndexMetadataLoaded(String indexName, String tableName, String columnName, String columnName2, String indexType, boolean isUnique) {}
    public record TypeMetadataLoaded(String typeName, String category, String values) {}
    public record ConstraintMetadataLoaded(String constraintName, String tableName, String constraintType, String definition) {}

    // --- DDL Rollback Support ---

    public void rollbackDdl(List<DdlUndoAction> actions) {
        if (actions == null || actions.isEmpty()) return;
        for (int i = actions.size() - 1; i >= 0; i--) {
            try {
                actions.get(i).undo();
            } catch (Exception e) {
                LOG.error("Failed executing DDL undo action", e);
            }
        }
    }

    public void rollbackSavepoint(List<DdlUndoAction> actions, int fromIndex) {
        if (actions == null || fromIndex < 0 || fromIndex >= actions.size()) return;
        for (int i = actions.size() - 1; i >= fromIndex; i--) {
            try {
                actions.remove(i).undo();
            } catch (Exception e) {
                LOG.error("Failed executing savepoint DDL undo action", e);
            }
        }
    }
}
