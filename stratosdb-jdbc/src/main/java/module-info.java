module stratosdb.jdbc {
    requires transitive java.sql;
    requires java.logging;
    requires org.slf4j;

    exports com.stratosdb.jdbc;

    provides java.sql.Driver with com.stratosdb.jdbc.StratosDriver;
}
