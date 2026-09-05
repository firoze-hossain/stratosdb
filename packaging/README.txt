StratosDB
=========

A real, from-scratch relational database engine speaking the PostgreSQL
wire protocol.

Starting the server
--------------------
Linux/macOS:  stratosdb [data-directory] [port] [--cluster]
Windows:      stratosdb.exe [data-directory] [port] [--cluster]

Defaults: data directory "./stratosdb_data", port 6582.

Add --cluster to run a real, multi-database server (PostgreSQL-style -
many independently-isolated databases under one server). Without it,
StratosDB runs as a single database per server process, matching a
tool like SQLite in that respect.

Connecting
----------
Any real PostgreSQL client works: psql, JDBC/ODBC drivers, or your own
tooling that speaks the wire protocol.

  psql -h localhost -p 6582 -U anyuser -d stratos

Looking for a GUI client?
--------------------------
DBNavigator (https://github.com/firoze-hossain/DBNavigator) is a real
database browser/query tool with direct, native support for StratosDB -
schema browsing, multi-database support, a query console, and row
editing - the closest equivalent to pgAdmin for PostgreSQL or MySQL
Workbench for MySQL.

More information
------------------
https://github.com/firoze-hossain/stratosdb
