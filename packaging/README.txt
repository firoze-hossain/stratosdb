StratosDB
=========

A real, from-scratch relational database engine speaking the PostgreSQL
wire protocol v3.

Starting the Server
--------------------
As a System Service (Recommended):
- Linux:    sudo systemctl start stratosdb
- Windows:  net start StratosDB (or use Services MMC)
- macOS:    sudo launchctl load -w /Library/LaunchDaemons/com.stratosdb.server.plist

From the Command Line:
- Linux/macOS:  stratosdb [data-directory] [port] [--cluster]
- Windows:      stratosdb.exe [data-directory] [port] [--cluster]

Defaults: data directory "./stratosdb_data", port 6582.

Add --cluster to run a multi-database server (PostgreSQL-style -
many independently-isolated databases under one server). Without it,
StratosDB runs as a single database per server process.

Configuration File
------------------
StratosDB automatically reads settings from:
- Linux:   /etc/stratosdb/stratosdb.conf
- Windows: C:\ProgramData\StratosDB\stratosdb.conf (or installation directory)
- macOS:   /usr/local/etc/stratosdb/stratosdb.conf

You can customize port, data directory, buffer pool size, and cluster mode.
On Linux, run 'sudo stratosdb-setup' anytime to reconfigure interactively.

Command-Line SQL Client (stdsql)
--------------------------------
StratosDB includes its own native wire-protocol CLI:
  stdsql -h localhost -p 6582 -U anyuser -d anydb

Or use any standard PostgreSQL client:
  psql -h localhost -p 6582 -U anyuser -d anydb

Looking for a GUI Client?
--------------------------
DBNavigator (https://github.com/firoze-hossain/DBNavigator) is a real
database browser/query tool with direct, native support for StratosDB -
schema browsing, multi-database support, an interactive SQL editor, and
row editing - the closest equivalent to pgAdmin or MySQL Workbench.

More Information
----------------
https://github.com/firoze-hossain/stratosdb
