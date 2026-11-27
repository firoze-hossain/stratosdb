package com.stratosdb.testing;

import com.stratosdb.core.DatabaseConfig;
import com.stratosdb.core.StratosDB;
import com.stratosdb.network.mysql.MySqlAuth;
import com.stratosdb.network.mysql.MySqlPacketCodec;
import com.stratosdb.network.mysql.MySqlServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 11 Week 21: MySQL Protocol v10 Gateway
 * (HandshakeV10, caching_sha2_password, mysql_native_password, COM_QUERY, COM_PING, COM_QUIT).
 */
class MySqlGatewayTest {

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Test
    void testCachingSha2AuthAndComQueryEndToEnd(@TempDir Path tempDir) throws Exception {
        assertEquals(3306, MySqlServer.DEFAULT_MYSQL_PORT);

        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        int port = findFreePort();

        MySqlServer server = new MySqlServer(port, db, MySqlAuth.PLUGIN_CACHING_SHA2);
        server.addUser("root", "mysql_secret");
        server.start();

        try {
            // Wrong password must fail with ERR_Packet (0xFF)
            try (Socket s = new Socket("127.0.0.1", port)) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                byte[] scramble = readHandshakeV10(in);
                byte[] badToken = MySqlAuth.scrambleCachingSha2("wrong_pw", scramble);
                sendHandshakeResponse41(out, "root", badToken, "stratos", MySqlAuth.PLUGIN_CACHING_SHA2);
                MySqlPacketCodec.Packet err = MySqlPacketCodec.readPacket(in);
                assertEquals(0xFF, err.payload()[0] & 0xFF, "Wrong password must return ERR_Packet");
            }

            // Valid caching_sha2_password login + COM_PING + COM_QUERY
            try (Socket s = new Socket("127.0.0.1", port)) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();

                byte[] scramble = readHandshakeV10(in);
                byte[] token = MySqlAuth.scrambleCachingSha2("mysql_secret", scramble);
                sendHandshakeResponse41(out, "root", token, "stratos", MySqlAuth.PLUGIN_CACHING_SHA2);

                // Fast-auth AuthMoreData (0x01, 0x03) followed by OK_Packet (0x00)
                MySqlPacketCodec.Packet more = MySqlPacketCodec.readPacket(in);
                assertEquals(0x01, more.payload()[0] & 0xFF);
                assertEquals(0x03, more.payload()[1] & 0xFF);
                MySqlPacketCodec.Packet authOk = MySqlPacketCodec.readPacket(in);
                assertEquals(0x00, authOk.payload()[0] & 0xFF);

                // COM_PING
                MySqlPacketCodec.writePacket(out, 0, new byte[]{MySqlPacketCodec.COM_PING});
                out.flush();
                assertEquals(0x00, MySqlPacketCodec.readPacket(in).payload()[0] & 0xFF);

                // Workbench/CLI probe: SET NAMES utf8mb4
                sendComQuery(out, "SET NAMES utf8mb4");
                assertEquals(0x00, MySqlPacketCodec.readPacket(in).payload()[0] & 0xFF);

                // Workbench/CLI probe: SELECT @@version_comment LIMIT 1
                sendComQuery(out, "SELECT @@version_comment LIMIT 1");
                List<List<String>> probeRows = readResultSet(in);
                assertEquals(1, probeRows.size());
                assertEquals("StratosDB MySQL Gateway", probeRows.get(0).get(0));

                // DDL + DML + SELECT via COM_QUERY
                sendComQuery(out, "CREATE TABLE products (id INT, name VARCHAR, price INT)");
                assertEquals(0x00, MySqlPacketCodec.readPacket(in).payload()[0] & 0xFF);

                sendComQuery(out, "INSERT INTO products VALUES (1, 'Laptop', 1200)");
                assertEquals(0x00, MySqlPacketCodec.readPacket(in).payload()[0] & 0xFF);

                sendComQuery(out, "INSERT INTO products VALUES (2, 'Mouse', 25)");
                assertEquals(0x00, MySqlPacketCodec.readPacket(in).payload()[0] & 0xFF);

                sendComQuery(out, "SELECT id, name, price FROM products ORDER BY id");
                List<List<String>> rows = readResultSet(in);
                assertEquals(2, rows.size());
                assertEquals(List.of("1", "Laptop", "1200"), rows.get(0));
                assertEquals(List.of("2", "Mouse", "25"), rows.get(1));

                // COM_QUIT
                MySqlPacketCodec.writePacket(out, 0, new byte[]{MySqlPacketCodec.COM_QUIT});
                out.flush();
            }
        } finally {
            server.stop();
            db.shutdown();
        }
    }

    @Test
    void testMysqlNativePasswordAuth(@TempDir Path tempDir) throws Exception {
        DatabaseConfig cfg = new DatabaseConfig();
        cfg.setDataDirectory(tempDir.toString());
        StratosDB db = new StratosDB(cfg);
        int port = findFreePort();

        MySqlServer server = new MySqlServer(port, db, MySqlAuth.PLUGIN_NATIVE_PASSWORD);
        server.addUser("admin", "native_pass");
        server.start();

        try (Socket s = new Socket("127.0.0.1", port)) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            OutputStream out = s.getOutputStream();

            byte[] scramble = readHandshakeV10(in);
            byte[] token = MySqlAuth.scrambleNativePassword("native_pass", scramble);
            sendHandshakeResponse41(out, "admin", token, "stratos", MySqlAuth.PLUGIN_NATIVE_PASSWORD);

            MySqlPacketCodec.Packet ok = MySqlPacketCodec.readPacket(in);
            assertEquals(0x00, ok.payload()[0] & 0xFF, "mysql_native_password must return OK_Packet");

            sendComQuery(out, "SELECT VERSION()");
            List<List<String>> rows = readResultSet(in);
            assertEquals(1, rows.size());
            assertTrue(rows.get(0).get(0).contains("StratosDB"));
        } finally {
            server.stop();
            db.shutdown();
        }
    }

    private static byte[] readHandshakeV10(DataInputStream in) throws IOException {
        MySqlPacketCodec.Packet pkt = MySqlPacketCodec.readPacket(in);
        byte[] buf = pkt.payload();
        assertEquals(10, buf[0] & 0xFF, "Protocol version must be 10");
        int pos = 1;
        while (buf[pos] != 0) pos++;
        pos++; // skip NUL
        pos += 4; // connection_id
        byte[] scramble = new byte[20];
        System.arraycopy(buf, pos, scramble, 0, 8);
        pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10; // filler + caps1 + charset + status + caps2 + authLen + reserved
        System.arraycopy(buf, pos, scramble, 8, 12);
        return scramble;
    }

    private static void sendHandshakeResponse41(OutputStream out, String user, byte[] authResp, String db, String plugin) throws IOException {
        int caps = MySqlPacketCodec.CLIENT_LONG_PASSWORD | MySqlPacketCodec.CLIENT_PROTOCOL_41
            | MySqlPacketCodec.CLIENT_SECURE_CONNECTION | MySqlPacketCodec.CLIENT_CONNECT_WITH_DB
            | MySqlPacketCodec.CLIENT_PLUGIN_AUTH | MySqlPacketCodec.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
            | MySqlPacketCodec.CLIENT_DEPRECATE_EOF;
        ByteArrayOutputStream buf = new ByteArrayOutputStream(128);
        MySqlPacketCodec.writeInt4LE(buf, caps);
        MySqlPacketCodec.writeInt4LE(buf, 16 * 1024 * 1024);
        buf.write(MySqlPacketCodec.DEFAULT_CHARSET_UTF8MB4);
        buf.write(new byte[23]);
        buf.write(user.getBytes(StandardCharsets.UTF_8));
        buf.write(0);
        buf.write(authResp.length);
        buf.write(authResp);
        buf.write(db.getBytes(StandardCharsets.UTF_8));
        buf.write(0);
        buf.write(plugin.getBytes(StandardCharsets.UTF_8));
        buf.write(0);
        MySqlPacketCodec.writePacket(out, 1, buf.toByteArray());
        out.flush();
    }

    private static void sendComQuery(OutputStream out, String sql) throws IOException {
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[1 + sqlBytes.length];
        payload[0] = MySqlPacketCodec.COM_QUERY;
        System.arraycopy(sqlBytes, 0, payload, 1, sqlBytes.length);
        MySqlPacketCodec.writePacket(out, 0, payload);
        out.flush();
    }

    private static List<List<String>> readResultSet(DataInputStream in) throws IOException {
        MySqlPacketCodec.Packet colCountPkt = MySqlPacketCodec.readPacket(in);
        int colCount = colCountPkt.payload()[0] & 0xFF;
        for (int i = 0; i < colCount; i++) {
            MySqlPacketCodec.readPacket(in); // ColumnDefinition41
        }
        List<List<String>> rows = new ArrayList<>();
        while (true) {
            MySqlPacketCodec.Packet pkt = MySqlPacketCodec.readPacket(in);
            byte[] p = pkt.payload();
            int first = p[0] & 0xFF;
            if ((first == 0x00 || first == 0xFE) && p.length < 9) {
                break; // OK_Packet (CLIENT_DEPRECATE_EOF) or EOF_Packet
            }
            List<String> row = new ArrayList<>(colCount);
            int pos = 0;
            for (int c = 0; c < colCount; c++) {
                int len = p[pos++] & 0xFF;
                if (len == 0xFB) {
                    row.add(null);
                } else {
                    row.add(new String(p, pos, len, StandardCharsets.UTF_8));
                    pos += len;
                }
            }
            rows.add(row);
        }
        return rows;
    }
}
