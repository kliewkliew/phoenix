/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Test;


public class DefaultColumnValueIT extends BaseClientManagedTimeIT {

    @Test
    public void testDefaultColumnValue() throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS T_DEFAULT (" +
                "pk1 INTEGER NOT NULL, " +
                "pk2 INTEGER NOT NULL, " +
                "pk3 INTEGER NOT NULL DEFAULT 10, " +
                "test1 INTEGER, " +
                "test2 INTEGER DEFAULT 5, " +
                "test3 INTEGER, " +
                "CONSTRAINT NAME_PK PRIMARY KEY (pk1, pk2, pk3))";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO T_DEFAULT VALUES (1, 2)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO T_DEFAULT VALUES (11, 12, 13, 14, null, 16)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        String projection = "*";

        ResultSet rs = conn.createStatement().executeQuery("SELECT " + projection + " FROM T_DEFAULT WHERE pk1 = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(10, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
        assertTrue(rs.wasNull());
        assertEquals(5, rs.getInt(5));
        assertEquals(0, rs.getInt(6));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT " + projection + " FROM T_DEFAULT WHERE pk1 = 11");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(12, rs.getInt(2));
        assertEquals(13, rs.getInt(3));
        assertEquals(14, rs.getInt(4));
        assertEquals(0, rs.getInt(5));
        assertTrue(rs.wasNull());
        assertEquals(16, rs.getInt(6));
        assertFalse(rs.next());
    }

    @Test
    public void testDefaultColumnValueProjected() throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS T_DEFAULT (" +
                "pk1 INTEGER NOT NULL, " +
                "pk2 INTEGER NOT NULL, " +
                "pk3 INTEGER NOT NULL DEFAULT 10, " +
                "test1 INTEGER, " +
                "test2 INTEGER DEFAULT 5, " +
                "test3 INTEGER, " +
                "CONSTRAINT NAME_PK PRIMARY KEY (pk1, pk2, pk3))";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO T_DEFAULT VALUES (1, 2)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO T_DEFAULT VALUES (11, 12, 13, 14, null, 16)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        String projection = "pk1, pk2, pk3, test1, test2, test3";

        ResultSet rs = conn.createStatement().executeQuery("SELECT " + projection + " FROM T_DEFAULT WHERE pk1 = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(10, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
        assertTrue(rs.wasNull());
        assertEquals(5, rs.getInt(5));
        assertEquals(0, rs.getInt(6));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT " + projection + " FROM T_DEFAULT WHERE pk1 = 11");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(12, rs.getInt(2));
        assertEquals(13, rs.getInt(3));
        assertEquals(14, rs.getInt(4));
        assertEquals(0, rs.getInt(5));
        assertTrue(rs.wasNull());
        assertEquals(16, rs.getInt(6));
        assertFalse(rs.next());

        projection = "pk1, pk3, pk2, test1, test3, test2";

        rs = conn.createStatement().executeQuery("SELECT " + projection + " FROM T_DEFAULT WHERE pk1 = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(10, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt(5));
        assertTrue(rs.wasNull());
        assertEquals(5, rs.getInt(6));
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT " + projection + " FROM T_DEFAULT WHERE pk1 = 11");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(13, rs.getInt(2));
        assertEquals(12, rs.getInt(3));
        assertEquals(14, rs.getInt(4));
        assertEquals(16, rs.getInt(5));
        assertEquals(0, rs.getInt(6));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
    }

    @Test
    public void testMultipleDefaults() throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS T_DEFAULT2 (" +
                "pk1 INTEGER NOT NULL, " +
                "pk2 INTEGER NOT NULL DEFAULT 5, " +
                "pk3 INTEGER NOT NULL DEFAULT 10, " +
                "test1 INTEGER, " +
                "test2 INTEGER DEFAULT 50, " +
                "test3 INTEGER DEFAULT 100, " +
                "test4 INTEGER, " +
                "CONSTRAINT NAME_PK PRIMARY KEY (pk1, pk2, pk3))";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO T_DEFAULT2 VALUES (1)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO T_DEFAULT2 VALUES (11, 12, 13, 21, null, null, 24)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM T_DEFAULT2 WHERE pk1 = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(5, rs.getInt(2));
        assertEquals(10, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
        assertTrue(rs.wasNull());
        assertEquals(50, rs.getInt(5));
        assertEquals(100, rs.getInt(6));
        assertEquals(0, rs.getInt(7));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT * FROM T_DEFAULT2 WHERE pk1 = 11");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(12, rs.getInt(2));
        assertEquals(13, rs.getInt(3));
        assertEquals(21, rs.getInt(4));
        assertEquals(0, rs.getInt(5));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt(6));
        assertTrue(rs.wasNull());
        assertEquals(24, rs.getInt(7));
        assertFalse(rs.next());
    }

    @Test
    public void testDefaultImmutableRows() throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS T_DEFAULT2 (" +
                "pk1 INTEGER NOT NULL, " +
                "pk2 INTEGER NOT NULL DEFAULT 5, " +
                "pk3 INTEGER NOT NULL DEFAULT 10, " +
                "test1 INTEGER, " +
                "test2 INTEGER DEFAULT 50, " +
                "test3 INTEGER DEFAULT 100, " +
                "test4 INTEGER, " +
                "CONSTRAINT NAME_PK PRIMARY KEY (pk1, pk2, pk3))"
                + "IMMUTABLE_ROWS=true";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO T_DEFAULT2 VALUES (1)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO T_DEFAULT2 VALUES (11, 12, 13, 21, null, null, 24)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM T_DEFAULT2 WHERE pk1 = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(5, rs.getInt(2));
        assertEquals(10, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
        assertTrue(rs.wasNull());
        assertEquals(50, rs.getInt(5));
        assertEquals(100, rs.getInt(6));
        assertEquals(0, rs.getInt(7));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT * FROM T_DEFAULT2 WHERE pk1 = 11");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(12, rs.getInt(2));
        assertEquals(13, rs.getInt(3));
        assertEquals(21, rs.getInt(4));
        assertEquals(0, rs.getInt(5));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt(6));
        assertTrue(rs.wasNull());
        assertEquals(24, rs.getInt(7));
        assertFalse(rs.next());
    }

    @Test
    public void testStatefulDefault() throws Exception {
        String ddl = "CREATE TABLE IF NOT EXISTS DEFAULT_DATE (" +
                "pk INTEGER PRIMARY KEY, " +
                "datecol DATE DEFAULT NOW)";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            // Expected
        }
    }

    @Test
    public void testTrailingNullOverwritingDefault() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE " + table + " (" +
                "pk INTEGER PRIMARY KEY, " +
                "mid INTEGER, " +
                "def INTEGER DEFAULT 10)";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + table + " VALUES (1, 10, null)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(10, rs.getInt(2));
        assertEquals(0, rs.getInt(3));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
    }

    @Test
    public void testDefaultReinit() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "pk INTEGER PRIMARY KEY, " +
                "test INTEGER DEFAULT 5)";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + table + " VALUES (1)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(5, rs.getInt(2));
        assertFalse(rs.next());

        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 50));
        Connection conn2 = DriverManager.getConnection(getUrl(), props);

        rs = conn2.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(5, rs.getInt(2));
        assertFalse(rs.next());
    }

    @Test
    public void testDefaultMiddlePrimaryKey() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "pk1 INTEGER NOT NULL, " +
                "pk2 INTEGER NOT NULL DEFAULT 100, " +
                "pk3 INTEGER NOT NULL, " +
                "test1 INTEGER, " +
                "CONSTRAINT NAME_PK PRIMARY KEY (pk1, pk2, pk3))";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + table + " VALUES (1)";
        try {
            conn.createStatement().execute(dml);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CONSTRAINT_VIOLATION.getErrorCode(), e.getErrorCode());
        }

        dml = "UPSERT INTO " + table + " VALUES (1, 2)";
        try {
            conn.createStatement().execute(dml);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CONSTRAINT_VIOLATION.getErrorCode(), e.getErrorCode());
        }

        dml = "UPSERT INTO " + table + " VALUES (1, 2, 3)";
        conn.createStatement().execute(dml);

        dml = "UPSERT INTO " + table + " (pk1, pk3) VALUES (11, 13)";
        conn.createStatement().execute(dml);

        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk1 = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(3, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk1 = 11");
        assertTrue(rs.next());
        assertEquals(11, rs.getInt(1));
        assertEquals(100, rs.getInt(2));
        assertEquals(13, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
    }

    @Test
    public void testDefaultMiddleKeyValueCol() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + "("
                + "pk INTEGER PRIMARY KEY,"
                + "c1 INTEGER,"
                + "c2 INTEGER DEFAULT 50,"
                + "c3 INTEGER)";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + table + " VALUES (1)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + table + " (pk, c3) VALUES (10, 100)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(0, rs.getInt(2));
        assertTrue(rs.wasNull());
        assertEquals(50, rs.getInt(3));
        assertEquals(0, rs.getInt(4));
        assertTrue(rs.wasNull());

        rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk = 10");
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        assertEquals(0, rs.getInt(2));
        assertTrue(rs.wasNull());
        assertEquals(50, rs.getInt(3));
        assertEquals(100, rs.getInt(4));
    }

    @Test
    public void testDefaultMostDataTypesKeyValueCol() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + "("
                + "pk INTEGER PRIMARY KEY,"
                + "int INTEGER DEFAULT -100,"
                + "uint UNSIGNED_INT DEFAULT 100, "
                + "bint BIGINT DEFAULT -200,"
                + "ubint UNSIGNED_LONG DEFAULT 200,"
                + "tint TINYINT DEFAULT -50,"
                + "utint UNSIGNED_TINYINT DEFAULT 50,"
                + "sint SMALLINT DEFAULT -10,"
                + "usint UNSIGNED_SMALLINT DEFAULT 10,"
                + "flo FLOAT DEFAULT -100.8,"
                + "uflo UNSIGNED_FLOAT DEFAULT 100.9,"
                + "doub DOUBLE DEFAULT -200.5,"
                + "udoubl UNSIGNED_DOUBLE DEFAULT 200.8,"
                + "dec DECIMAL DEFAULT -654624562.3462642362,"
                + "bool BOOLEAN DEFAULT true,"
                + "tim TIME DEFAULT time '1900-10-01 14:03:22.559',"
                + "dat DATE DEFAULT date '1900-10-01 14:03:22.559',"
                + "timest TIMESTAMP DEFAULT timestamp '1900-10-01 14:03:22.559',"
                + "utim UNSIGNED_TIME DEFAULT time '2005-10-01 14:03:22.559',"
                + "udat UNSIGNED_DATE DEFAULT date '2005-10-01 14:03:22.559',"
                + "utimest UNSIGNED_TIMESTAMP DEFAULT timestamp '2005-10-01 14:03:22.559',"
                + "vc VARCHAR DEFAULT 'ABCD',"
                + "c CHAR(5) DEFAULT 'EF'"
                + ")";

        testDefaultMostDataTypes(table, ddl);
    }

    @Test
    public void testDefaultMostDataTypesPrimaryKey() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + "("
                + "pk INTEGER NOT NULL,"
                + "int INTEGER NOT NULL DEFAULT -100,"
                + "uint UNSIGNED_INT NOT NULL DEFAULT 100, "
                + "bint BIGINT NOT NULL DEFAULT -200,"
                + "ubint UNSIGNED_LONG NOT NULL DEFAULT 200,"
                + "tint TINYINT NOT NULL DEFAULT -50,"
                + "utint UNSIGNED_TINYINT NOT NULL DEFAULT 50,"
                + "sint SMALLINT NOT NULL DEFAULT -10,"
                + "usint UNSIGNED_SMALLINT NOT NULL DEFAULT 10,"
                + "flo FLOAT NOT NULL DEFAULT -100.8,"
                + "uflo UNSIGNED_FLOAT NOT NULL DEFAULT 100.9,"
                + "doub DOUBLE NOT NULL DEFAULT -200.5,"
                + "udoub UNSIGNED_DOUBLE NOT NULL DEFAULT 200.8,"
                + "dec DECIMAL NOT NULL DEFAULT -654624562.3462642362,"
                + "bool BOOLEAN NOT NULL DEFAULT true,"
                + "tim TIME NOT NULL DEFAULT time '1900-10-01 14:03:22.559',"
                + "dat DATE NOT NULL DEFAULT date '1900-10-01 14:03:22.559',"
                + "timest TIMESTAMP NOT NULL DEFAULT timestamp '1900-10-01 14:03:22.559',"
                + "utim UNSIGNED_TIME NOT NULL DEFAULT time '2005-10-01 14:03:22.559',"
                + "udat UNSIGNED_DATE NOT NULL DEFAULT date '2005-10-01 14:03:22.559',"
                + "utimest UNSIGNED_TIMESTAMP NOT NULL DEFAULT timestamp '2005-10-01 14:03:22.559',"
                + "vc VARCHAR NOT NULL DEFAULT 'ABCD',"
                + "c CHAR(5) NOT NULL DEFAULT 'EF',"
                + "CONSTRAINT pk_final PRIMARY KEY (pk, int, uint, bint, ubint, tint, utint,"
                + "sint, usint, flo, uflo, doub, udoub, dec, bool, tim, dat, timest, utim, udat, utimest,"
                + "vc, c)"
                + ")";

        testDefaultMostDataTypes(table, ddl);
    }

    private void testDefaultMostDataTypes(String table, String ddl) throws SQLException {

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + table + " VALUES (1)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(-100, rs.getInt(2));
        assertEquals(100, rs.getInt(3));
        assertEquals(-200, rs.getLong(4));
        assertEquals(200, rs.getLong(5));
        assertEquals(-50, rs.getByte(6));
        assertEquals(50, rs.getByte(7));
        assertEquals(-10, rs.getShort(8));
        assertEquals(10, rs.getShort(9));
        assertEquals(new Float(-100.8), rs.getFloat(10), 0);
        assertEquals(new Float(100.9), rs.getFloat(11), 0);
        assertEquals(-200.5, rs.getDouble(12), 0);
        assertEquals(200.8, rs.getDouble(13), 0);
        assertEquals(new BigDecimal("-654624562.3462642362"), rs.getBigDecimal(14));
        assertEquals(true, rs.getBoolean(15));
        assertEquals(DateUtil.parseTime("1900-10-01 14:03:22.559"), rs.getTime(16));
        assertEquals(DateUtil.parseDate("1900-10-01 14:03:22.559"), rs.getDate(17));
        assertEquals(DateUtil.parseTimestamp("1900-10-01 14:03:22.559"), rs.getTimestamp(18));
        assertEquals(DateUtil.parseTime("2005-10-01 14:03:22.559"), rs.getTime(19));
        assertEquals(DateUtil.parseDate("2005-10-01 14:03:22.559"), rs.getDate(20));
        assertEquals(DateUtil.parseTimestamp("2005-10-01 14:03:22.559"), rs.getTimestamp(21));
        assertEquals("ABCD", rs.getString(22));
        assertEquals("EF", rs.getString(23));
    }

    @Test
    public void testDefaultExpressionKeyValueCol() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "pk INTEGER PRIMARY KEY,"
                + "c1 INTEGER DEFAULT 1 + 9,"
                + "c2 DOUBLE DEFAULT SQRT(91506.25),"
                + "c3 DECIMAL DEFAULT TO_NUMBER('$123.33', '\u00A4###.##'),"
                + "c4 VARCHAR DEFAULT 'AB' || 'CD',"
                + "c5 CHAR(5) DEFAULT 'E' || 'F',"
                + "c6 INTEGER DEFAULT MONTH(TO_TIMESTAMP('2015-6-05'))"
                + ")";

        testDefaultExpression(table, ddl);
    }

    @Test
    public void testDefaultExpressionPrimaryKey() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + " (" +
                "pk INTEGER NOT NULL,"
                + "c1 INTEGER NOT NULL DEFAULT 1 + 9,"
                + "c2 DOUBLE NOT NULL DEFAULT SQRT(91506.25),"
                + "c3 DECIMAL NOT NULL DEFAULT TO_NUMBER('$123.33', '\u00A4###.##'),"
                + "c4 VARCHAR NOT NULL DEFAULT 'AB' || 'CD',"
                + "c5 CHAR(5) NOT NULL DEFAULT 'E' || 'F',"
                + "c6 INTEGER NOT NULL DEFAULT MONTH(TO_TIMESTAMP('2015-6-05')),"
                + "CONSTRAINT pk_key PRIMARY KEY (pk,c1,c2,c3,c4,c5,c6)"
                + ")";

        testDefaultExpression(table, ddl);
    }

    private void testDefaultExpression(String table, String ddl) throws SQLException {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + table + " VALUES (1)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(10, rs.getInt(2));
        assertEquals(302.5, rs.getDouble(3), 0);
        assertEquals(new BigDecimal("123.33"), rs.getBigDecimal(4));
        assertEquals("ABCD", rs.getString(5));
        assertEquals("EF", rs.getString(6));
        assertEquals(6, rs.getInt(7));
        assertFalse(rs.next());
    }

    @Test
    public void testDefaultUpsertSelectPrimaryKey() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String selectTable = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + selectTable + " ("
                + "pk INTEGER PRIMARY KEY)";
        conn.createStatement().execute(ddl);

        String table = generateUniqueName();
        ddl = "CREATE TABLE IF NOT EXISTS " + table + " ("
                + "pk1 INTEGER NOT NULL, "
                + "pk2 INTEGER NOT NULL DEFAULT 100,"
                + "CONSTRAINT pk_key PRIMARY KEY(pk1, pk2))";
        conn.createStatement().execute(ddl);
        conn.commit();

        String dml = "UPSERT INTO " + selectTable + " VALUES (1)";
        conn.createStatement().execute(dml);
        dml = "UPSERT INTO " + selectTable + " VALUES (2)";
        conn.createStatement().execute(dml);
        conn.commit();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        dml = "UPSERT INTO " + table + " (pk1) SELECT pk FROM " + selectTable;
        conn.createStatement().executeUpdate(dml);
        dml = "UPSERT INTO " + table + " SELECT pk,pk FROM " + selectTable;
        conn.createStatement().executeUpdate(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + selectTable);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs =conn.createStatement().executeQuery("SELECT * FROM " + table);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(100, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(100, rs.getInt(2));
    }

    @Test
    public void testDefaultArrays() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE IF NOT EXISTS " + table + "("
                + "pk INTEGER PRIMARY KEY,"
                + "int INTEGER[5] DEFAULT ARRAY[-100, 50],"
                + "uint UNSIGNED_INT[5] DEFAULT ARRAY[100, 50], "
                + "bint BIGINT[5] DEFAULT ARRAY[-200, 100],"
                + "ubint UNSIGNED_LONG[5] DEFAULT ARRAY[200, 100],"
                + "tint TINYINT[5] DEFAULT ARRAY[-50, 25],"
                + "utint UNSIGNED_TINYINT[5] DEFAULT ARRAY[50, 25],"
                + "sint SMALLINT[5] DEFAULT ARRAY[-10, 5],"
                + "usint UNSIGNED_SMALLINT[5] DEFAULT ARRAY[10, 5],"
                + "flo FLOAT[5] DEFAULT ARRAY[-100.8, 50.4],"
                + "uflo UNSIGNED_FLOAT[5] DEFAULT ARRAY[100.9, 50.45],"
                + "doub DOUBLE[5] DEFAULT ARRAY[-200.5, 100.25],"
                + "udoubl UNSIGNED_DOUBLE[5] DEFAULT ARRAY[200.8, 100.4],"
                + "dec DECIMAL[5] DEFAULT ARRAY[-654624562.3462642362, 1],"
                + "bool BOOLEAN[5] DEFAULT ARRAY[true, false],"
                + "tim TIME[5] DEFAULT ARRAY[time '1900-10-01 14:03:22.559', time '1901-10-01 14:03:22.559'],"
                + "dat DATE[5] DEFAULT ARRAY[date '1900-10-01 14:03:22.559', date '1901-10-01 14:03:22.559'],"
                + "timest TIMESTAMP[5] DEFAULT ARRAY[timestamp '1900-10-01 14:03:22.559', timestamp '1901-10-01 14:03:22.559'],"
                + "utim UNSIGNED_TIME[5] DEFAULT ARRAY[time '2005-10-01 14:03:22.559', time '2006-10-01 14:03:22.559'],"
                + "udat UNSIGNED_DATE[5] DEFAULT ARRAY[date '2005-10-01 14:03:22.559', date '2006-10-01 14:03:22.559'],"
                + "utimest UNSIGNED_TIMESTAMP[5] DEFAULT ARRAY[timestamp '2005-10-01 14:03:22.559', timestamp '2006-10-01 14:03:22.559'],"
                + "vc VARCHAR[5] DEFAULT ARRAY['ABCD', 'XY'],"
                + "c CHAR(5)[5] DEFAULT ARRAY['EF', 'Z']"
                + ")";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_CREATE_DEFAULT.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testDefaultNull() throws Exception {
        String table = generateUniqueName();
        String ddl = "CREATE TABLE " + table + " (" +
                "pk INTEGER PRIMARY KEY, " +
                "def INTEGER DEFAULT NULL)";

        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);

        String dml = "UPSERT INTO " + table + " VALUES (1)";
        conn.createStatement().execute(dml);
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);

        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + table + " WHERE pk = 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(0, rs.getInt(2));
        assertTrue(rs.wasNull());
        assertFalse(rs.next());
    }
}
