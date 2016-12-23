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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.ResultSet;

import org.junit.Test;

import static org.junit.Assert.*;

public class TableCaseSensitivityIT extends ParallelStatsDisabledIT {
    private String firstName = "Joe";
    private String lastName = "Smith";

    @Test
    public void testCIColumnCSTable() throws SQLException {
        String tableName = String.format("\"%s%s\"",
                "mixedCasePrefix", generateUniqueName());
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " ("
                + "id INTEGER PRIMARY KEY,"
                + "first_name VARCHAR,"
                + "last_name VARCHAR"
                + ")";
        conn.createStatement().execute(ddl);
        String dml = String.format("UPSERT INTO %s VALUES("
                        + "1, '%s', '%s'"
                        + ")",
                tableName, firstName, lastName);
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT first_name from " + tableName);
        verifyCIColumn(rs);

        rs = conn.createStatement().executeQuery("SELECT firsT_Name from " + tableName);
        verifyCIColumn(rs);

        try {
            rs = conn.createStatement().executeQuery("SELECT \"firsT_Name\" from " + tableName);
            fail();
        } catch (SQLException e) {
            // Expected
        }
    }
    private void verifyCIColumn(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        assertEquals(firstName, rs.getString("first_name"));
        assertEquals(firstName, rs.getString("first_Name"));
        assertEquals(firstName, rs.getString("FIRST_NAME"));
        assertFalse(rs.next());
    }

    @Test
    public void testCSColumnCITable() throws SQLException {
        String tableName = "t" + generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + tableName + " ("
                + "id INTEGER PRIMARY KEY,"
                + "\"first_name\" VARCHAR,"
                + "\"last_name\" VARCHAR"
                + ")";
        conn.createStatement().execute(ddl);
        String dml = String.format("UPSERT INTO %s VALUES("
                        + "1, '%s', '%s'"
                        + ")",
                tableName, firstName, lastName);
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs =
                conn.createStatement().executeQuery("SELECT \"first_name\" from " + tableName);
        assertTrue(rs.next());
        assertEquals(firstName, rs.getString("first_name"));
        try {
            rs.getString("first_Name");
            fail();
        } catch (SQLException e) {
            // Expected
        }
        try {
            rs.getString("FIRST_NAME");
            fail();
        } catch (SQLException e) {
            // Expected
        }
    }

}
