package org.h2.test.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * @author aschoerk
 */
public class TestCatalog extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    /**
     * This method will be called by the test framework.
     *
     * @throws Exception if an exception in the test occurs
     */
    @Override
    public void test() throws Exception {
        // canCreateCatalogAndDrop();
        // canCreateSchemaAndDrop();
        // canDoCataloglessYet();
        canCreateCatalogSchemaAndTable();

    }

    private void canCreateCatalogAndDrop() throws Exception {
        try (Connection conn = getConnection("catalogs")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("drop catalog catalog1 if exists cascade ");
                stat.execute("create catalog catalog1");
                stat.execute("create schema catalog1.dbo");
                stat.execute("create table catalog1.dbo.table1(a varchar)");
                stat.execute("drop schema catalog1.dbo cascade");
                stat.execute("drop catalog catalog1");
                stat.execute("create catalog catalog1");
                stat.execute("drop catalog catalog1");
            }
        }
        try (Connection conn = getConnection("catalogs")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("create catalog catalog1");
                stat.execute("create schema catalog1.dbo");
                stat.execute("create table catalog1.dbo.table1(a varchar)");
            }
        }
        try (Connection conn = getConnection("catalogs")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("drop schema catalog1.dbo cascade");
                stat.execute("drop catalog catalog1");
                stat.execute("create catalog catalog1");
                stat.execute("drop catalog catalog1");
            }
        }

    }
    private void canCreateSchemaAndDrop() throws Exception {
        try (Connection conn = getConnection("catalogs")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("drop schema dbo if exists cascade");
                stat.execute("create schema dbo");
                stat.execute("create table dbo.table1(a varchar)");
                stat.execute("drop schema dbo cascade");
                stat.execute("create schema dbo");
                stat.execute("create table dbo.table1(a varchar)");
                stat.execute("drop schema dbo cascade");

            }
        }
        try (Connection conn = getConnection("catalogs")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("create schema dbo");
                stat.execute("create table dbo.table1(a varchar)");
            }
        }
        try (Connection conn = getConnection("catalogs")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("drop schema dbo cascade");
            }
        }
    }

    private void canDoCataloglessYet() throws Exception {
        try (Connection conn = getConnection("catalogs")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("drop all objects");
                stat.execute("create table table1(c varchar)");
                stat.execute("insert into table1 (c) values ('1')");
                stat.execute("insert into table1 (c) values ('2')");
                stat.execute("drop table table1");
            }
        }
    }

    private void canCreateCatalogSchemaAndTable() throws Exception {
        try (Connection conn = getConnection("catalogs")) {
            try (Statement stat = conn.createStatement()) {
                stat.execute("drop all objects");
                stat.execute("create catalog catalog1");
                stat.execute("create schema catalog1.dbo");
                stat.execute("create schema dbo");
                stat.execute("create table catalog1.dbo.table1(a varchar)");
                stat.execute("create table dbo.table1(b varchar)");
                stat.execute("create table table1(c varchar)");
                stat.execute("insert into catalog1.dbo.table1 (a) values ('1')");
                stat.execute("insert into catalog1.dbo.table1 (a) values ('2')");
                stat.execute("insert into dbo.table1 (b) values ('1')");
                stat.execute("insert into dbo.table1 (b) values ('2')");
                stat.execute("insert into table1 (c) values ('1')");
                stat.execute("insert into table1 (c) values ('2')");
                checkResults(stat,"select * from table1 t1 join dbo.table1 t2 on t1.c = t2.b",2);
                checkResults(stat,"select * from table1 t1 join catalog1.dbo.table1 t2 on t1.c = t2.a",2);
                stat.execute("drop catalog catalog1 cascade");
                stat.execute("drop schema dbo cascade");
            }
        }
    }

    private void checkResults(final Statement stat, String sql, int expected) throws SQLException {
        try (ResultSet rs = stat.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            assertEquals(expected, count);
        }
    }
}
