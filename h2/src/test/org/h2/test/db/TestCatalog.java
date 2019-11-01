package org.h2.test.db;

import java.sql.Connection;
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
        canCreateCatalogSchemaAndTable();

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
                stat.execute("select * from table1 t1 join dbo.table1 t2 on t1.c = t2.b");
                stat.execute("select * from table1 t1 join catalog1.dbo.table1 t2 on t1.c = t2.a");
            }
        }
    }
}
