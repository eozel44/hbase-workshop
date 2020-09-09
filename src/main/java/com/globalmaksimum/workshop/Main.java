package com.globalmaksimum.workshop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        String tableName = "test";
        String columnFamily = "cf";
        try {

            if (true == delete(tableName)) {
                System.out.println("Delete Table " + tableName + " success!");

            }

            create(tableName, columnFamily);

            put(tableName, "row1", columnFamily, "column", "data1");
            put(tableName, "row2", columnFamily, "column", "data2");
            put(tableName, "row3", columnFamily, "column", "data3");
            put(tableName, "row4", columnFamily, "column", "data4");
            put(tableName, "row5", columnFamily, "column", "data5");
            put(tableName, "row44", columnFamily, "column", "data44");


            //scanregexstringcomparator
            put(tableName, "row45", columnFamily, "column", "data45");
            put(tableName, "row46", columnFamily, "column", "notonlydata46");
            put(tableName, "row47", columnFamily, "column", "data47");
            put(tableName, "row48", columnFamily, "column", "notonlydata48");

            get(tableName, "row1");
            scan(tableName);
            scanfilter(tableName);
            scanrowprefixfilter(tableName);
            scanregexstringcomparator(tableName, columnFamily);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void create(String tableName, String columnFamily)
            throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "namenode.c.t3-project.internal");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName table = TableName.valueOf(tableName);

        if (admin.tableExists(table)) {
            System.out.println(tableName + " exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            HColumnDescriptor cf = new HColumnDescriptor(columnFamily);
            cf.setBloomFilterType(BloomType.ROW);
            cf.setCompressionType(Compression.Algorithm.GZ);
            tableDesc.addFamily(cf);
            admin.createTable(tableDesc);
            System.out.println(tableName + " create successfully!");
        }
        admin.close();
        conn.close();
    }

    public static boolean delete(String tableName) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "namenode.c.t3-project.internal");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        Connection conn = ConnectionFactory.createConnection(configuration);

        Admin admin = conn.getAdmin();
        TableName table = TableName.valueOf(tableName);

        if (admin.tableExists(table)) {
            try {
                admin.disableTable(table);
                admin.deleteTable(table);

            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        admin.close();
        conn.close();
        return true;
    }

    public static void get(String tablename, String row) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "namenode.c.t3-project.internal");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        Connection conn = ConnectionFactory.createConnection(configuration);

        Table table = conn.getTable(TableName.valueOf(tablename));

        Get get = new Get(Bytes.toBytes(row));

        Result result = table.get(get);

        System.out.println("Get: " + result);
        table.close();
        conn.close();
    }


    public static void put(String tablename, String row, String columnFamily,
                           String column, String data) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "namenode.c.t3-project.internal");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Table table = conn.getTable(TableName.valueOf(tablename));

        Put put = new Put(Bytes.toBytes(row));

        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(data));

        table.put(put);

        System.out.println("put '" + row + "', '" + columnFamily + ":" + column
                + "', '" + data + "'");

        table.close();
        conn.close();

    }


    public static void scan(String tableName) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "namenode.c.t3-project.internal");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            System.out.println("Scan: " + r);

        }
        table.close();
        conn.close();
    }


    /* advanced api */
    public static void scanfilter(String tableName) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "namenode.c.t3-project.internal");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan s = new Scan();
        s.setStartRow(Bytes.toBytes("row1"));
        s.setStopRow(Bytes.toBytes("row3"));
        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            System.out.println("Scan filter: " + r);

        }
        table.close();
        conn.close();
    }


    public static void scanrowprefixfilter(String tableName) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "namenode.c.t3-project.internal");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan s = new Scan();
        s.setRowPrefixFilter(Bytes.toBytes("row4"));
        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            System.out.println("Scan row prefix filter: " + r);

        }
        table.close();
        conn.close();
    }

    public static void scanregexstringcomparator(String tableName, String columnFamily) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "namenode.c.t3-project.internal");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("zookeeper.znode.parent", "/hbase");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Table table = conn.getTable(TableName.valueOf(tableName));

        Scan s = new Scan();
        RegexStringComparator comparator = new RegexStringComparator("notonlydata.");
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes("column"), CompareFilter.CompareOp.EQUAL, comparator);
        s.setFilter(filter);

        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            System.out.println("Scan regex string comparator: " + r);

        }
        table.close();
        conn.close();
    }


}
