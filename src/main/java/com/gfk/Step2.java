package com.gfk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Step2 {

  public static void main(String... args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("gfk");
    try (Connection connection = ConnectionFactory.createConnection(conf);
         Admin admin = connection.getAdmin()) {
      run(admin, tableName);
    }
  }

  private static void run(Admin admin, TableName tableName) throws IOException {
    // 1. Create a table called “gfk” with a column family “cds”
    System.out.println("Creating table...");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor cfCds = new HColumnDescriptor("cds");
    cfCds.setMaxVersions(3);
    desc.addFamily(cfCds);
    admin.createTable(desc);

    // 2. Describe the table and print the results
    System.out.println("Getting table descriptor...");
    desc = admin.getTableDescriptor(tableName);
    System.out.println(desc);

    // 3. Add a column family “stats”
    System.out.println("Adding a column family...");
    HColumnDescriptor cfStats = new HColumnDescriptor("stats");
    admin.addColumn(tableName, cfStats);

    // 4. Describe the table again and print the results
    System.out.println("Getting table descriptor...");
    desc = admin.getTableDescriptor(tableName);
    System.out.println(desc);
  }

}
