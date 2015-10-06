package com.gfk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class Basic {

  public static void main(String... args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("gfk");
    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(tableName);
         Admin admin = connection.getAdmin()) {
      // ... your code here

      System.out.println("Closed? " + connection.isClosed());
    }

  }

}
