package com.gfk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Step5 {

  public static void main(String... args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("gfk");
    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(tableName)) {
      run(table);
    }
  }

  private static void run(Table table) throws IOException {
    // 1. Increment our "stats:reads" counter in row "1" by one
    // 2. Increment the non-existing counter "stats:writes" in row "1" by 1 using a different API
    // 3. Increment the counter "stats:writes" in row "1" by 11
    // 4. Get the counter value for the "stats:reads" column
    // 5. Get the counter value for the "stats:writes" column
  }

  public static void printResult(CellScannable result) throws IOException {
    CellScanner scanner = result.cellScanner();
    while (scanner.advance()) {
      Cell cell = scanner.current();
      System.out.println("Cell: " + cell + ", Value: " + Bytes.toString(cell.getValueArray(),
                                                                        cell.getValueOffset(),
                                                                        cell.getValueLength()));
    }
  }

}
