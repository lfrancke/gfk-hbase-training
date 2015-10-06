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

public class Step4 {

  public static void main(String... args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("gfk");
    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(tableName)) {
      run(table);
    }
  }

  private static void run(Table table) throws IOException {
    // 1. Add at least one more row with arbitrary data with row key "2"
    // 2. Scan the table to get all current values and print them
    // 3. Scan again but this time also retrieve at least three older versions of each cell
  }

  public static void printResult(CellScannable result) throws IOException {
    CellScanner scanner = result.cellScanner();
    while (scanner.advance()) {
      Cell cell = scanner.current();
      System.out.println("Cell:" + cell + ", Value: " + Bytes.toString(cell.getValueArray(),
                                                                       cell.getValueOffset(),
                                                                       cell.getValueLength()));
    }
  }

}
