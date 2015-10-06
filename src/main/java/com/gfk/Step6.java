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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Step6 {

  public static void main(String... args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("gfk");
    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(tableName)) {
      run(table);
      scanTable(table);
    }
  }

  private static void run(Table table) throws IOException {
    // 1. Delete all versions of the column "stats:reads" in row "1"
    System.out.println("Deleting stats:reads...");
    Delete delete = new Delete(Bytes.toBytes("1"));
    delete.addColumns(Bytes.toBytes("stats"), Bytes.toBytes("reads"));
    table.delete(delete);

    // 2. Delete the whole row with row key "2"
    System.out.println("Deleting row 2...");
    delete = new Delete(Bytes.toBytes("2"));
    table.delete(delete);
  }

  private static void scanTable(Table table) throws IOException {
    Scan scan = new Scan();
    try (ResultScanner scanner = table.getScanner(scan)) {
      for (Result result : scanner) {
        printResult(result);
      }
    }
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
