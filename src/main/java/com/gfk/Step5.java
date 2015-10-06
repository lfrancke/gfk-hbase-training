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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
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
    byte[] rowOneBytes = Bytes.toBytes("1");
    byte[] statsBytes = Bytes.toBytes("stats");
    byte[] readsBytes = Bytes.toBytes("reads");
    byte[] writesBytes = Bytes.toBytes("writes");

    // 1. Increment our "stats:reads" counter in row "1" by one
    System.out.println("Incrementing stats:reads...");
    Increment increment = new Increment(rowOneBytes);
    increment.addColumn(statsBytes, readsBytes, 1);
    try {
      table.increment(increment);
    } catch (IOException e) {
      System.out.println("Caught expected IO exception");
      e.printStackTrace();
    }

    // 2. Increment the non-existing counter "stats:writes" in row "1" by 1 using a different API
    System.out.println("Incrementing stats:writes...");
    long counter = table.incrementColumnValue(rowOneBytes, statsBytes, writesBytes, 1);
    System.out.println(counter);

    // 3. Increment the counter "stats:writes" in row "1" by 11
    System.out.println("Incrementing stats:writes by 11...");
    counter = table.incrementColumnValue(rowOneBytes, statsBytes, writesBytes, 11);
    System.out.println(counter);

    // 4. Get the counter value for the "stats:reads" column
    // 5. Get the counter value for the "stats:writes" column
    System.out.println("Getting counter value for stats:reads and stats:writes...");
    Get get = new Get(rowOneBytes);
    get.addColumn(statsBytes, readsBytes);
    get.addColumn(statsBytes, writesBytes);
    Result result = table.get(get);
    printResult(result);
    System.out.println(Bytes.toLong(result.getValue(statsBytes, writesBytes)));
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
