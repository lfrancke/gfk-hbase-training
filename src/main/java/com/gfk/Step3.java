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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Step3 {

  public static void main(String... args) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("gfk");
    try (Connection connection = ConnectionFactory.createConnection(conf);
         Table table = connection.getTable(tableName)) {
      run(table);
    }
  }

  private static void run(Table table) throws IOException {
    byte[] rowKey = Bytes.toBytes("1");
    byte[] cfCds = Bytes.toBytes("cds");
    byte[] dateBytes = Bytes.toBytes("date");

    // 1. Check whether row with key "1" exists
    //      Be careful: https://issues.apache.org/jira/browse/HBASE-13779
    //      This is fixed in CDH 5.4.3+
    System.out.println("Checking for row...");
    Get get = new Get(rowKey);
    System.out.println(table.exists(get));

    // 2. Get the row with row key "1"
    System.out.println("Getting row...");
    get = new Get(rowKey); //to work around https://issues.apache.org/jira/browse/HBASE-13779
    Result result = table.get(get);
    printResult(result);

    // 3. Add a row with the following details
    //      Row key: 1
    //      Column Family: cds, Column: delivery_id, Value: 10000 (as a String)
    //      Column Family: stats, Column: reads, Value: 123 (as a String)
    System.out.println("Putting data...");
    Put put = new Put(rowKey);
    put.addColumn(cfCds, Bytes.toBytes("delivery_id"), Bytes.toBytes("10000"));
    put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("reads"), Bytes.toBytes("123"));
    table.put(put);

    // 4. Get the row with row key "1" again
    System.out.println("Getting row...");
    result = table.get(get);
    printResult(result);

    // 5. Add more data to the same row
    //      Column Family: cds, Column: date, Value: 2015-07-21
    System.out.println("Putting data...");
    put = new Put(rowKey);
    put.addColumn(cfCds, dateBytes, Bytes.toBytes("2015-07-21"));
    table.put(put);

    // 6. Get the row again
    System.out.println("Getting row...");
    result = table.get(get);
    printResult(result);

    // 7. Change a value
    //      Column Family: cds, Column: date, Value: 2015-07-22
    System.out.println("Putting data...");
    put = new Put(rowKey);
    put.addColumn(cfCds, dateBytes, Bytes.toBytes("2015-07-22"));
    table.put(put);

    // 8. Get two versions of just the date column
    System.out.println("Getting two versions...");
    get.setMaxVersions(2);
    get.addColumn(cfCds, dateBytes);
    result = table.get(get);
    printResult(result);
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
