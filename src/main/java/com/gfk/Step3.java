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
    // 1. Check whether row with key "1" exists
    //      Be careful: https://issues.apache.org/jira/browse/HBASE-13779
    //      This is fixed in CDH 5.4.3+

    // 2. Get the row with row key "1"

    // 3. Add a row with the following details
    //      Row key: 1
    //      Column Family: cds, Column: delivery_id, Value: 10000 (as a String)
    //      Column Family: stats, Column: reads, Value: 123 (as a String)

    // 4. Get the row with row key "1" again

    // 5. Add more data to the same row
    //      Column Family: cds, Column: date, Value: 2015-07-21

    // 6. Get the row again

    // 7. Change a value
    //      Column Family: cds, Column: date, Value: 2015-07-22

    // 8. Get two versions of just the date column
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
