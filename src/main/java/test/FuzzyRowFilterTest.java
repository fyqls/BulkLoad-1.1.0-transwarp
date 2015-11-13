package test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FusionRowFilter;
import org.apache.hadoop.hbase.filter.FusionRowFilter.BytesRange;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FuzzyRowFilterTest {
  
  
  public static void main(String[] args) {
    for (int i = 0; i < 100; i++) {
      int x = Math.max((int) (0.3 * i), 1);
      System.out.println(x);
      
    }
    
    
  }
  
  public static void main1(String[] args) throws IOException {
    byte[] tableName = Bytes.toBytes("tr_trans_info");
    HTableInterface ht = new HTable(HBaseConfiguration.create(), tableName);
    Scan scan = new Scan();
    List<BytesRange> ranges = new ArrayList<FusionRowFilter.BytesRange>();
    BytesRange range1 = new BytesRange(0, 18, Bytes.toBytes("2013-01-20 00:00:0"), Bytes.toBytes("2013-01-22 00:00:0"));
    ranges.add(range1);
    BytesRange range2 = new BytesRange(20, 64, "00019862006D08ABDEA4D6DA95B8E39716166428D9D09BC362F866A4FCAD0A0B".getBytes(), "00019862006D08ABDEA4D6DA95B8E39716166428D9D09BC362F866A4FCAD0A0B".getBytes());
    ranges.add(range2);
    FusionRowFilter frf = new FusionRowFilter(ranges);
    scan.setFilter(frf);
    scan.setStartRow(Bytes.toBytes("2013-01-19 00:00:0"));
    scan.setStopRow(Bytes.toBytes("2013-03-30 00:00:0"));
    ResultScanner rs = ht.getScanner(scan);
    Iterator<Result> ite = rs.iterator();
    while (ite.hasNext()) {
      System.out.println(ite.next());
    }
    rs.close();
    ht.close();
  }

}


