package com.transwarp.hbase.bulkload.secondaryindex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;

public class MultiHFileSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit{

  static final Log LOG = LogFactory.getLog(MultiHFileSplit.class);

  private byte [] tableName;
  private byte [] startRow;
  private byte [] endRow;
  private String regionLocation;
  private String[] hosts;
  private Map<byte [], byte []> familyDirs;
  private Map<byte [], NavigableSet<byte []>> familyMap = null;
  
  public MultiHFileSplit()
  {
	  
  }
  
  public MultiHFileSplit( byte[] tableName, byte[] startRow,
      byte[] endRow, String hostname, Map<byte[], byte[]> familyDirs,
      String[] hosts, Map<byte[], NavigableSet<byte[]>> familyMap) {
    this.tableName = tableName;
    this.startRow = startRow;
    this.endRow = endRow;
    this.regionLocation = hostname;
    this.familyDirs=familyDirs;
    this.hosts=hosts;
    this.familyMap=familyMap;
  }
  
  public boolean isGetScan() {
    return this.startRow != null && this.startRow.length > 0 &&
      Bytes.equals(this.startRow, this.endRow);
  }

  

  public Map<byte[], byte[]> getFamilyDirs() {
    return familyDirs;
  }

  public void setFamilyDirs(Map<byte[], byte[]> familyDirs) {
    this.familyDirs = familyDirs;
  }

  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    return familyMap;
  }

  public void setFamilyMap(Map<byte[], NavigableSet<byte[]>> familyMap) {
    this.familyMap = familyMap;
  }

  public void write(DataOutput out) throws IOException {
    // TODO Auto-generated method stub
    Bytes.writeByteArray(out, tableName);
    Bytes.writeByteArray(out, startRow);
    Bytes.writeByteArray(out, endRow);
    Bytes.writeByteArray(out, Bytes.toBytes(regionLocation));
    out.writeInt(familyMap.size());
    for(Map.Entry<byte [], NavigableSet<byte []>> entry : familyMap.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      NavigableSet<byte []> columnSet = entry.getValue();
      if(columnSet != null){
        out.writeInt(columnSet.size());
        for(byte [] qualifier : columnSet) {
          Bytes.writeByteArray(out, qualifier);
        }
      } else {
        out.writeInt(0);
      }
    }
    out.writeInt(familyDirs.size());
    for(Map.Entry<byte [], byte []> entry : familyDirs.entrySet()) {
      Bytes.writeByteArray(out, entry.getKey());
      Bytes.writeByteArray(out, entry.getValue());
    }
  }

  public void readFields(DataInput in) throws IOException {
    // TODO Auto-generated method stub
    tableName = Bytes.readByteArray(in);
    startRow = Bytes.readByteArray(in);
    endRow = Bytes.readByteArray(in);
    regionLocation = Bytes.toString(Bytes.readByteArray(in));
    int numFamilies = in.readInt();
    this.familyMap =
      new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
    for(int i=0; i<numFamilies; i++) {
      byte [] family = Bytes.readByteArray(in);
      int numColumns = in.readInt();
      TreeSet<byte []> set = new TreeSet<byte []>(Bytes.BYTES_COMPARATOR);
      for(int j=0; j<numColumns; j++) {
        byte [] qualifier = Bytes.readByteArray(in);
        set.add(qualifier);
      }
      this.familyMap.put(family, set);
    }
    
    int numDirs = in.readInt();
    this.familyDirs =
      new TreeMap<byte [], byte []>(Bytes.BYTES_COMPARATOR);
    for(int i=0; i<numDirs; i++) {
      byte [] family = Bytes.readByteArray(in);
      byte [] dir = Bytes.readByteArray(in);
      this.familyDirs.put(family, dir);
    }
    hosts = null;
  }

  @Override
  public long getLength() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    // TODO Auto-generated method stub
    if (this.hosts == null) {
      return new String[]{};
    } else {
      return this.hosts;
    }
  }

  public byte[] getTableName() {
    return tableName;
  }

  public void setTableName(byte[] tableName) {
    this.tableName = tableName;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public void setStartRow(byte[] startRow) {
    this.startRow = startRow;
  }

  public byte[] getEndRow() {
    return endRow;
  }

  public void setEndRow(byte[] endRow) {
    this.endRow = endRow;
  }
  
}
