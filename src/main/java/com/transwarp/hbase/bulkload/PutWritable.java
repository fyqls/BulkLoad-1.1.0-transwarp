package com.transwarp.hbase.bulkload;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

public class PutWritable implements Writable {
  protected Map<byte [], List<QualifierWritable>> familyMap =
      new TreeMap<byte [], List<QualifierWritable>>(Bytes.BYTES_COMPARATOR);
  private byte[] rowKey = null;
  private int totalKvs = 0;
  private long timeStamp;
  
  private static final int DEFAULT_ALLOC_SIZE = 20;
  
  private class QualifierWritable implements Writable {
    private byte[] qualifier = null;
    private byte[] value = null;
    
    public QualifierWritable() {
      
    }
    
    public QualifierWritable(byte[] qualifier, byte[] value) {
      this.qualifier = qualifier;
      this.value = value;
    }
    
    public byte[] getQualifier() {
      return qualifier;
    }
    
    public byte[] getValue() {
      return value;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      Bytes.writeByteArray(out, qualifier);
      Bytes.writeByteArray(out, value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.qualifier = Bytes.readByteArray(in);
      this.value = Bytes.readByteArray(in);
    }
    
  }
  
  public PutWritable() {
    
  }
  
  public PutWritable(byte[] rowKey, long timeStamp) {
    this.rowKey = rowKey;
    this.timeStamp = timeStamp;
  }
  
  public void add(byte [] family, byte [] qualifier, byte [] value) {
    List<QualifierWritable> values = getQualifiers(family);
    values.add(new QualifierWritable(qualifier, value));
  }
  
  private List<QualifierWritable> getQualifiers(byte[] family) {
    List<QualifierWritable> values = this.familyMap.get(family);
    if (values == null) {
      values = new ArrayList<QualifierWritable>();
      this.familyMap.put(family, values);
      return values;
    }
    return values;
  }
  
  public List<KeyValue> genKvs() {
    if (rowKey == null) {
      return null;
    }
    List<KeyValue> kvs = new ArrayList<KeyValue>(totalKvs > 0 ? totalKvs : DEFAULT_ALLOC_SIZE);
    for (Map.Entry<byte [], List<QualifierWritable>> e : familyMap.entrySet()) {
      byte[] cfName = e.getKey();
      for (QualifierWritable q : e.getValue()) {
        kvs.add(new KeyValue(rowKey, cfName, q.getQualifier(), 
            timeStamp, KeyValue.Type.Put, q.getValue()));
      }
    }
    return kvs;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, rowKey);
    out.writeLong(this.timeStamp);
    out.writeInt(familyMap.size());
    int totalKvs = 0;
    for (Map.Entry<byte [], List<QualifierWritable>> e : familyMap.entrySet()) {
      Bytes.writeByteArray(out, e.getKey());
      out.writeInt(e.getValue().size());
      for (QualifierWritable q : e.getValue()) {
        q.write(out);
        ++totalKvs;
      }
    }
    out.writeInt(totalKvs);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    reset();
    this.rowKey = Bytes.readByteArray(in);
    this.timeStamp = in.readLong();
    int mapSize = in.readInt();
    for (int i=0; i<mapSize; ++i) {
      byte[] cfname = Bytes.readByteArray(in);
      int size = in.readInt();
      List<QualifierWritable> qualifiers = new ArrayList<QualifierWritable>(size);
      for (int j=0; j<size; ++j) {
        QualifierWritable q = new QualifierWritable();
        q.readFields(in);
        qualifiers.add(q);
      }
      List<QualifierWritable> old = familyMap.put(cfname, qualifiers);
      if (old != null) {
        throw new IOException("find dup cf name " + Bytes.toString(cfname) + " for row " + Bytes.toString(rowKey));
      }
    }
    this.totalKvs = in.readInt();
  }
  
  private void reset() {
    this.rowKey = null;
    this.timeStamp = 0;
    this.familyMap.clear();
    this.totalKvs = 0;
  }

}
