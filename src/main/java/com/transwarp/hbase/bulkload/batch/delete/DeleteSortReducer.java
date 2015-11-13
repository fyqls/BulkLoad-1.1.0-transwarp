package com.transwarp.hbase.bulkload.batch.delete;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class DeleteSortReducer extends
    Reducer<ImmutableBytesWritable, Delete, ImmutableBytesWritable, KeyValue> {
  
  @Override
  protected void reduce(
      ImmutableBytesWritable row,
      java.lang.Iterable<Delete> deletes,
      Reducer<ImmutableBytesWritable, Delete,
              ImmutableBytesWritable, KeyValue>.Context context)
      throws java.io.IOException, InterruptedException
  {
    // although reduce() is called per-row, handle pathological case
    long threshold = context.getConfiguration().getLong(
        "putsortreducer.row.threshold", 2L * (1<<30));
    Iterator<Delete> iter = deletes.iterator();
    while (iter.hasNext()) {
      TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
      long curSize = 0;
      // stop at the end or the RAM threshold
      while (iter.hasNext() && curSize < threshold) {
        Delete p = iter.next();
        for (List<KeyValue> kvs : p.getFamilyMap().values()) {
          for (KeyValue kv : kvs) {
            map.add(kv);
            curSize += kv.getLength();
          }
        }
      }
      context.setStatus("Read " + map.size() + " entries of " + map.getClass()
          + "(" + StringUtils.humanReadableInt(curSize) + ")");
      int index = 0;
      for (KeyValue kv : map) {
        context.write(row, kv);
        if (index > 0 && index % 100 == 0)
          context.setStatus("Wrote " + index);
      }

      // if we have more entries to process
      if (iter.hasNext()) {
        // force flush because we cannot guarantee intra-row sorted order
        context.write(null, null);
      }
    }
  }
}

