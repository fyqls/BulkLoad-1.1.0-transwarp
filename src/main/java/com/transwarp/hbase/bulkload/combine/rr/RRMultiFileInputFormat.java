package com.transwarp.hbase.bulkload.combine.rr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.transwarp.hbase.bulkload.combine.MultiFileInputWritableComparable;
import com.transwarp.hbase.bulkload.combine.remote.CombineRemoteFileLineRecordReader;

public class RRMultiFileInputFormat extends RRCombineFileInputFormat<MultiFileInputWritableComparable, Text> {

  @Override
  public RecordReader<MultiFileInputWritableComparable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new CombineFileRecordReader<MultiFileInputWritableComparable, Text>
    ((CombineFileSplit)split, context, CombineRemoteFileLineRecordReader.class);
  }
  
}
