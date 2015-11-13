package com.transwarp.hbase.bulkload.combine.remote;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.transwarp.hbase.bulkload.combine.CombineFileLineRecordReader;
import com.transwarp.hbase.bulkload.combine.MultiFileInputWritableComparable;


public class MultiRemoteFileInputFormat extends CombineRemoteFileInputFormat<MultiFileInputWritableComparable, Text>  
{
    public RecordReader<MultiFileInputWritableComparable,Text> createRecordReader(InputSplit split,TaskAttemptContext context) throws IOException 
    {
      return new CombineFileRecordReader<MultiFileInputWritableComparable, Text>
      ((CombineFileSplit)split, context, CombineRemoteFileLineRecordReader.class);
    }

}
