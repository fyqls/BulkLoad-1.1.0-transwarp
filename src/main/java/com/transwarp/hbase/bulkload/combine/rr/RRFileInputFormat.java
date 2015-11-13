package com.transwarp.hbase.bulkload.combine.rr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public abstract class RRFileInputFormat<K, V> extends FileInputFormat<K, V> {

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<List<FileStatus>> fileStatusList = getFileStatusList(job);
    List<List<InputSplit>> inputSplitsList = new ArrayList<List<InputSplit>>();
    for (List<FileStatus> fileStatus : fileStatusList) {
      List<InputSplit> splits = new ArrayList<InputSplit>();
      for (FileStatus status: fileStatus) {
        splits.add(new FileSplit(status.getPath(), 0, status.getLen(), new String[0]));
      }
    }
    // TODO Auto-generated method stub
    return roundRobinSplits(inputSplitsList);
  }

  protected List<List<FileStatus>> getFileStatusList(JobContext job) throws IOException {
    List<List<FileStatus>> filesList = new ArrayList<List<FileStatus>>();
    Path[] dirs = getInputPaths(job);
    for(Path dir : dirs) {
      filesList.add(listStatus(dir));
    }
    return filesList;
  }

  protected List<FileStatus> listStatus(Path dir) throws IOException {
    JobConf tempConf = new JobConf();
    tempConf.set("mapred.input.dir", dir.toString());
    JobContext temp = new Job(tempConf);
    return listStatus(temp);
  }
  
  protected List<InputSplit> roundRobinSplits(List<List<InputSplit>> inputSplitsList) {
    int max = 0;
    for (List<InputSplit> splits: inputSplitsList) {
      if (splits.size() > max) {
        max = splits.size();
      }
    }
    List<InputSplit> result = new ArrayList<InputSplit>();
    
    for (int count = 0;count < max; count++) {
      for(List<InputSplit> splits : inputSplitsList) {
        if(splits.size() > count) {
          result.add(splits.get(count));
        }
      }
    }
    return result;
  }
}
