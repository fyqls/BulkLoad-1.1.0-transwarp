package com.transwarp.hbase.bulkload.combine.rr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public abstract class RRCombineFileInputFormat<K, V> extends
		RRFileInputFormat<K, V> {

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<List<FileStatus>> fileStatusList = getFileStatusList(job);
		List<List<InputSplit>> inputSplitsList = new ArrayList<List<InputSplit>>();
		long minSizeNode = job.getConfiguration().getLong(
				"mapred.min.split.size.per.node", 0);
		for (List<FileStatus> fileStatus : fileStatusList) {
			inputSplitsList.add(createInputSplits(fileStatus, minSizeNode));
		}
		return roundRobinSplits(inputSplitsList);
	}

	private List<InputSplit> createInputSplits(List<FileStatus> fileStatusList,
			long minSplitSize) {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		int start = 0;
		int offset = 0;
		long length = 0;
		for (int i = 0; i < fileStatusList.size(); i++) {
			length += fileStatusList.get(i).getLen();
			if (length > minSplitSize) {
				addCreatedSplit(splits, fileStatusList, start, offset);
				start = start + offset + 1;
				offset = 0;
				length = 0;
			} else {
				offset++;
			}
		}
		if (length != 0) {
			addCreatedSplit(splits, fileStatusList, start, offset - 1);
		}
		return splits;
	}

	private void addCreatedSplit(List<InputSplit> splits,
			List<FileStatus> fileStatusList, int start, int offset) {
		Path[] fl = new Path[offset + 1];
		long[] lengths = new long[offset + 1];
		for (int i = 0; i <= offset; i++) {
			fl[i] = fileStatusList.get(start + i).getPath();
			lengths[i] = fileStatusList.get(start + i).getLen();
		}
		// add this split to the list that is returned
		long[] startoffset = new long[fl.length];
                for (int i = 0; i < startoffset.length; i++) {
    	          startoffset[i] = 0;
                }
                CombineFileSplit thissplit = new CombineFileSplit(fl, startoffset, lengths, new String[0]);

		//CombineFileSplit thissplit = new CombineFileSplit(fl, lengths);
		splits.add(thissplit);
	}
}
