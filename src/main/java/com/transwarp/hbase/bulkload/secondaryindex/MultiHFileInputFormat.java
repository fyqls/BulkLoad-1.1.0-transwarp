package com.transwarp.hbase.bulkload.secondaryindex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HDFSBlocksDistribution.HostAndWeight;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoderImpl;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.KeyValueHeap;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


import com.google.common.collect.MinMaxPriorityQueue;
import com.transwarp.hbase.bulkload.common.BConstants;

public class MultiHFileInputFormat extends
		InputFormat<ImmutableBytesWritable, Result> {
	
	static final Log LOG = LogFactory.getLog(MultiHFileInputFormat.class);

	public class StoreScanner implements KeyValueScanner,InternalScanner{

	    private KeyValueHeap storeHeap;
	    private  List<byte[]> columns;
	    private int index;
	    private byte [] column;
	    private boolean isWildCard=false;
	    private byte [] columnBuffer = null;
	    private int columnOffset = 0;
	    private int columnLength = 0;
	    
	    public StoreScanner(KeyValueHeap storeHeap, List<byte[]> columns){
	      this.storeHeap=storeHeap;
	      this.columns=columns;
	      if(columns.size()>0){
	        this.reset();
	      }else{
	        this.isWildCard=true;
	      }
	    }
	    
	    public void reset() {
	      if(this.isWildCard){
	        this.columnBuffer=null;
	      }else{
	        this.index=0;
	        this.column=this.columns.get(this.index);
	      }

	    }
	    
	    @Override
	    public KeyValue peek() {
	      // TODO Auto-generated method stub
	      return this.storeHeap.peek();
	    }

	    @Override
	    public KeyValue next() throws IOException {
	      // TODO Auto-generated method stub
	      KeyValue kvReturn=this.storeHeap.next();
	      return kvReturn;
	    }

	    @Override
	    public boolean seek(KeyValue key) throws IOException {
	      // TODO Auto-generated method stub
	      return this.seek(key);
	    }

	    @Override
	    public boolean reseek(KeyValue key) throws IOException {
	      // TODO Auto-generated method stub
	      return this.reseek(key);
	    }

	    @Override
	    public long getSequenceID() {
	      // TODO Auto-generated method stub
	      return 0;
	    }

	    @Override
	    public void close() {
	      // TODO Auto-generated method stub
	      this.storeHeap.close();
	    }

	    @Override
	    public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom)
	        throws IOException {
	      // TODO Auto-generated method stub
	      return this.storeHeap.requestSeek(kv, forward, useBloom);
	    }

	    @Override
	    public boolean realSeekDone() {
	      // TODO Auto-generated method stub
	      return this.storeHeap.realSeekDone();
	    }

	    @Override
	    public void enforceSeek() throws IOException {
	      // TODO Auto-generated method stub
	      this.storeHeap.enforceSeek();
	    }

	    @Override
	    public boolean isFileScanner() {
	      // TODO Auto-generated method stub
	      return this.storeHeap.isFileScanner();
	    }

	    @Override
	    public boolean next(List<Cell> results) throws IOException {
	      // TODO Auto-generated method stub
	      return this.next(results, -1);
	    }
	    
	    private void resetBuffer(byte[] bytes, int offset, int length) {
	      columnBuffer = bytes;
	      columnOffset = offset;
	      columnLength = length;
	    }
	    
	    private boolean checkWildCardColumn(KeyValue kv){
	      if (columnBuffer == null) {
	        // first iteration.
	        resetBuffer(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
	      }
	      int cmp = Bytes.compareTo(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength(),
	          columnBuffer, columnOffset, columnLength);
	      if(cmp==0){
	        return false;
	      }
	      if(cmp > 0){
	        resetBuffer(kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
	        return true;
	      }
	      return true;
	    }
	    
	    private boolean checkExplicitColumn(KeyValue kv){
	      int ret = Bytes.compareTo(this.column, 0,
	          column.length, kv.getBuffer(), kv.getQualifierOffset(), kv.getQualifierLength());
	      if(ret==0){
	        this.index++;
	        return true;
	      }else if(ret>0){
	        return false;
	      }else{
	        if(this.index+1<this.columns.size()){
	          this.index++;
	          this.column=this.columns.get(this.index);
	          return this.checkExplicitColumn(kv);
	        }else{
	          return false;
	        }
	      }
	    }

	    private boolean checkColumn(KeyValue kv){
	      if(this.isWildCard){
	        return this.checkWildCardColumn(kv);
	      }else{
	        return this.checkExplicitColumn(kv);
	      }
	    }
	    
	    @Override
	    public boolean next(List<Cell> result, int limit) throws IOException {
	      // TODO Auto-generated method stub
	      boolean flag=false;
	      KeyValue kv = this.storeHeap.peek();
	      if(kv==null){
	        return flag;
	      }
	      KeyValue nextKV = null;
	      this.reset();
	      do{
	        KeyValue kvReturn=this.storeHeap.next();
	        if(kvReturn!=null && this.checkColumn(kvReturn)){
	          result.add(kvReturn);
	          flag=true;
	        }
	        nextKV = this.storeHeap.peek();
	        if(nextKV==null){
	          break;
	        }
	      }while(KeyValue.COMPARATOR.compareRows(kv, nextKV)==0);
	      return flag;
	    }

	    @Override
	    public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns,
	        long oldestUnexpiredTS) {
	      // TODO Auto-generated method stub
	      return false;
		}

			public boolean next(List<KeyValue> arg0, String arg1) throws IOException {
				// TODO Auto-generated method stub
				return false;
			}

			public boolean next(List<KeyValue> arg0, int arg1, String arg2)
					throws IOException {
				// TODO Auto-generated method stub
				return false;
			}

      @Override
      public boolean backwardSeek(KeyValue key) throws IOException {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean seekToPreviousRow(KeyValue key) throws IOException {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean seekToLastRow() throws IOException {
        // TODO Auto-generated method stub
        return false;
      }

	}

	/**
	 * comparator used to sort hosts based on weight
	 */
	public static class WeightComparator implements Comparator<HostAndWeight> {
		@Override
		public int compare(HostAndWeight l, HostAndWeight r) {
			if (l.getWeight() == r.getWeight()) {
				return l.getHost().compareTo(r.getHost());
			}
			return l.getWeight() < r.getWeight() ? 1 : -1;
		}
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {

		String hbaseTableName = context.getConfiguration().get(BConstants.BulkLoadProps.TABLE_NAME.getName());
		String indexColStr = context.getConfiguration().get(BConstants.BulkLoadProps.INDEX_COLUMNS.getName());
		String[] columns = indexColStr.split(",");
		HashMap<String, String> fetchedColumns = new HashMap<String, String>();
		for(String col : columns){
			String[] colParts = col.split(":",3);
			String tempFamily = colParts[0];
			fetchedColumns.put(tempFamily,null);
		}
		JobConf jobConf = new JobConf(context.getConfiguration());
		HTable table = new HTable(HBaseConfiguration.create(),
				Bytes.toBytes(hbaseTableName));
		Scan scan = new Scan();
		String startRow = context.getConfiguration().get(BConstants.BulkLoadProps.START_ROW.getName());
		String stopRow = context.getConfiguration().get(BConstants.BulkLoadProps.STOP_ROW.getName());
		if(startRow != null && !startRow.isEmpty()){
			scan.setStartRow(Bytes.toBytes(startRow));
		}
		if(stopRow != null && !stopRow.isEmpty()){
			scan.setStopRow(Bytes.toBytes(stopRow));
		}
		List<HRegionLocation> regionList = table.getRegionsInRange(
				scan.getStartRow(), scan.getStopRow());
		HTableDescriptor tableDesc = table.getTableDescriptor();
		Map<byte[], NavigableSet<byte[]>> familyMap = null;

		HColumnDescriptor[] familyDesc = tableDesc.getColumnFamilies();
		familyMap = new TreeMap<byte[], NavigableSet<byte[]>>(
				Bytes.BYTES_COMPARATOR);
		for (int i = 0; i < familyDesc.length; i++) {
			if (fetchedColumns.containsKey(Bytes.toString(familyDesc[i].getName()))) {
				familyMap.put(familyDesc[i].getName(), null);
			}
		}

		List<InputSplit> splits = new ArrayList<InputSplit>();

		for (HRegionLocation regionLoc : regionList) {
			HRegionInfo regionInfo = regionLoc.getRegionInfo();
			Path hbDir = new Path(table.getConfiguration().get(
					HConstants.HBASE_DIR));
			Path tableDir = HTableDescriptor.getTableDir(hbDir,
					table.getTableName());
			HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
			Map<byte[], byte[]> familyDirs = new TreeMap<byte[], byte[]>(
					Bytes.BYTES_COMPARATOR);
			for (byte[] family : familyMap.keySet().toArray(new byte[0][0])) {
				Path homedir = new Path(tableDir, new Path(
						regionInfo.getEncodedName(), new Path(
								Bytes.toString(family))));
				familyDirs.put(family, Bytes.toBytes(homedir.toString()));
				LOG.info("file path=" + Bytes.toString(family) + ","
						+ homedir.toString());
				FileStatus files[] = FSUtils.listStatus(
						FileSystem.get(jobConf), homedir, null);
				if (files == null || files.length == 0) {
					continue;
				}
				FileSystem fs = FileSystem.get(jobConf);
				for (int i = 0; i < files.length; i++) {
					// Skip directories.
					if (files[i].isDir()) {
						continue;
					}
					FileStatus file = files[i];
					HDFSBlocksDistribution storeFileBlocksDistribution = FSUtils
							.computeHDFSBlocksDistribution(fs, file, 0,
									file.getLen());
					hdfsBlocksDistribution.add(storeFileBlocksDistribution);
				}
			}
			int topCount = 3;
			MinMaxPriorityQueue<HostAndWeight> queue = MinMaxPriorityQueue
					.orderedBy(new WeightComparator())
					.maximumSize(topCount).create();
			queue.addAll(hdfsBlocksDistribution.getHostAndWeights().values());
			List<String> topHosts = new ArrayList<String>();
			HostAndWeight elem = queue.pollFirst();
			while (elem != null) {
				topHosts.add(elem.getHost());
				elem = queue.pollFirst();
			}
			LOG.info("topHosts=" + topHosts);
			InputSplit split = new MultiHFileSplit(table.getTableName(), scan.getStartRow(),
					scan.getStopRow(), regionLoc.getHostname(),
					familyDirs, (String[]) topHosts.toArray(new String[0]),
					familyMap);
			splits.add(split);
		}

		return splits;
	}
	
	public static class ColumnMapping {

	    ColumnMapping() {
	      binaryStorage = new ArrayList<Boolean>(2);
	    }

	    String familyName;
	    String qualifierName;
	    byte [] familyNameBytes;
	    byte [] qualifierNameBytes;
	    List<Boolean> binaryStorage;
	    boolean hbaseRowKey;
	    String mappingSpec;
	  }
	
	private class MultiHFileRecordReader extends RecordReader<ImmutableBytesWritable, Result> {

	    private KeyValueHeap storeHeap;
	    List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
	    List<StoreFile.Reader> readers = new ArrayList<StoreFile.Reader>();
	    private byte [] stopRow;
	    private int isScan;
	    // For multiVersionNext() method store values as a buffer.
	    private List<KeyValue> values = new ArrayList<KeyValue>();
	    private ImmutableBytesWritable currentKey = null;
	    private Result currentValue = null;
	    
	    public MultiHFileRecordReader(MultiHFileSplit split, Configuration conf)
	        throws IOException {
	    	if (split.getEndRow() == null) {
	    		this.stopRow = HConstants.EMPTY_END_ROW;
//	      if (Bytes.equals(split.getEndRow(), HConstants.EMPTY_END_ROW)) {
//	        this.stopRow = null;
	      } else {
	        this.stopRow = split.getEndRow();
	      }

	      Scan scan = new Scan(split.getStartRow(), this.stopRow);

	      HTable hTable=new HTable(HBaseConfiguration.create(), split.getTableName());
	      HTableDescriptor tableDesc = hTable.getTableDescriptor();
	      
	      for(byte[] family:split.getFamilyDirs().keySet()){
//	      	if (!addAll && !columnFamilySet.contains(Bytes.toString(family))) {
//	      		LOG.info("Jump family " + Bytes.toString(family));
//	      		continue;
//	      	}
	        LOG.info(Bytes.toString(family)+"="+Bytes.toString(split.getFamilyDirs().get(family)));
	        Path homedir=new Path(Bytes.toString(split.getFamilyDirs().get(family)));
	        FileStatus files[] = FSUtils.listStatus(FileSystem.get(conf), homedir, null);
	        if(files==null || files.length==0){
	          continue;
	        }
	        FileSystem fs=FileSystem.get(conf);
	        List<byte[]> columns =new ArrayList<byte[]>();
	        for(byte[] column:split.getFamilyMap().get(family)){
	          columns.add(column);
	        }
	           
	        HColumnDescriptor columnDesc=tableDesc.getFamily(family);
	        HFileDataBlockEncoder dataBlockEncoder =
	          new HFileDataBlockEncoderImpl(columnDesc.getDataBlockEncoding());
	        KeyValue startKey = KeyValue.createFirstDeleteFamilyOnRow(split.getStartRow(),family);
	        List<KeyValueScanner> familyScanners = new ArrayList<KeyValueScanner>();
	        for (int i = 0; i < files.length; i++) {
	          // Skip directories.
	          if (files[i].isDir()) {
	            continue;
	          }

	          conf.setFloat("hfile.block.cache.size", 0);
	          /*
	          StoreFile.Reader reader =
	              new StoreFile(fs, files[i].getPath(), conf, new CacheConfig(conf), columnDesc.getBloomFilterType(),
	                  dataBlockEncoder).createReader();
	                  */
	          StoreFile.Reader reader =
                new StoreFile(fs, files[i].getPath(), conf, new CacheConfig(conf),
                    columnDesc.getBloomFilterType()).createReader();
	          reader.loadFileInfo();
	          reader.loadBloomfilter();
	          this.readers.add(reader);
	          StoreFileScanner scanner = reader.getStoreFileScanner(false, false);
	          if (scanner.shouldUseScanner(scan, split.getFamilyMap().get(family),  Long.MIN_VALUE)) {
	            scanner.requestSeek(startKey, false, true);
	            familyScanners.add(scanner);
	          }
	          
	        }
//	          StoreScanner storeScanner=new StoreScanner(new KeyValueHeap(familyScanners, KeyValue.COMPARATOR),
//	              columns);
	       StoreScanner storeScanner =  new StoreScanner(new KeyValueHeap(familyScanners, KeyValue.COMPARATOR),columns);
	          scanners.add(storeScanner);
	      }
	      if (Bytes.equals(split.getEndRow(), HConstants.EMPTY_END_ROW)) {
	      	this.stopRow = null;
	      } 
	      this.storeHeap = new KeyValueHeap(scanners, KeyValue.COMPARATOR);
	    }   

	    public boolean multiVersionNext(ImmutableBytesWritable rowKey, Result result) throws IOException{
	      values.clear();
	      KeyValue kv = this.storeHeap.peek();  
	      if(kv==null){
	        return false;
	      }
	      if (isStopRow(kv)) {
	        return false;
	      }
	      
	      KeyValue nextKV = null;
	      do{
	        values.add(this.storeHeap.next());
	        nextKV = this.storeHeap.peek();
	        if(nextKV==null){
	          break;
	        }    
	      }while(KeyValue.COMPARATOR.compareRows(kv, nextKV)==0);
	      if(values.size()==0){
	        return false;
	      }
	      rowKey.set(kv.getBuffer(),kv.getKeyOffset(),kv.getKeyLength());
	      Result value=new Result(values); 
	      result.copyFrom(value);
	      // Writables.copyWritable(value, result);
	      return true;
	    }
	    
	    private boolean isStopRow(KeyValue kv) {
	        return kv == null ||
	            (stopRow != null &&
	                KeyValue.COMPARATOR.compareRows(stopRow, 0, stopRow.length,
	                kv.getBuffer(), kv.getRowOffset(), kv.getRowLength()) <= isScan);
	      }
	  

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		}



		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return this.multiVersionNext(getCurrentKey(), getCurrentValue());
		}



		@Override
		public ImmutableBytesWritable getCurrentKey() throws IOException,
				InterruptedException {
			if(currentKey == null){
				currentKey = new ImmutableBytesWritable();
			}
			return currentKey;
		}



		@Override
		public Result getCurrentValue() throws IOException,
				InterruptedException {
			if(currentValue == null){
				currentValue = new Result();
			}
			return currentValue;
		}



		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}



		@Override
		public void close() throws IOException {
			for (int i = 0; i < this.readers.size(); i++) {
				this.readers.get(i).close(false);
			}
			this.storeHeap.close();

		}
	    
	  } 

	@Override
	public RecordReader<ImmutableBytesWritable, Result> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		MultiHFileSplit fileSplit = (MultiHFileSplit) split;
	    return new MultiHFileRecordReader(fileSplit,new JobConf(context.getConfiguration()));
	}

}
