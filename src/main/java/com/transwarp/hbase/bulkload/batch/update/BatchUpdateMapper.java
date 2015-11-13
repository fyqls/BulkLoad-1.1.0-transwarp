package com.transwarp.hbase.bulkload.batch.update;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import com.transwarp.hbase.bulkload.common.BConstants;

/**
 * Write table content out to files in hdfs.
 */
public class BatchUpdateMapper extends
		Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put> {

	private HashMap<String, HashMap<String, String>> tobe_updated_columns = null;
	private byte[] tobe_updated_date = null;
	private int date_start_index = 0;
	private int date_column_length = 0;

	/**
	 * Handles initializing this class with objects specific to it (i.e., the
	 * parser). Common initialization that might be leveraged by a subsclass is
	 * done in <code>doSetup</code>. Hence a subclass may choose to override
	 * this method and call <code>doSetup</code> as well before handling it's
	 * own custom params.
	 * 
	 * @param context
	 */
	@Override
	protected void setup(Context context) {
		doSetup(context);
	}

	/**
	 * Handles common parameter initialization that a subclass might want to
	 * leverage.
	 * 
	 * @param context
	 */
	protected void doSetup(Context context) {
	    String allColumnsTobeUpdatedStr = context.getConfiguration().get(BConstants.BulkLoadProps.COLUMNS_TOBE_UPDATED.getName());
	    if(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_TOBE_UPDATED.getName())!=null
	    	&& !context.getConfiguration().get(BConstants.BulkLoadProps.DATE_TOBE_UPDATED.getName()).isEmpty()) { 
	    	tobe_updated_date = Bytes.toBytes(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_TOBE_UPDATED.getName()));
	    }
	    
	    tobe_updated_columns = new HashMap<String, HashMap<String,String>>();
	    if(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_START_INDEX.getName()).trim() != null
	    		&& !context.getConfiguration().get(BConstants.BulkLoadProps.DATE_START_INDEX.getName()).trim().isEmpty()) {
	    	date_start_index = Integer.valueOf(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_START_INDEX.getName()).trim());
	    }
	    if(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_COLUMN_LENGTH.getName()).trim() != null
	    		&& !context.getConfiguration().get(BConstants.BulkLoadProps.DATE_COLUMN_LENGTH.getName()).trim().isEmpty()) {
	    	date_column_length = Integer.valueOf(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_COLUMN_LENGTH.getName()).trim());
	    }
	    
	    String[] columnTobeUpdated = allColumnsTobeUpdatedStr.split(","); 
	    //format is cf:column:newvalue
	    for(String singleColumn : columnTobeUpdated) {
	    	String[] parts = singleColumn.split(":",3);
	    	String columnFamily = parts[0];
	    	String column = parts[1];
	    	String newValue = parts[2];
	    	HashMap<String, String> certainCFColumns = tobe_updated_columns.get(columnFamily);
	    	
	    	if(certainCFColumns == null) {
	    		certainCFColumns = new HashMap<String,String>();
	    		tobe_updated_columns.put(columnFamily, certainCFColumns);
	    	}
	    	
	    	certainCFColumns.put(column, newValue);
	    }
	   
	}

	/**
	 * Convert a line of TXT text into an HBase table row.
	 */
	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException {
					ImmutableBytesWritable outputkey = null;
					byte[] rowKey = value.getRow();
					try {
						if(tobe_updated_date != null
								&& Bytes.compareTo(value.getRow(), date_start_index, date_column_length,tobe_updated_date, 0, date_column_length) != 0) {
							return ;
						}
						outputkey = new ImmutableBytesWritable(value.getRow());
						for(String cf : tobe_updated_columns.keySet()) {
							HashMap<String, String> columns = tobe_updated_columns.get(cf);
							for(String column : columns.keySet()) {
								Put put = new Put(rowKey);
								put.add(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(columns.get(column)));
								context.write(outputkey, put);
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
	}
}
