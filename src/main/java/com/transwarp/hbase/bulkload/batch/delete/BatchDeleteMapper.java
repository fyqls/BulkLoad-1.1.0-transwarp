package com.transwarp.hbase.bulkload.batch.delete;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import com.transwarp.hbase.bulkload.common.BConstants;

/**
 * Write table content out to files in hdfs.
 */
public class BatchDeleteMapper extends
		Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Delete> {

    private int date_start_index = 0;
    private int date_column_length = 0;
    private byte[] tobe_deleted_date = null; 
    private ArrayList<byte[]> allColumnFamilies = new ArrayList<byte[]>();

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
	    String allColumnFamiliesStr = context.getConfiguration().get(BConstants.BulkLoadProps.ALL_COLUMNFAMILIES_TOBE_DELETED.getName());
	    if(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_TOBE_DELETED.getName()) != null
	    		&& !context.getConfiguration().get(BConstants.BulkLoadProps.DATE_TOBE_DELETED.getName()).isEmpty()) {
	    	tobe_deleted_date = Bytes.toBytes(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_TOBE_DELETED.getName()));
	    }
	    if(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_START_INDEX.getName()).trim() != null
	    		&& !context.getConfiguration().get(BConstants.BulkLoadProps.DATE_START_INDEX.getName()).trim().isEmpty()) {
	    	date_start_index = Integer.valueOf(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_START_INDEX.getName()).trim());
	    }
	    if(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_COLUMN_LENGTH.getName()).trim() != null
	    		&& !context.getConfiguration().get(BConstants.BulkLoadProps.DATE_COLUMN_LENGTH.getName()).trim().isEmpty()) {
	    	date_column_length = Integer.valueOf(context.getConfiguration().get(BConstants.BulkLoadProps.DATE_COLUMN_LENGTH.getName()).trim());
	    }
	    
	    if(allColumnFamiliesStr != null) {
		    String[] cfs = allColumnFamiliesStr.split(",");
		    for(String cf : cfs) {
		    	allColumnFamilies.add(Bytes.toBytes(cf));
		    }
	    }
	}

	/**
	 * Convert a line of TXT text into an HBase table row.
	 */
	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException {
					Delete delete = null;
					ImmutableBytesWritable outputkey = null;
					byte[] rowKey = value.getRow();
					try {
						if(tobe_deleted_date != null &&
								Bytes.compareTo(value.getRow(), date_start_index, date_column_length,tobe_deleted_date, 0, date_column_length) != 0) {
							return;
						}
						delete = new Delete(rowKey);
						outputkey = new ImmutableBytesWritable(rowKey);
						for(byte[] cf : allColumnFamilies) {
							delete.deleteFamily(cf);
						}
						context.write(outputkey, delete);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
	}
}
