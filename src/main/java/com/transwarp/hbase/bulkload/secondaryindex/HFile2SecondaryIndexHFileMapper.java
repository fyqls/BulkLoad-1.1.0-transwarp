package com.transwarp.hbase.bulkload.secondaryindex;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;

import com.transwarp.hbase.bulkload.common.BConstants;

/**
 * Write table content out to files in hdfs.
 */
public class HFile2SecondaryIndexHFileMapper extends
		Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put> {

	private HashMap<String, HashMap<String, String>> fetchedColumns = null;
	private String targetColumnFamily = "f1";
	private static final byte PADDING_BYTE = 0;

	/**
	 * Handles initializing this class with objects specific to it (i.e., the
	 * parser). Common initialization that might be leveraged by a subsclass is
	 * done in <code>doSetup</code>. Hence a subclass may choose to override this
	 * method and call <code>doSetup</code> as well before handling it's own
	 * custom params.
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
		fetchedColumns = new LinkedHashMap<String, HashMap<String, String>>();
		String indexColStr = context.getConfiguration().get(
				BConstants.BulkLoadProps.INDEX_COLUMNS.getName());
		targetColumnFamily = context.getConfiguration().get(
				BConstants.BulkLoadProps.TARGET_COLUMN_FAMILY.getName(), "f1");
		String[] columns = indexColStr.split(",");
		for (String col : columns) {
			String[] colParts = col.split(":", 3);
			String tempFamily = colParts[0];
			String tempColumn = colParts[1];
			String targetLength = "0";
			if (colParts.length > 2) {
				targetLength = colParts[2];
			}
			HashMap<String, String> singleColumns = fetchedColumns.get(tempFamily);
			if (singleColumns == null) {
				singleColumns = new LinkedHashMap<String, String>();
				fetchedColumns.put(tempFamily, singleColumns);
			}
			singleColumns.put(tempColumn, targetLength);
		}

	}

	/**
	 * Convert a line of TXT text into an HBase table row.
	 */
	@Override
	public void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException {
		for (String family : fetchedColumns.keySet()) {
			Put put = null;
			ImmutableBytesWritable outputkey = null;
			if (fetchedColumns.get(family).size() > 1) {
				byte[] rowkeyBytes = null;
				int length = 0;
				for (String column : fetchedColumns.get(family).keySet()) {
					byte[] tempColumnValue = (value.getValue(Bytes.toBytes(family),
							Bytes.toBytes(column))) == null ? new byte[0] : value.getValue(
							Bytes.toBytes(family), Bytes.toBytes(column));
					int tempColumnExpectedLength = Integer.valueOf(fetchedColumns.get(
							family).get(column));
					short tempColumnActualLength = (short) tempColumnValue.length;
					Log.info("Actual length is :" + tempColumnActualLength);
					if (tempColumnExpectedLength <= 0) {
						tempColumnExpectedLength = tempColumnActualLength;
					}
					if (tempColumnExpectedLength < tempColumnActualLength) {
						length += tempColumnActualLength + 2;
					} else {
						length += tempColumnExpectedLength + 2;
					}
				}
				rowkeyBytes = new byte[length];
				int pos = 0;
				for (String column : fetchedColumns.get(family).keySet()) {
					byte[] tempColumnValue = (value.getValue(Bytes.toBytes(family),
							Bytes.toBytes(column))) == null ? new byte[0] : value.getValue(
							Bytes.toBytes(family), Bytes.toBytes(column));
					int tempColumnExpectedLength = Integer.valueOf(fetchedColumns.get(
							family).get(column));
					short tempColumnActualLength = (short) tempColumnValue.length;
					if (tempColumnExpectedLength <= 0) {
						tempColumnExpectedLength = tempColumnActualLength;
					}
					if (tempColumnActualLength > 0) {
						if (tempColumnExpectedLength < tempColumnActualLength) {
							Bytes.putBytes(rowkeyBytes, pos, tempColumnValue, 0,
									tempColumnExpectedLength);
						} else {
							Bytes.putBytes(rowkeyBytes, pos, tempColumnValue, 0,
									tempColumnActualLength);
							for (int i = tempColumnActualLength; i < tempColumnExpectedLength; i++) {
								rowkeyBytes[pos + i] = PADDING_BYTE;
							}
						}
						pos += tempColumnExpectedLength;
					}
				}
				for (String column : fetchedColumns.get(family).keySet()) {
					byte[] tempColumnValue = (value.getValue(Bytes.toBytes(family),
							Bytes.toBytes(column))) == null ? new byte[0] : value.getValue(
							Bytes.toBytes(family), Bytes.toBytes(column));
					int tempColumnExpectedLength = Integer.valueOf(fetchedColumns.get(
							family).get(column));
					short tempColumnActualLength = (short) tempColumnValue.length;
					if (tempColumnExpectedLength <= 0) {
						tempColumnExpectedLength = tempColumnActualLength;
					}
					byte[] lengthBytes = Bytes.toBytes(tempColumnActualLength);
					Log.info("Actual lenghth is:" + tempColumnActualLength);
					Bytes.putBytes(rowkeyBytes, pos, lengthBytes, 0, lengthBytes.length);
					pos += 2;
					if (tempColumnActualLength > tempColumnExpectedLength) {
						Bytes.putBytes(rowkeyBytes, pos, tempColumnValue,
								tempColumnExpectedLength,
								(tempColumnActualLength - tempColumnExpectedLength));
						pos += (tempColumnActualLength - tempColumnExpectedLength);
					}
				}
				// If the rowkey is null, continue to do the next
				if (rowkeyBytes == null || rowkeyBytes.length == 0) {
					continue;
				}
				put = new Put(rowkeyBytes);
				put.add(Bytes.toBytes(targetColumnFamily), value.getRow(),
						Bytes.toBytes(""));
				outputkey = new ImmutableBytesWritable(rowkeyBytes);

			} else {
				for (String column : fetchedColumns.get(family).keySet()) {
					byte[] rowBytes = value.getValue(Bytes.toBytes(family),
							Bytes.toBytes(column));
					// Check the value of the row, if it is null, skip it
					if (rowBytes.length == 0) {
						continue;
					}
					// Split the key and get the 
					byte[] valueGetRow = value.getRow();
					
					put = new Put(rowBytes);
					put.add(Bytes.toBytes(targetColumnFamily), valueGetRow,
							Bytes.toBytes(""));
					outputkey = new ImmutableBytesWritable(value.getValue(
							Bytes.toBytes(family), Bytes.toBytes(column)));
				}
			}
			try {
				if (put != null) {
					context.write(outputkey, put);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
}
