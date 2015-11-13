/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.transwarp.hbase.bulkload.withindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import com.transwarp.hbase.bulkload.FormatException;
import com.transwarp.hbase.bulkload.HBaseTableSpec;
import com.transwarp.hbase.bulkload.ParsedLine;
import com.transwarp.hbase.bulkload.TextRecord2HBaseRowConverter;
import com.transwarp.hbase.bulkload.TextRecordSpec;
import com.transwarp.hbase.bulkload.common.BConstants.BulkLoadProps;

/**
 * Emits Sorted KeyValues. Reads the text passed, parses it and creates the Key Values then Sorts
 * them and emits Keyalues in sorted order. 
 * @see HFileOutputFormat
 * @see KeyValueSortReducer
 * @see PutWritableSortReducer
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TextWithIndexSortReducer extends
    Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, KeyValue> {

	public TextRecord2HBaseRowConverter converter;

	/** Timestamp for all inserted rows */
	private final long ts = 0;

	/** Should skip bad lines */
	private boolean skipBadLines;

	private Counter badLineCount;

	public long getTs() {
		return ts;
	}

	public boolean getSkipBadLines() {
		return skipBadLines;
	}

	public Counter getBadLineCount() {
		return badLineCount;
	}

	public void incrementBadLineCount(int count) {
		this.badLineCount.increment(count);
	}

	/**
	 * Handles initializing this class with objects specific to it (i.e., the parser).
	 * Common initialization that might be leveraged by a subsclass is done in
	 * <code>doSetup</code>. Hence a subclass may choose to override this method
	 * and call <code>doSetup</code> as well before handling it's own custom params.
	 *
	 * @param context
	 */
	@Override
	protected void setup(Context context) {
		doSetup(context);
	}

	/**
	 * Handles common parameter initialization that a subclass might want to leverage.
	 * @param context
	 */
	protected void doSetup(Context context) {
		badLineCount = context
				.getCounter("ImportTextRecord2HBase", "Bad Lines");
		
		String tableName = context.getConfiguration().get("tableName");
		String splitKeySpec = context.getConfiguration().get("splitKeySpec");
		String textRecordSpec = context.getConfiguration()
				.get("textRecordSpec");
		String fieldDelimiter = context.getConfiguration()
				.get("fieldDelimiter");
		String internalFieldDelimiter = context.getConfiguration().get(
				"internalFieldDelimiter");
		String encoding = context.getConfiguration().get("encoding");
		String rowSpec = context.getConfiguration().get("rowSpec");
		TextRecordSpec recordSpec = null;
		boolean emptyStringAsNull = context.getConfiguration().getBoolean(BulkLoadProps.EMPTY_STRING_AS_NULL.getName(), false);
		boolean useHyperbaseDataType = context.getConfiguration().getBoolean(BulkLoadProps.USE_HYPERBASE_DATATYPE.getName(), false);
		try {
			recordSpec = new TextRecordSpec(textRecordSpec,
					encoding, fieldDelimiter);
			HBaseTableSpec tableSpec = null;
			if (org.apache.commons.lang.StringUtils.isEmpty(internalFieldDelimiter)) {
				tableSpec = new HBaseTableSpec(tableName,
						rowSpec, splitKeySpec, recordSpec);
			} else {
				tableSpec = new HBaseTableSpec(tableName,
						rowSpec, splitKeySpec, recordSpec,
						HBaseTableSpec.EXTERNAL_COLUMN_DELIMITER,
						internalFieldDelimiter);
			}
			tableSpec.setEmptyStringAsNull(emptyStringAsNull);
			tableSpec.setUseHyperbaseDataType(useHyperbaseDataType);

			converter = new TextRecord2HBaseRowConverter(recordSpec, tableSpec);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		/* Initial Line Parser */
		ParsedLine.escapedFieldDelimiter = recordSpec.getEscapedFieldDelimiter();
	}

	byte[] family = Bytes.toBytes("f");
	byte[] qualifier = Bytes.toBytes("q");
	byte[] emptyByte = new byte[0];
	@Override
	protected void reduce(
		ImmutableBytesWritable rowKey,
		java.lang.Iterable<Text> lines,
		Reducer<ImmutableBytesWritable, Text,
		ImmutableBytesWritable, KeyValue>.Context context)
	throws java.io.IOException, InterruptedException
	{
		// although reduce() is called per-row, handle pathological case
		long threshold = context.getConfiguration().getLong(
				"reducer.row.threshold", 1L * (1<<30));
		Iterator<Text> iter = lines.iterator();
		boolean qualifier = context.getConfiguration().getBoolean("indexqualifier", false);
		while (iter.hasNext()) {
		  // Get the prefix to judge whethre primary table(Prefix == 0) or index table (prefix  > 0)
		  int rowkeyPrefix = Bytes.toInt(rowKey.get(), 0, 4);
			byte[] rowKeyWithoutPrefix = Bytes.tail(rowKey.get(), rowKey.get().length - 4);
		  Set<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
			long curSize = 0;
			// stop at the end or the RAM threshold
			while (iter.hasNext() && curSize < threshold) {
				Text line = iter.next();
				String lineStr = line.toString();
				try {
				  Put p = null;
				  if (rowkeyPrefix == 0) {
				    ArrayList<String> parsedLine = ParsedLine.parse(converter.getRecordSpec(),
	              lineStr);
	          
	          p = converter.convert(parsedLine, rowKeyWithoutPrefix);
				  } else {
				    p = new Put(rowKeyWithoutPrefix);
				    if (qualifier) {
				      p.add(family, line.getBytes(), emptyByte);
				    } else {
				      p.add(family, this.qualifier, line.getBytes());
				    }
				  }
					
					if (p != null) {
					  for (List<KeyValue> kvs : p.getFamilyMap().values()) {
	            for (KeyValue kv : kvs) {
	              map.add(kv);
	              curSize += kv.getLength();
	            }
	          }
					}
				} catch (FormatException badLine) {
					if (skipBadLines) {
						System.err.println("Bad line." + badLine.getMessage());
						incrementBadLineCount(1);
						return;
					}
					throw new IOException(badLine);
				} catch (IllegalArgumentException e) {
					if (skipBadLines) {
						System.err.println("Bad line." + e.getMessage());
						incrementBadLineCount(1);
						return;
					}
					throw new IOException(e);
				}
			}
			context.setStatus("Read " + map.size() + " entries of " + map.getClass()
					+ "(" + StringUtils.humanReadableInt(curSize) + ")");
			int index = 0;
			for (KeyValue kv : map) {
				context.write(rowKey, kv);
				if (++index > 0 && index % 100 == 0)
					context.setStatus("Wrote " + index + " key values.");
			}

			// if we have more entries to process
			if (iter.hasNext()) {
				// force flush because we cannot guarantee intra-row sorted order
				context.write(null, null);
			}
		}
	}
}
