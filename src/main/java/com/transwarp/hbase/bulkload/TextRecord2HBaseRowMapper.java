package com.transwarp.hbase.bulkload;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Write table content out to files in hdfs.
 */
public class TextRecord2HBaseRowMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	public static final String ENCODING = "encoding";

	public TextRecord2HBaseRowConverter converter;

	/** Timestamp for all inserted rows */
	private final long ts = 0;//

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
	protected long uniqID = 0;
	/**
	 * Handles common parameter initialization that a subclass might want to
	 * leverage.
	 * 
	 * @param context
	 */
	protected void doSetup(Context context) {
		uniqID = (((long)context.getJobID().getId()) << 52);
		uniqID += (((long)context.getTaskAttemptID().getTaskID().getId()) << 32);
		
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
		System.out.println("##########Encoding:"
				+ context.getConfiguration().get("encoding"));
		String rowSpec = context.getConfiguration().get("rowSpec");
		TextRecordSpec recordSpec = null;
		try {
			recordSpec = new TextRecordSpec(textRecordSpec,
					encoding, fieldDelimiter);
			HBaseTableSpec tableSpec = null;
			if (StringUtils.isEmpty(internalFieldDelimiter)) {
				tableSpec = new HBaseTableSpec(tableName,
						rowSpec, splitKeySpec, recordSpec);
			} else {
				tableSpec = new HBaseTableSpec(tableName,
						rowSpec, splitKeySpec, recordSpec,
						HBaseTableSpec.EXTERNAL_COLUMN_DELIMITER,
						internalFieldDelimiter);
			}

			converter = new TextRecord2HBaseRowConverter(recordSpec, tableSpec);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		/* Initial Line Parser */
		ParsedLine.escapedFieldDelimiter = recordSpec.getEscapedFieldDelimiter();
	}

	/**
	 * Convert a line of TXT text into an HBase table row.
	 */
	@Override
	public void map(LongWritable offset, Text value, Context context)
			throws IOException {
		/** This comment is very important:
		 *  If you want to get the line from the text, you must use the toString() instead of getBytes()
		 *  The getBytes() will make the end of the string more chars, that is, it has a bug
		*/
		String lineStr = value.toString();

		try {
			ArrayList<String> parsed = ParsedLine.parse(converter.getRecordSpec(),
					lineStr);
			byte[] rowKeyBytes = converter.genRowKey(parsed, uniqID++);
			Put put = converter.convert(parsed, rowKeyBytes);
			ImmutableBytesWritable row = new ImmutableBytesWritable(put
					.getRow());
			context.write(row, put);
		} catch (FormatException ex) {
			System.err.println("Bad line at offset: " + offset.get()
					+ " --> Malformat Line: " + lineStr);
			ex.printStackTrace();
			incrementBadLineCount(1);
			return;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
