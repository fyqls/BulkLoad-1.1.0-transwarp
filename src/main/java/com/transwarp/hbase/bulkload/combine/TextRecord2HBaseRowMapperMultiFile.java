package com.transwarp.hbase.bulkload.combine;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.transwarp.hbase.bulkload.FormatException;
import com.transwarp.hbase.bulkload.HBaseTableSpec;
import com.transwarp.hbase.bulkload.ParsedLine;
import com.transwarp.hbase.bulkload.TextRecord2HBaseRowConverter;
import com.transwarp.hbase.bulkload.TextRecordSpec;

/**
 * Write table content out to files in hdfs.
 */
public class TextRecord2HBaseRowMapperMultiFile extends
		Mapper<MultiFileInputWritableComparable, Text, ImmutableBytesWritable, Text> {

	public static final String ENCODING = "encoding";

	public TextRecord2HBaseRowConverter converter;

	/** Timestamp for all inserted rows */
	private final long ts = 0;//

	/** Should skip bad lines */
	private boolean skipBadLines;
	private Counter badLineCount;
	private String encoding ="UTF-8";
	
  public static Text setEncode(Text text,String encoding){
    if (null == encoding) {
        return text;
    }
   String value=null;
   try {
       value=new String(text.getBytes(),0,text.getLength(),encoding);
       return new Text(value);
   } catch (Exception e) {
       e.printStackTrace();
   }
    return text;
   }
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
		this.encoding = context.getConfiguration().get("encoding");
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
	public void map(MultiFileInputWritableComparable offset, Text value, Context context)
			throws IOException {
		/** This comment is very important:
		 *  If you want to get the line from the text, you must use the toString() instead of getBytes()
		 *  The getBytes() will make the end of the string more chars, that is, it has a bug
		*/
		value = setEncode(value, encoding);
	   String lineStr = value.toString();

		try {
			ArrayList<String> parsedLine = ParsedLine.parse(converter.getRecordSpec(),
					lineStr);
			byte[] rowKey = converter.genRowKey(parsedLine, uniqID++);
			ImmutableBytesWritable row = new ImmutableBytesWritable(rowKey);
			context.write(row, value);
		} catch (FormatException ex) {
			System.err.println("Bad line at offset: " + offset.getOffset()
					+ " --> Malformat Line: " + lineStr);
			ex.printStackTrace();
			incrementBadLineCount(1);
			return;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
