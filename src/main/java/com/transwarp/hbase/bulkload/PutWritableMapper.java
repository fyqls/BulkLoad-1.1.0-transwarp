package com.transwarp.hbase.bulkload;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.transwarp.hbase.bulkload.common.BConstants.BulkLoadProps;

/**
 * Write table content out to files in hdfs.
 */
public class PutWritableMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, PutWritable> {

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
		boolean emptyStringAsNull = context.getConfiguration().getBoolean(BulkLoadProps.EMPTY_STRING_AS_NULL.getName(), false);
		boolean rowKeyAutoIncrease = context.getConfiguration().getBoolean(BulkLoadProps.ROWKEY_AUTO_INCREASING.getName(), false);
		boolean useHyperbaseDataType = context.getConfiguration().getBoolean(BulkLoadProps.USE_HYPERBASE_DATATYPE.getName(), false);
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
			tableSpec.setEmptyStringAsNull(emptyStringAsNull);
			tableSpec.setRowkeyAutoIncreasing(rowKeyAutoIncrease);
			tableSpec.setUseHyperbaseDataType(useHyperbaseDataType);

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
		value = setEncode(value, encoding);
	  String lineStr = value.toString();
		
		try {
			ArrayList<String> parsedLine = ParsedLine.parse(converter.getRecordSpec(),
					lineStr);
			byte[] rowKey = converter.genRowKey(parsedLine, uniqID++);
			PutWritable p = converter.convertPutWritable(parsedLine, rowKey);
			ImmutableBytesWritable row = new ImmutableBytesWritable(rowKey);
			context.write(row, p);
		} catch (FormatException ex) {
		  ex.printStackTrace();
			incrementBadLineCount(1);
			return;
		} catch (InterruptedException e) {
		}
	}
}
