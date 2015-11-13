package com.transwarp.hbase.bulkload.withindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.transwarp.hbase.bulkload.ColumnSpec;
import com.transwarp.hbase.bulkload.FormatException;
import com.transwarp.hbase.bulkload.HBaseTableSpec;
import com.transwarp.hbase.bulkload.ParsedLine;
import com.transwarp.hbase.bulkload.TextRecord2HBaseRowConverter;
import com.transwarp.hbase.bulkload.TextRecordSpec;
import com.transwarp.hbase.bulkload.combine.MultiFileInputWritableComparable;
import com.transwarp.hbase.bulkload.common.BConstants.BulkLoadProps;

/**
 * Write table content out to files in hdfs.
 */
public class TextMultiFileInputWithIndexMapper extends
		Mapper<MultiFileInputWritableComparable, Text, ImmutableBytesWritable, Text> {

  static final Log LOG = LogFactory.getLog(TextMultiFileInputWithIndexMapper.class);
  
	public static final String ENCODING = "encoding";
	
	/** Index tables setting*/
	public String[] indexTables = new String[0];
	
	public TextRecord2HBaseRowConverter converter;

	/** Timestamp for all inserted rows */
	private final long ts = 0;//
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
    this.encoding = context.getConfiguration().get("encoding");
    
		System.out.println("WithIndexMapper##########Encoding:"
				+ context.getConfiguration().get("encoding"));
		String rowSpec = context.getConfiguration().get("rowSpec");
		boolean rowKeyAutoIncrease = context.getConfiguration().getBoolean(BulkLoadProps.ROWKEY_AUTO_INCREASING.getName(), false);
		boolean useHyperbaseDataType = context.getConfiguration().getBoolean(BulkLoadProps.USE_HYPERBASE_DATATYPE.getName(), false);
		TextRecordSpec recordSpec = null;
		HashMap<String, Pair<ColumnSpec, ColumnSpec>> indexTableRowkeySpecs = new HashMap<String, Pair<ColumnSpec, ColumnSpec>>();
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
			tableSpec.setRowkeyAutoIncreasing(rowKeyAutoIncrease);
			tableSpec.setUseHyperbaseDataType(useHyperbaseDataType);

			 /* Get index tables settings */
	    indexTables = context.getConfiguration().get("indextables").split(",");
	    for (String indextable : indexTables) {
	      try {
	        String strs = context.getConfiguration().get(indextable);
	        String[] values = strs.split("@");
	        String rowkeyFormat = values[1];
	        ColumnSpec rowkeySpec = new ColumnSpec("rowkey",rowkeyFormat, recordSpec.getFieldMap());
	        String familyFormat = values[2];
	        ColumnSpec familySpec = new ColumnSpec("fam", familyFormat, recordSpec.getFieldMap());
	        indexTableRowkeySpecs.put(indextable, new Pair<ColumnSpec, ColumnSpec>(rowkeySpec, familySpec));
	      } catch (RuntimeException e) {
	        String error = "ExceptionError: " + context.getConfiguration().getStrings(indextable).length;
	        for (String str : context.getConfiguration().getStrings(indextable)) {
	          error += str + " ";
	        }
	        throw new RuntimeException(error, e);
	      }
	        
	    }
	    
	    converter = new TextRecord2HBaseRowConverter(recordSpec, tableSpec, indexTableRowkeySpecs);
			
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
			byte[] rowKey = converter.genRowKey(parsedLine, uniqID);
			// Add rowkey prefix for primary table
			byte[] prefixRowKey = Bytes.add(Bytes.toBytes(0), rowKey);
			ImmutableBytesWritable row = new ImmutableBytesWritable(prefixRowKey);
			context.write(row, value);
			
			// Generate output for index tables
			int tableIndex = 1;
			for (String indextable : indexTables) {
			  byte[] indexTableRowkey = converter.genIndexTableRowKey(indextable, parsedLine, uniqID);
			  // Add rowkey prefix for index table
			  byte[] prefixIndexRowKey = Bytes.add(Bytes.toBytes(tableIndex), indexTableRowkey);
	      ImmutableBytesWritable indexRow = new ImmutableBytesWritable(prefixIndexRowKey);
	      Text indexValue= new Text(converter.genIndexTableQualifier(indextable, parsedLine, tableIndex));
	      context.write(indexRow, indexValue);
	      tableIndex++;
			}
			
			uniqID++;
		} catch (FormatException ex) {
			incrementBadLineCount(1);
			return;
		} catch (InterruptedException e) {
		} 
	}
}
