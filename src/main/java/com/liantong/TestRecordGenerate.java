package com.liantong;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import com.transwarp.hbase.bulkload.HBaseTableSpec;
import com.transwarp.hbase.bulkload.ParsedLine;
import com.transwarp.hbase.bulkload.TextRecord2HBaseRowConverter;
import com.transwarp.hbase.bulkload.TextRecordSpec;
import com.transwarp.hbase.bulkload.common.BConstants;

/**
 * Tool to import data from a text record file delimited by special characters.
 * 
 * This tool is rather simplistic - it doesn't do any quoting or escaping, but
 * is useful for many data loads.
 * 
 */
public class TestRecordGenerate {

	public static void main(String[] args) throws Exception {
		args = new String[1];
		args[0] = "test.properties";
		Configuration conf = new Configuration();
		Properties props = new Properties();
		props.load(new FileInputStream(args[0]));
		initBulkConf(conf, props);
		runBulkLoadTest(conf);
	}

	private static void runBulkLoadTest(Configuration conf) throws Exception {
    String textRecordSpec = conf.get(BConstants.BulkLoadProps.TEXT_RECORD_SPEC.getName());
    String fieldDelimiter = conf.get(BConstants.BulkLoadProps.FIELD_DELIMITER.getName());
    String internalFieldDelimiter = conf.get(BConstants.BulkLoadProps.INTERNAL_FIELD_DELIMITER.getName());
    String encoding = conf.get(BConstants.BulkLoadProps.ENCODING.getName(), "UTF-8");
    String rowSpec = conf.get(BConstants.BulkLoadProps.ROW_SPEC.getName());
    String splitKeySpec = conf.get(BConstants.BulkLoadProps.SPLIT_KEY_SPEC.getName());
    String tableName = conf.get(BConstants.BulkLoadProps.TABLE_NAME.getName());
    String indexTableName = conf.get(BConstants.BulkLoadProps.INDEX_TABLE_NAME.getName());
    String indexSplitKeySpec = conf.get(BConstants.BulkLoadProps.INDEX_SPLIT_KEY_SPEC.getName());
    
    TextRecordSpec recordSpec = new TextRecordSpec(textRecordSpec, encoding,
        fieldDelimiter);

    ParsedLine.escapedFieldDelimiter = recordSpec.getEscapedFieldDelimiter();
    // Set the table spec based on the properties
    HBaseTableSpec tableSpec = null;
    tableSpec = new HBaseTableSpec(tableName, rowSpec, splitKeySpec, recordSpec,
        HBaseTableSpec.EXTERNAL_COLUMN_DELIMITER, internalFieldDelimiter,
        indexTableName, indexSplitKeySpec);
    runRecordGenerateTest(tableSpec, recordSpec, conf);
  }

	
	private static void initBulkConf(Configuration conf, Properties pro) {
	  for (Object str : pro.keySet()) {
	    if (str instanceof String) {
	      conf.set((String)str, pro.getProperty((String)str));
	      System.out.println(str + " : " + pro.getProperty((String) str));
	    }
	  }
	}
	
	private static void runRecordGenerateTest(HBaseTableSpec tableSpec, TextRecordSpec recordSpec, Configuration conf)
	    throws Exception {
	  //String lineStr = ""
		String lineStr = "1^A厂轴试武进屈壶膛伊卡揩诽殿^A尹立军^A320483600343581^AL15267122^A10^A在营";
		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(recordSpec, tableSpec);
    ArrayList<String> parsedLine = ParsedLine.parse(converter.getRecordSpec(),
        lineStr);
    byte[] rowKey = converter.genRowKey(parsedLine, 111);
    System.out.println(Bytes.toString(rowKey));
	}
	
	
}
