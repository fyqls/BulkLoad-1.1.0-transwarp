package com.transwarp.hbase.bulkload.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;

public class BConstants {
	private static final String DFS_SHORTCIRCUIT = "dfs.client.read.shortcircuit";
	private static final String HBASE_BLOCK_MULTIPLIER = "hbase.hregion.memstore.block.multiplier";
	private static final String REGIONSERVER_MSGINTERVAL = "hbase.regionserver.msginterval";
	private static final String HSTORE_BLOCKINGWAITTIME = "hbase.hstore.blockingWaitTime";
	private static final String HSTORE_COMPACTION_THRESHOLD = "hbase.hstore.compactionThreshold";
	private static final String CLIENT_SCANNER_CACHING = "hbase.client.scanner.caching";
	private static final String CLIENT_WRITE_BUFFER = "hbase.client.write.buffer";

	public static final String BLOOMFILTER = "BLOOMFILTER";
	public static final String BLOCKCACHE = "BLOCKCACHE";
	private static final String TEST_PREFIX = "test_";
	
	// The default hbase conf for the bulk load
	// I guess these configuration parameters have some limitation, maybe
	public static Map<String, String> DEFAULT_HBASE_CONF = new HashMap<String, String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put(DFS_SHORTCIRCUIT, "true");
			put(HConstants.HREGION_MAX_FILESIZE, "3221225472"); // 3GB
			put(HConstants.BYTES_PER_CHECKSUM, "131072");
			put(HConstants.HBASE_CHECKSUM_VERIFICATION, "false");
			put(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, "134217728"); // 128MB
			put(HBASE_BLOCK_MULTIPLIER, "2");
			put(REGIONSERVER_MSGINTERVAL, "9");
			put("hbase.hstore.blockingStoreFiles", "15");
			put(HSTORE_BLOCKINGWAITTIME, "6000");
			put("hbase.hstore.compaction.max", "25");
			put(HSTORE_COMPACTION_THRESHOLD, "6");
			put(CLIENT_SCANNER_CACHING, "2000");
			put(CLIENT_WRITE_BUFFER, "16777216"); // 16MB
			put(HConstants.VERSIONS, "1"); // 1
			put(BLOOMFILTER, BloomType.ROW.name());
			put(BLOCKCACHE, "true");
		}
	};

	public static Map<String, String> DEFAULT_HFILE_CONF = new HashMap<String, String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put(HFileConf.DATABLOCK_ENCODING_CONF_KEY.getName(),
					DataBlockEncoding.PREFIX.name());
			put(HFileConf.COMPRESSION_CONF_KEY.getName(),
					Algorithm.SNAPPY.getName());
			put(HFileConf.HFILEOUTPUTFORMAT_BLOCKSIZE.getName(), HConstants.DEFAULT_BLOCKSIZE + "");
			put(HFileConf.HFILE_COMPRESSION.getName(),
					Algorithm.SNAPPY.getName());
		}
	};

	public static Map<String, String> BULKLOAD_PROPS = new HashMap<String, String>() {
    private static final long serialVersionUID = 1L;
    
    {
      put(BulkLoadProps.INPUT_FORMAT.getName(), "Text");
      put(BulkLoadProps.MIN_SPLIT_SIZE.getName(), "1");
      put(BulkLoadProps.SPLIT_SIZE.getName(), "67108864");
      put(BulkLoadProps.IS_REMOTE.getName(), "false");
      put(BulkLoadProps.INDEX_COLUMNS.getName(), "");
      put(BulkLoadProps.TARGET_COLUMN_FAMILY.getName(), "");
      put(BulkLoadProps.START_ROW.getName(), "");
      put(BulkLoadProps.STOP_ROW.getName(), "");
      put(BulkLoadProps.INDEX_OUTPUT_DIR.getName(), "");
      put(BulkLoadProps.INDEX_SPLIT_KEY_SPEC.getName(), "");
      put(BulkLoadProps.FIELD_NAME_TYPE_DELIMITER.getName(), ":");
      put(BulkLoadProps.ENCODING.getName(), "UTF-8");
      put(BulkLoadProps.ROWKEY_AUTO_INCREASING.getName(), "false");
      put(BulkLoadProps.SPLIT_KEY_SPEC.getName(), "");
      put(BulkLoadProps.EMPTY_STRING_AS_NULL.getName(), "false");
      put(BulkLoadProps.USE_HYPERBASE_DATATYPE.getName(), "false");
      put(BulkLoadProps.INDEXTABLE_LIST.getName(), "");
    }
  };
	
	public enum BulkLoadProps {
		// === Common ================
		TABLE_NAME("tableName"),
		INPUT_DIR("inputDir"),
		OUTPUT_DIR("outputDir"),
		TEXT_RECORD_SPEC("textRecordSpec"),
		ENCODING("encoding"),
		FIELD_DELIMITER("fieldDelimiter"),
		FIELD_NAME_TYPE_DELIMITER("fieldNameTypeDelimiter"),
		ROW_SPEC("rowSpec"),
		INTERNAL_FIELD_DELIMITER("internalFieldDelimiter"),
		SPLIT_KEY_SPEC("splitKeySpec"),
		SCHEMA_HDFS_PATH("schemaPath"),
		INPUT_FORMAT("inputFormat"),
		ROWKEY_AUTO_INCREASING("rowkeyAutoIncreasing"),
		EMPTY_STRING_AS_NULL("emptyStringAsNull"),
		USE_HYPERBASE_DATATYPE("useHyperbaseDataType"),
		LOADTHEMIS("loadThemis"),
		INDEXTABLE_LIST("indextablelist"),
		INDEXTYPE("indextype"),
				
		// ============= Text File Input Format ==============
		MIN_SPLIT_SIZE("mapred_min_split_size"),

		// ============= Combine File Input Format ===========
		SPLIT_SIZE("splitSize"),
		IS_REMOTE("isRemote"),

		// ============= Multi HFile Input Format ===========
		INDEX_TABLE_NAME("indextablename"),
		INDEX_COLUMNS("indexcolumns"),
		TARGET_COLUMN_FAMILY("targetcolumnfamily"),
		START_ROW("startrow"),
		STOP_ROW("stoprow"),
		INDEX_OUTPUT_DIR("indexOutputDir"),
		INDEX_SPLIT_KEY_SPEC("indexSplitKeySpec"),
		
		// ============= Batch Update and Delete ============
		BATCHMODIFY("batchmodifytable"),
		BATCHUPDATE("batchupdate"),
		BATCHDELETE("batchdelete"),
		DATE_TOBE_UPDATED("datetobeupdated"),
		COLUMNS_TOBE_UPDATED("allcolumnstobeupdated"),
		DATE_START_INDEX("datestartindex"),
		DATE_COLUMN_LENGTH("datecolumnlength"),
		DATE_TOBE_DELETED("datetobedeleted"),
		ALL_COLUMNFAMILIES_TOBE_DELETED("allcfstobedeleted");

		private String name;

		BulkLoadProps(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}
		
	}

	public static enum HFileConf {
		COMPRESSION_CONF_KEY("hbase.hfileoutputformat.families.compression"),
		DATABLOCK_ENCODING_CONF_KEY("hbase.mapreduce.hfileoutputformat.datablock.encoding"),
		HFILEOUTPUTFORMAT_BLOCKSIZE("hbase.mapreduce.hfileoutputformat.blocksize"),
		HFILEOUTPUTFORMAT_COMPACTION_EXCLUDE("hbase.mapreduce.hfileoutputformat.compaction.exclude"),
		HFILE_COMPRESSION("hfile.compression"),
		REPLICATION_CONF_KEY("hbase.hfileoutputformat.families.replication");
		
		private String name;

		HFileConf(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}
	}

	public static enum InputFormat {
		TEXT("Text"),
		COMBINE_FILE("CombineFile"),
		MULTI_HFILE("MultiHFile");

		private String name;

		InputFormat(String name) {
			this.name = name;
		}

		public String getName() {
			return this.name;
		}
	}
	
	public static enum Function {
		TRIM, RPAD, LPAD, SUBSTR, TOTIMESTAMP,CONCAT,
		REVERSE, RAND, STRING_CONCAT, ASTYPE, NOTNULL,
    TRIM_QUOTATION, REPLACE, LIANTONG_TRANSFERURL,
    LIANTONG_TRANSFERIP, REMOVE_WHITESPACE, DATETOMILLTIME,
    TIMEREVERSE, COMBINEINDEX,CURRENT_TIME, LENGTH,
    SUBSTRFORMAT, STRINGFORMAT,TO_SHORT_B,TO_INT_B, TO_LONG_B,
    LONG_B_TO_ST, INT_B_TO_ST, SHORT_B_TO_ST,TO_DOUBLE
	}
	
	public static enum ParamType {
		FIELD, CONST, RESULT
	}
}
