package com.transwarp.hbase.bulkload.batch;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.transwarp.hbase.bulkload.batch.delete.BatchDeleteMapper;
import com.transwarp.hbase.bulkload.batch.delete.DeleteSortReducer;
import com.transwarp.hbase.bulkload.batch.update.BatchUpdateMapper;
import com.transwarp.hbase.bulkload.common.BConstants;
import com.transwarp.hbase.bulkload.secondaryindex.MultiHFileInputFormat;

/**
 * Tool to import data from a text record file delimited by special characters.
 * 
 * This tool is rather simplistic - it doesn't do any quoting or escaping, but
 * is useful for many data loads.
 * 
 */
public class BatchModifyHBase {
	final static String NAME = "BatchModifyHBase";

	private static String TEST_PREFIX = "test_";

	protected static final String CF_C_DELIMITER = "|";

	private static HBaseAdmin hbaseAdmin;

	private static boolean isNewTable = false;

	/**
	 * Sets up the actual job.
	 * 
	 * @param conf
	 *          The current configuration.
	 * @param args
	 *          The command line parameters.
	 * @return The newly created job.
	 * @throws IOException
	 *           When setting up the job fails.
	 */
	public static Job createSubmittableJob(Configuration conf,
			String tableName, String inputDir,
			String outputDirStr) throws IOException, ClassNotFoundException {

		String inputSize = conf.get(BConstants.BulkLoadProps.SPLIT_SIZE.getName());
		if (inputSize != null) {
			conf.set("mapred.max.split.size", inputSize);
			conf.set("mapred.min.split.size.per.rack", inputSize);
			conf.set("mapred.min.split.size.per.node", inputSize);
		}
		Job job = new Job(conf, NAME + "_" + tableName);

		String inputFormat = conf.get(BConstants.BulkLoadProps.INPUT_FORMAT
				.getName());
		if (inputFormat != null) {
			job.setReducerClass(PutSortReducer.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);
				String batchModifyTable = conf.get(BConstants.BulkLoadProps.BATCHMODIFY
						.getName());
				if(batchModifyTable!= null) {
					if(batchModifyTable.equalsIgnoreCase(BConstants.BulkLoadProps.BATCHUPDATE
							.getName())) {
						job.setJarByClass(BatchUpdateMapper.class);
						job.setMapperClass(BatchUpdateMapper.class);
						job.setInputFormatClass(MultiHFileInputFormat.class);
					} else  {
						job.setReducerClass(DeleteSortReducer.class);
						job.setMapOutputKeyClass(ImmutableBytesWritable.class);
						job.setMapOutputValueClass(Delete.class);
						job.setJarByClass(BatchDeleteMapper.class);
						job.setMapperClass(BatchDeleteMapper.class);
						job.setInputFormatClass(MultiHFileInputFormat.class);
					}
				}
			}
		

		// job.setInputFormatClass(TextInputFormat.class);
		// job.setMapperClass(TextRecord2HBaseRowMapper.class);

		String hfileOutPath = outputDirStr;
		if (hfileOutPath != null) {
			if (!doesTableExist(tableName)) {
				System.out.println(tableName + "does not exit on hbase");
				System.exit(2);
			}
			HTable table = new HTable(conf, tableName);
			Path outputDir = new Path(hfileOutPath);
			FileOutputFormat.setOutputPath(job, outputDir);
			HFileOutputFormat.configureIncrementalLoad(job, table);
		} else {
			// No reducers. Just write straight to table. Call
			// initTableReducerJob
			// to set up the TableOutputFormat.
			TableMapReduceUtil.initTableReducerJob(tableName, null, job);
			job.setNumReduceTasks(0);
		}

		TableMapReduceUtil.addDependencyJars(job);
		TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
				com.google.common.base.Function.class);
		return job;
	}

	private static boolean doesTableExist(String tableName) throws IOException {
		return hbaseAdmin.tableExists(tableName.getBytes());
	}


	/**
	 * Main entry point.
	 * 
	 * @param args
	 *          The command line parameters.
	 * @throws Exception
	 *           When running the job fails.
	 */
	public static void main(String[] args) throws Exception {
		boolean isTest = false;
		if (args.length < 1 || args.length > 2) {
			System.out.println("Usage: " + NAME + " <configFile> [-test]\n");
			System.exit(1);
		}
		if (args.length == 2) {
			if (args[1].equalsIgnoreCase("-test")) {
				isTest = true;
			}
		}

		Configuration conf = HBaseConfiguration.create();

		Properties props = new Properties();
		props.load(new FileInputStream(args[0]));

//		if (BulkLoadChecker.checkProps(props, isTest)) {
//			System.out.println(BulkLoadChecker.hintInfo.toString());
//		} else {
//			System.out.println(BulkLoadChecker.errorInfo.toString());
//			System.exit(1);
//		}

		// Initialize the configuration for bulk load
		// Use the default value if the user don't set them in the
		// conf.properties
		initHBaseConf(conf, props);
		initHFileConf(conf, props);
		initAllConf(conf, props);

		hbaseAdmin = new HBaseAdmin(conf);

		String tableNameProperty = isTest ? TEST_PREFIX
				+ BConstants.BulkLoadProps.TABLE_NAME.getName()
				: BConstants.BulkLoadProps.TABLE_NAME.getName();
		String tableName = props.getProperty(tableNameProperty);
		String inputDirProperty = isTest ? TEST_PREFIX
				+ BConstants.BulkLoadProps.INPUT_DIR.getName()
				: BConstants.BulkLoadProps.INPUT_DIR.getName();
		String inputDirStr = props.getProperty(inputDirProperty, "");
		String outputDirProperty = isTest ? TEST_PREFIX
				+ BConstants.BulkLoadProps.OUTPUT_DIR.getName()
				: BConstants.BulkLoadProps.OUTPUT_DIR.getName();
		String outPutDirStr = props.getProperty(outputDirProperty);
		String inputFormat = props.getProperty(
				BConstants.BulkLoadProps.INPUT_FORMAT.getName(), "Text");

		// ========== Get the properties for TextFile ===================
		String minSplitSize = props.getProperty(
				BConstants.BulkLoadProps.MIN_SPLIT_SIZE.getName(), "1");

		// ========== Get the properties for CombineFile ================
		String splitSize = props.getProperty(
				BConstants.BulkLoadProps.SPLIT_SIZE.getName(), "67108864"); // 64MB
		String isRemote = props.getProperty(
				BConstants.BulkLoadProps.IS_REMOTE.getName(), "false");

		// ========== Get the properties for MultiHFile ==================
		String indexTableNameProperty = isTest ? TEST_PREFIX
				+ BConstants.BulkLoadProps.INDEX_TABLE_NAME.getName()
				: BConstants.BulkLoadProps.INDEX_TABLE_NAME.getName();
		String indexTableName = props.getProperty(indexTableNameProperty, "");
		String indexColumns = props.getProperty(
				BConstants.BulkLoadProps.INDEX_COLUMNS.getName(), "");
		String targetColumnFamily = props.getProperty(
				BConstants.BulkLoadProps.TARGET_COLUMN_FAMILY.getName(), "");
		String startRow = props.getProperty(
				BConstants.BulkLoadProps.START_ROW.getName(), "");
		String stopRow = props.getProperty(
				BConstants.BulkLoadProps.STOP_ROW.getName(), "");
		String indexOutputDir = props.getProperty(
				BConstants.BulkLoadProps.INDEX_OUTPUT_DIR.getName(), "");
		String indexSplitKeySpec = props.getProperty(
				BConstants.BulkLoadProps.INDEX_SPLIT_KEY_SPEC.getName(), "");

		// ================= Common =================================
		String textRecordSpec = props
				.getProperty(BConstants.BulkLoadProps.TEXT_RECORD_SPEC.getName(),"");
		String fieldDelimiter = props
				.getProperty(BConstants.BulkLoadProps.FIELD_DELIMITER.getName(),"");
		String internalFieldDelimiter = props
				.getProperty(BConstants.BulkLoadProps.INTERNAL_FIELD_DELIMITER.getName(),"");
		String fieldNameTypeDelimiter = props.getProperty(
				BConstants.BulkLoadProps.FIELD_NAME_TYPE_DELIMITER.getName(), ":");
		String encoding = props.getProperty(
				BConstants.BulkLoadProps.ENCODING.getName(), "UTF-8");
		String rowSpec = props.getProperty(BConstants.BulkLoadProps.ROW_SPEC
				.getName(),"");
		String splitKeySpec = props
				.getProperty(BConstants.BulkLoadProps.SPLIT_KEY_SPEC.getName(),"");
		
		//get the properties used by batch modification
		String batchModification =  props
				.getProperty(BConstants.BulkLoadProps.BATCHMODIFY.getName(),"");
		String startdateindex =  props
				.getProperty(BConstants.BulkLoadProps.DATE_START_INDEX.getName(),"");
		String datecolumnlength = props
				.getProperty(BConstants.BulkLoadProps.DATE_COLUMN_LENGTH.getName(),"");
		String update_date = props.getProperty(BConstants.BulkLoadProps.DATE_TOBE_UPDATED.getName(),"");
		String delete_date = props.getProperty(BConstants.BulkLoadProps.DATE_TOBE_DELETED.getName(),"");
		String updateColumns = props.getProperty(BConstants.BulkLoadProps.COLUMNS_TOBE_UPDATED.getName(),"");
		String deleteColumns = props.getProperty(BConstants.BulkLoadProps.ALL_COLUMNFAMILIES_TOBE_DELETED.getName(),"");
		
		// Set the table spec based on the properties
//		HBaseTableSpec tableSpec = null;
//		tableSpec = new HBaseTableSpec(tableName, rowSpec, splitKeySpec,
//				HBaseTableSpec.EXTERNAL_COLUMN_DELIMITER, internalFieldDelimiter,
//				indexTableName, indexSplitKeySpec);

		// Judge whether the hbase table has existed
		if (!doesTableExist(tableName)) {
			System.err.print("Source table does not exist. Exit.");
			System.exit(1);
		}

		conf.set(BConstants.BulkLoadProps.TABLE_NAME.getName(), tableName);
		conf.set(BConstants.BulkLoadProps.INPUT_DIR.getName(), inputDirStr);
		conf.set(BConstants.BulkLoadProps.OUTPUT_DIR.getName(), outPutDirStr);
		conf.set(BConstants.BulkLoadProps.INPUT_FORMAT.getName(), inputFormat);

		// Set the parameters for the TextFile
		conf.set(
				BConstants.BulkLoadProps.MIN_SPLIT_SIZE.getName().replaceAll("_", "."),
				minSplitSize);

		// Set the parameters for the CombineFile
		conf.set(BConstants.BulkLoadProps.SPLIT_SIZE.getName(), splitSize);
		conf.set(BConstants.BulkLoadProps.IS_REMOTE.getName(), isRemote);

		// Set the parameters for the MultiHFile
		conf.set(BConstants.BulkLoadProps.INDEX_TABLE_NAME.getName(), indexTableName);
		conf.set(BConstants.BulkLoadProps.INDEX_COLUMNS.getName(), indexColumns);
		conf.set(BConstants.BulkLoadProps.TARGET_COLUMN_FAMILY.getName(),
				targetColumnFamily);
		conf.set(BConstants.BulkLoadProps.START_ROW.getName(), startRow);
		conf.set(BConstants.BulkLoadProps.STOP_ROW.getName(), stopRow);
		conf.set(BConstants.BulkLoadProps.INDEX_OUTPUT_DIR.getName(), indexOutputDir);
		conf.set(BConstants.BulkLoadProps.INDEX_SPLIT_KEY_SPEC.getName(),
				indexSplitKeySpec);

		conf.set(BConstants.BulkLoadProps.TEXT_RECORD_SPEC.getName(), textRecordSpec);
		conf.set(BConstants.BulkLoadProps.FIELD_DELIMITER.getName(), fieldDelimiter);
		conf.set(BConstants.BulkLoadProps.INTERNAL_FIELD_DELIMITER.getName(),
				internalFieldDelimiter);
		conf.set(BConstants.BulkLoadProps.FIELD_NAME_TYPE_DELIMITER.getName(),
				fieldNameTypeDelimiter);
		conf.set(BConstants.BulkLoadProps.ENCODING.getName(), encoding);
		conf.set(BConstants.BulkLoadProps.ROW_SPEC.getName(), rowSpec);
		conf.set(BConstants.BulkLoadProps.SPLIT_KEY_SPEC.getName(), splitKeySpec);

		conf.set(BConstants.BulkLoadProps.BATCHMODIFY.getName(), batchModification);
		conf.set(BConstants.BulkLoadProps.DATE_START_INDEX.getName(), startdateindex);
		conf.set(BConstants.BulkLoadProps.DATE_COLUMN_LENGTH.getName(), datecolumnlength);
		conf.set(BConstants.BulkLoadProps.DATE_TOBE_UPDATED.getName(), update_date);
		conf.set(BConstants.BulkLoadProps.DATE_TOBE_DELETED.getName(), delete_date);
		conf.set(BConstants.BulkLoadProps.COLUMNS_TOBE_UPDATED.getName(), updateColumns);
		conf.set(BConstants.BulkLoadProps.ALL_COLUMNFAMILIES_TOBE_DELETED.getName(), deleteColumns);
		
		// ========= Start to run the Job =======================
		long start = System.currentTimeMillis();
		Job job = createSubmittableJob(conf, tableName, inputDirStr,
				outPutDirStr);
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		// ============= End of the Job =====================

		System.out.println("Import File to HBase Costs: " + (end - start) / 1000
				+ "s");
	}

	private static void initHBaseConf(Configuration conf, Properties props) {
		for (String hbaseConfKey : BConstants.DEFAULT_HBASE_CONF.keySet()) {
			String hbaseConfValue = props.getProperty(hbaseConfKey);
			if ((hbaseConfValue != null) && (!hbaseConfValue.isEmpty())) {
				BConstants.DEFAULT_HBASE_CONF.put(hbaseConfKey, hbaseConfValue);
			}
			conf.set(hbaseConfKey, BConstants.DEFAULT_HBASE_CONF.get(hbaseConfKey));
		}
	}

	private static void initHFileConf(Configuration conf, Properties props) {
		for (String hfileConfKey : BConstants.DEFAULT_HFILE_CONF.keySet()) {
			String hfileConfValue = props.getProperty(hfileConfKey);
			if ((hfileConfValue != null) && (!hfileConfValue.isEmpty())) {
				BConstants.DEFAULT_HFILE_CONF.put(hfileConfKey, hfileConfValue);
			}
			conf.set(hfileConfKey, BConstants.DEFAULT_HFILE_CONF.get(hfileConfKey));
		}
	}
	
	private static void initAllConf (Configuration conf, Properties props) {
	  for (Object str : props.keySet()) {
	    if (str instanceof String) {
	      conf.set((String)str, props.getProperty((String)str));
	    }
	  }
	}
}
