package com.transwarp.hbase.bulkload;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.datatype.DataType;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.transwarp.hbase.bulkload.check.BulkLoadChecker;
import com.transwarp.hbase.bulkload.combine.MultiFileInputFormat;
import com.transwarp.hbase.bulkload.combine.TextRecord2HBaseRowMapperMultiFile;
import com.transwarp.hbase.bulkload.combine.remote.MultiRemoteFileInputFormat;
import com.transwarp.hbase.bulkload.common.BConstants;
import com.transwarp.hbase.bulkload.common.BConstants.BulkLoadProps;
import com.transwarp.hbase.bulkload.common.BConstants.HFileConf;
import com.transwarp.hbase.bulkload.common.BulkLoadUtils;
import com.transwarp.hbase.bulkload.secondaryindex.HFile2SecondaryIndexHFileMapper;
import com.transwarp.hbase.bulkload.secondaryindex.MultiHFileInputFormat;
import com.transwarp.hbase.bulkload.withindex.TextMultiFileInputWithIndexMapper;
import com.transwarp.hbase.bulkload.withindex.TextWithIndexMapper;
import com.transwarp.hbase.bulkload.withindex.TranswarpHFileOutputFormat;

/**
 * Tool to import data from a text record file delimited by special characters.
 * 
 * This tool is rather simplistic - it doesn't do any quoting or escaping, but
 * is useful for many data loads.
 * 
 */
public class ImportTextFile2HBase {
	final static String NAME = "ImportTextFile2HBase";

	protected static final String CF_C_DELIMITER = "|";
	private static final String MAPRED_TASK_TIMEOUT = "1800000";

	private static HBaseAdmin hbaseAdmin;

	private static boolean isNewTable = false;

  private static final Log LOG =
        LogFactory.getLog(ImportTextFile2HBase.class);

	/**
	 * Sets up the actual job.
	 * 
	 * @param conf
	 *          The current configuration.
	 * @param recordSpec
	 *          The command line parameters.
	 * @return The newly created job.
	 * @throws IOException
	 *           When setting up the job fails.
	 */
	public static Job createSubmittableJob(Configuration conf,
			TextRecordSpec recordSpec, HBaseTableSpec tableSpec, String inputDir,
			String outputDirStr) throws IOException, ClassNotFoundException {

	  int shufflePallel = 10;
	  shufflePallel = conf.getInt("mapreduce.reduce.shuffle.parallelcopies", shufflePallel);
	  conf.setInt("mapreduce.reduce.shuffle.parallelcopies", shufflePallel);
		String tableName = tableSpec.getTableName();
		String indexTableName = tableSpec.getIndexTableName();
		if ((indexTableName != null) && (!indexTableName.isEmpty())) {
			tableName = tableSpec.getIndexTableName();
		}
		String inputSize = conf.get(BConstants.BulkLoadProps.SPLIT_SIZE.getName());
		if (inputSize != null) {
			conf.set("mapred.max.split.size", inputSize);
			conf.set("mapred.min.split.size.per.rack", inputSize);
			conf.set("mapred.min.split.size.per.node", inputSize);
		}
		boolean isRemote = conf.getBoolean(
				BConstants.BulkLoadProps.IS_REMOTE.getName(), false);
		conf.setLong("reducer.row.threshold", 256 * 1024 * 1024);
		Job job = new Job(conf, NAME + "_" + tableName);
		job.setJarByClass(DataType.class);

		String inputFormat = conf.get(BConstants.BulkLoadProps.INPUT_FORMAT
				.getName());
		String indexTableList = conf.get(BConstants.BulkLoadProps.INDEXTABLE_LIST.getName(), "");
    
    if (!indexTableList.isEmpty()) {
      System.out.println("Use mapper : TextWithIndexMapper");
      job.setJarByClass(TextWithIndexMapper.class);
      FileInputFormat.setInputPaths(job, inputDir);
      if (inputFormat.equalsIgnoreCase(BConstants.InputFormat.COMBINE_FILE
          .getName())) {
        job.setMapperClass(TextMultiFileInputWithIndexMapper.class);
        job.setInputFormatClass(MultiFileInputFormat.class);
      } else {
        job.setMapperClass(TextWithIndexMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
      }
    } else {
      if (inputFormat != null) {
        System.out.println("Input format is : " + inputFormat);
        // If the input format is CombineFile
        if (inputFormat.equalsIgnoreCase(BConstants.InputFormat.COMBINE_FILE
            .getName())) {
          System.out.println("COMBINE INPUT");
          job.setJarByClass(TextRecord2HBaseRowMapperMultiFile.class);
          FileInputFormat.setInputPaths(job, inputDir);
          if (isRemote) {
            job.setInputFormatClass(MultiRemoteFileInputFormat.class);
            job.setMapperClass(TextRecord2HBaseRowMapperMultiFile.class);
          } else {
            job.setInputFormatClass(MultiFileInputFormat.class);
            job.setMapperClass(TextRecord2HBaseRowMapperMultiFile.class);
          }
        }
        // If the input format is MultiHFile
        else if (inputFormat
            .equalsIgnoreCase(BConstants.InputFormat.MULTI_HFILE.getName())) {
          job.setJarByClass(HFile2SecondaryIndexHFileMapper.class);
          job.setMapperClass(HFile2SecondaryIndexHFileMapper.class);
          job.setInputFormatClass(MultiHFileInputFormat.class);
        }
      }

      if (conf.getBoolean("bulkload.mapper.useput", false)) {
        System.out.println("Use mapper : PutWritableMapper");
        //job.setJarByClass(WazdMapper.class);
       // job.setMapperClass(WazdMapper.class);
        job.setJarByClass(PutWritableMapper.class);
        job.setMapperClass(PutWritableMapper.class);
      } else {
        System.out.println("Use mapper : TextMapper");
        job.setJarByClass(TextMapper.class);
        job.setMapperClass(TextMapper.class);
      }

      FileInputFormat.setInputPaths(job, inputDir);
      job.setInputFormatClass(TextInputFormat.class);
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
			if (!isNewTable) {
				// If this table has existed before, the HFileOutputFormat
				// should
				// use the configuration from this table
				HTableDescriptor tableDesc = table.getTableDescriptor();
				conf.set(HConstants.HREGION_MAX_FILESIZE, tableDesc.getMaxFileSize()
						+ "");
			}
			
			if (conf.getBoolean("bulkload.mapper.useput", false)) {
			  System.out.println("Use reducer : PutWritableSortReducer");
			  job.setReducerClass(PutWritableSortReducer.class);
			  job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(PutWritable.class);
			} else {
			  System.out.println("Use reducer : TextSortReducer");
			  job.setReducerClass(TextSortReducer.class);
			  job.setMapOutputKeyClass(ImmutableBytesWritable.class);
	      job.setMapOutputValueClass(Text.class);
			}
			
			Path outputDir = new Path(hfileOutPath);
			FileOutputFormat.setOutputPath(job, outputDir);
			if (indexTableList.isEmpty()) {
			  HFileOutputFormat2.configureIncrementalLoad(job, table, true);
			} else {
			  TranswarpHFileOutputFormat.configureIncrementalLoad(job, table);
			}
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
		TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
		    org.apache.hadoop.hbase.codec.prefixtree.PrefixTreeCodec.class);
		if (tableSpec.getLoadThemis()) {
		  TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
		      org.apache.hadoop.hbase.transaction.columns.ColumnUtil.class);
		}
    LOG.info("mapreduce.framework.name:" + conf.get("mapreduce.framework.name"));
		return job;
	}

	private static boolean doesTableExist(String tableName) throws IOException {
		return hbaseAdmin.tableExists(tableName.getBytes());
	}

	private static void createIndexTable(Configuration conf,
			HBaseTableSpec tableSpec) throws IOException {
		String indexTableName = tableSpec.getIndexTableName();
		HTableDescriptor idxDescriptor = new HTableDescriptor(indexTableName);
		idxDescriptor
			.addFamily(new HColumnDescriptor(Bytes.toBytes(conf
				.get(BConstants.BulkLoadProps.TARGET_COLUMN_FAMILY.getName())))
				.setMaxVersions(conf.getInt(HConstants.VERSIONS, 1))
				.setCompressionType(
					BulkLoadUtils.getCompressionTypeByString(conf
						.get(BConstants.HFileConf.COMPRESSION_CONF_KEY.getName())))
				.setBloomFilterType(
					BulkLoadUtils.getBloomTypeByString(conf
						.get(BConstants.BLOOMFILTER)))
				.setDataBlockEncoding(
					BulkLoadUtils.getDataBlockEncodingByString(conf
						.get(BConstants.HFileConf.DATABLOCK_ENCODING_CONF_KEY
							.getName())))
				.setBlockCacheEnabled(conf.getBoolean(BConstants.BLOCKCACHE, true)));
		if (!hbaseAdmin.tableExists(indexTableName)) {
			hbaseAdmin.createTable(idxDescriptor,
					getSplitKeys(tableSpec.getIndexSplitKeySpec()));
		} else {
			if (hbaseAdmin.isTableDisabled(indexTableName)) {
				hbaseAdmin.enableTable(indexTableName);
			}
		}
	}
	
	private static void createTable(Configuration conf, HBaseTableSpec tableSpec)
			throws IOException {

		HTableDescriptor htd = new HTableDescriptor(tableSpec.getTableName()
				.getBytes());
		Map<HBaseTableSpec.ColumnName, ColumnSpec> columnMap = tableSpec.columnMap;
		Set<String> cfSet = new LinkedHashSet<String>();
		for (HBaseTableSpec.ColumnName columnName : columnMap.keySet()) {
			cfSet.add(columnName.getFamily());
		}
		for (String cf : cfSet) {
			HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(cf))
					.setMaxVersions(conf.getInt(HConstants.VERSIONS, 1))
					.setCompressionType(
							BulkLoadUtils.getCompressionTypeByString(conf
									.get(BConstants.HFileConf.COMPRESSION_CONF_KEY.getName())))
					.setBloomFilterType(
							BulkLoadUtils.getBloomTypeByString(conf
									.get(BConstants.BLOOMFILTER)))
					.setDataBlockEncoding(
							BulkLoadUtils.getDataBlockEncodingByString(conf
									.get(BConstants.HFileConf.DATABLOCK_ENCODING_CONF_KEY
											.getName())))
					.setBlockCacheEnabled(conf.getBoolean(BConstants.BLOCKCACHE, true));
			int blocksize = conf.getInt(HFileConf.HFILEOUTPUTFORMAT_BLOCKSIZE.getName(), HConstants.DEFAULT_BLOCKSIZE);
			hcd.setBlocksize(blocksize);
			if (tableSpec.getLoadThemis()) {
			  hcd.setValue(HConstants.THEMIS_ENABLE_KEY, "true");
			}
			htd.addFamily(hcd);
		}
		
		if (tableSpec.getSplitKeySpec().isEmpty()) {
		  hbaseAdmin.createTable(htd);
		} else {
		  byte[][] splits = getSplitKeys(tableSpec.getSplitKeySpec());
	    hbaseAdmin.createTable(htd, splits);
		}
		
	}

	public static byte[][] getSplitKeys(String splitKeyStr) {
		LOG.info("Region endKeys:" + splitKeyStr);
		String[] keys = splitKeyStr.split(",");
		byte[][] results = new byte[keys.length][];
		for (int i = 0; i < keys.length; i++) {
			String s = keys[i];
			results[i] = Bytes.toBytes(s);
		}

		return results;
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
			System.out.println("Usage: " + NAME + " <configFile>\n");
			System.exit(1);
		}
		if (args.length == 2) {
			if (args[1].equalsIgnoreCase("-test")) {
				isTest = true;
			}
		}
		
		ImportTextFile2HBase.importData(args[0], isTest);
		
	}
	
	public static void importData(String path, boolean isTest) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");
    conf.addResource("mapred-site.xml");

    Properties props = new Properties();
    props.load(new FileInputStream(path));

    if (BulkLoadChecker.checkProps(props)) {
      System.out.println(BulkLoadChecker.hintInfo.toString());
    } else {
      System.out.println(BulkLoadChecker.errorInfo.toString());
      System.exit(1);
    }

    // Initialize the configuration for bulk load
    // Use the default value if the user don't set them in the
    // conf.properties
    initHBaseConf(conf, props);
    initHFileConf(conf, props);
    initBulkProps(conf, props);
    initBulkConf(conf);

    if (!isTest) {
      hbaseAdmin = new HBaseAdmin(conf);
      runBulkLoad(conf);
    } else {
      System.err.println("Check conf succ.");
    }
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
		
		HFileConf[] confs = HFileConf.values();
		for (HFileConf c : confs) {
		  String value = BConstants.DEFAULT_HFILE_CONF.get(c.getName());
      System.err.println("HFileProps " + c.getName() + " : " + value);
		}
	}
	
	private static void initBulkProps(Configuration conf, Properties props) {
	  BulkLoadProps[] confs = BulkLoadProps.values();
	  for (BulkLoadProps BulkLoadProps : confs) {
	    String confValue = props.getProperty(BulkLoadProps.getName());
	    if ((confValue) != null && (!confValue.isEmpty())) {
	      BConstants.BULKLOAD_PROPS.put(BulkLoadProps.getName(), confValue);
	    }
	  }
	  
	  // debug
	  for (BulkLoadProps BulkLoadProps : confs) {
	    String value = BConstants.BULKLOAD_PROPS.get(BulkLoadProps.getName());
	    System.err.println("BulkLoadProps " + BulkLoadProps.getName() + " : " + value);
    }
	   for (Object str : props.keySet()) {
	      if (str instanceof String) {
	        if((props.getProperty((String)str) !=null) && (!props.getProperty((String)str).isEmpty())){
	          conf.set((String)str, props.getProperty((String)str));
	          System.out.println(str + " : " + props.getProperty((String) str));
	        } 
	      }
	   }
	}
	
	private static void initBulkConf(Configuration conf) {
	  for (Map.Entry<String, String> e : BConstants.BULKLOAD_PROPS.entrySet()) {
	    conf.set(e.getKey(), e.getValue());
	  }
	}
	
	private static void runJob(HBaseTableSpec tableSpec, TextRecordSpec recordSpec, Configuration conf)
	    throws ClassNotFoundException, IOException, InterruptedException {
	  // ========= Start to run the Job =======================
    long start = System.currentTimeMillis();
    String inputDirStr = conf.get(BConstants.BulkLoadProps.INPUT_DIR.getName());
    String outPutDirStr = conf.get(BConstants.BulkLoadProps.OUTPUT_DIR.getName());
    // Set the timeout seconds
    conf.set("mapred.task.timeout", MAPRED_TASK_TIMEOUT);
    Job job = createSubmittableJob(conf, recordSpec, tableSpec, inputDirStr,
        outPutDirStr);
    LOG.info("mapreduce.framework.name in jobconf:" + job.getConfiguration().get("mapreduce.framework.name"));
    System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");

    System.out.println("CONF:"+conf.toString());
    
    System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
    
    // LOG.info("init job state:" + job); 
    job.submit();
    LOG.info("submitted job state:" + job); 
    job.waitForCompletion(true);
  
    System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");

        System.out.println("CONF:"+conf.toString());

            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++");
    long end = System.currentTimeMillis();
    // ============= End of the Job =====================

    System.out.println("Import File to HBase Costs: " + (end - start) / 1000
        + "s");
	}
	
	private static void runBulkLoad(Configuration conf) throws Exception {
	  String textRecordSpec = conf.get(BConstants.BulkLoadProps.TEXT_RECORD_SPEC.getName());
    String fieldDelimiter = conf.get(BConstants.BulkLoadProps.FIELD_DELIMITER.getName());
    String internalFieldDelimiter = conf.get(BConstants.BulkLoadProps.INTERNAL_FIELD_DELIMITER.getName());
    String fieldNameTypeDelimiter = conf.get(BConstants.BulkLoadProps.FIELD_NAME_TYPE_DELIMITER.getName());
    String encoding = conf.get(BConstants.BulkLoadProps.ENCODING.getName(), "UTF-8");
    String rowSpec = conf.get(BConstants.BulkLoadProps.ROW_SPEC.getName());
    String splitKeySpec = conf.get(BConstants.BulkLoadProps.SPLIT_KEY_SPEC.getName());
    String tableName = conf.get(BConstants.BulkLoadProps.TABLE_NAME.getName());
    String indexTableName = conf.get(BConstants.BulkLoadProps.INDEX_TABLE_NAME.getName());
    String indexSplitKeySpec = conf.get(BConstants.BulkLoadProps.INDEX_SPLIT_KEY_SPEC.getName());
    
	  TextRecordSpec recordSpec = new TextRecordSpec(textRecordSpec, encoding,
        fieldDelimiter, fieldNameTypeDelimiter);

    // Set the table spec based on the properties
    HBaseTableSpec tableSpec = null;
    tableSpec = new HBaseTableSpec(tableName, rowSpec, splitKeySpec, recordSpec,
        HBaseTableSpec.EXTERNAL_COLUMN_DELIMITER, internalFieldDelimiter,
        indexTableName, indexSplitKeySpec);
    boolean loadThemis = conf.getBoolean(BulkLoadProps.LOADTHEMIS.getName(), false);
    tableSpec.setLoadThemis(loadThemis);

    // Judge whether the hbase table has existed
    if (!doesTableExist(tableName)) {
      System.out
        .println(tableName + " doesn't exist. We will create it first.");
      createTable(conf, tableSpec);
      isNewTable = true;
    }
    
    // Create the index table if needed
    if ((indexTableName != null) && (!indexTableName.isEmpty())) {
      if (!doesTableExist(indexTableName)) {
        createIndexTable(conf, tableSpec);
      }
    }
    
    runJob(tableSpec, recordSpec, conf);
	}
}
