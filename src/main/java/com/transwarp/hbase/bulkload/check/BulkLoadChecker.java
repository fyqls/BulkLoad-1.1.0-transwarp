package com.transwarp.hbase.bulkload.check;

import java.util.Properties;

import org.apache.hadoop.fs.Path;

import com.transwarp.hbase.bulkload.common.BConstants;

public class BulkLoadChecker {
	/**
	 * 
	 * @return
	 * @throws IOException
	 */
	public static StringBuilder errorInfo = new StringBuilder();
	public static StringBuilder hintInfo = new StringBuilder();
	private final static String ERROR_PREFIX = "ERROR: ";

	private static String errorInfoPrefix = "";

	public static boolean checkProps(Properties props) {
		errorInfoPrefix = ERROR_PREFIX;
		checkTableName(props);
		checkInputDir(props);
		checkOutputDir(props);
		checkTextRecordSpec(props);
		checkIndexTableName(props);
		//checkIsRemote(props);
		if (errorInfo.toString().isEmpty()) {
			return true;
		} else {
			return false;
		}
	}

	private static boolean checkTableName(Properties props) {
		String propertyName = BConstants.BulkLoadProps.TABLE_NAME.getName();
		String tableName = props.getProperty(propertyName);

		if (tableName == null || tableName.isEmpty()) {
			errorInfo.append(errorInfoPrefix + "You must specify the property \""
					+ propertyName + "\".\n");
			return false;
		}
		return true;
	}

	private static boolean checkInputDir(Properties props) {
		String propertyName = BConstants.BulkLoadProps.INPUT_DIR.getName();
		String inputDir = props.getProperty(propertyName);

		if (inputDir == null || inputDir.isEmpty()) {
			errorInfo.append(errorInfoPrefix + "You must specify the property \""
					+ propertyName + "\".\n");
			return false;
		}
		return true;
	}

	private static boolean checkOutputDir(Properties props) {
		String propertyName = BConstants.BulkLoadProps.OUTPUT_DIR.getName();
		String outputDir = props.getProperty(propertyName);

		if (outputDir == null || outputDir.isEmpty()) {
			errorInfo.append(errorInfoPrefix + "You must specify the property \""
					+ propertyName + "\".\n");
			return false;
		}
		return true;
	}

	private static boolean checkTextRecordSpec(Properties props) {
		String textRecordSpec = props
				.getProperty(BConstants.BulkLoadProps.TEXT_RECORD_SPEC.getName());
		String[] recordFields = textRecordSpec.split(",");
		hintInfo
				.append("The size of the fields which you specify in the source file will be: "
						+ recordFields.length + ".\n");
		return true;
	}

	private static boolean checkIndexTableName(Properties props) {
		String propertyName = BConstants.BulkLoadProps.INDEX_TABLE_NAME.getName();
		String indexTableName = props.getProperty(propertyName);
		String inputFormat = props.getProperty(BConstants.BulkLoadProps.INPUT_FORMAT
				.getName());
		if (indexTableName == null || indexTableName.isEmpty()) {
			if (inputFormat.equalsIgnoreCase(BConstants.InputFormat.MULTI_HFILE
					.getName())) {
				errorInfo.append(errorInfoPrefix
						+ "When you use the multi hfile to generate the index table, " +
						"you must specify the property \""
						+ propertyName + "\".\n");
				return false;
			}
		}
		if (indexTableName != null && !indexTableName.isEmpty()) {
			if (!inputFormat.equalsIgnoreCase(BConstants.InputFormat.MULTI_HFILE
					.getName())) {
				errorInfo
						.append(errorInfoPrefix + "If you don't use the MultiHFile to generate the index table, "
								+ "please don't set the property \"" + propertyName + "\".\n");
				return false;
			}
		}
		return true;
	}

	private static boolean checkIsRemote(Properties props) {
		// Check the input path and output path to judge
		// whether it load the data from the remote cluster or not.
		// If so, set the isRemote as true or false
		String isRemoteOrgStr = props.getProperty(
				BConstants.BulkLoadProps.IS_REMOTE.getName(), "false");
		boolean isRemoteOrg = Boolean.getBoolean(isRemoteOrgStr);
		boolean isRemote = isRemoteOrg;
		Path inputDir = new Path(
				props.getProperty(BConstants.BulkLoadProps.INPUT_DIR.getName()));
		Path outputDir = new Path(
				props.getProperty(BConstants.BulkLoadProps.OUTPUT_DIR.getName()));
		if ((inputDir.toUri().getScheme().equalsIgnoreCase(outputDir.toUri()
				.getScheme()))
				&& (inputDir.toUri().getHost().equalsIgnoreCase(outputDir.toUri()
						.getHost()))
				&& (inputDir.toUri().getPort() == outputDir.toUri().getPort())) {
			// The two schemes are the same, it is not the remote
			isRemote = false;
		} else {
			// The two schemes are not same, it is remote
			isRemote = true;
		}
		// ========== End the checking of the input/output path =========

		if (isRemoteOrg != isRemote) {
			errorInfo.append("ERROR: The isRemote property is actually \""
					+ isRemote + "\", while your setting in the configuration file is \""
					+ isRemoteOrg + "\"\n");
			return false;
		}

		return true;
	}

}
