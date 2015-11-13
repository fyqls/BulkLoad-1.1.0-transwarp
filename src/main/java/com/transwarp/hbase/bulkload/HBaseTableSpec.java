package com.transwarp.hbase.bulkload;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import com.transwarp.hbase.bulkload.common.ExpressionUtils;

public class HBaseTableSpec {

	public static final String COLUMN_FAMILY_NAME = "f";
	public static final byte[] COLUMN_FAMILY_NAME_BYTES = Bytes
			.toBytes(COLUMN_FAMILY_NAME);
	public static final String EXTERNAL_COLUMN_DELIMITER = ";";
	public static final String COLUMN_KEY_VALUE_DELIMITER = ":";

	public static final String ROW_KEY_NAME = "rowKey";
	
	public static final String DEFAUL_INTERNAL_COLUMN_DELIMITER = "|";

	private String tableName;
	private String indexTableName;
	private String specString;
	private String splitKeySpec;
	private String indexSplitKeySpec;

	private String externalColumnDelimiter;
	private String internalColumnDelimiter;

	private String escapedExternalColumnDelimiter;
	private String escapedInternalColumnDelimiter;

	ColumnSpec rowKeySpec;
	
	private boolean rowkeyAutoIncreasing = false;
	private boolean emptyStringAsNull = false;
	private boolean useHyperbaseDataType = false;
	private boolean loadThemis = false;

	public Map<ColumnName, ColumnSpec> columnMap = new HashMap<ColumnName, ColumnSpec>();

	public class ColumnName {
	  private String family;
	  private String qualifier;
	  private String fullName;
	  
	  public ColumnName(String name) {
	    String[] cfColumnArray = name.split(ExpressionUtils
          .getEscapedDelimiter(ImportTextFile2HBase.CF_C_DELIMITER));
	    this.family = cfColumnArray[0];
	    if (cfColumnArray.length == 2) {
	      this.qualifier = cfColumnArray[1];
	    }
	    this.fullName = name;
	  }
	  
	  public String getFamily() {
	    return family;
	  }
	  
	  public String getQualifier() {
	    return qualifier;
	  }
	  
	  public String getFullName() {
	    return fullName;
	  }
	  
	  @Override
	  public int hashCode() {
	    return this.fullName.hashCode();
	  }
	}
	
	public HBaseTableSpec(String tableName, String formatSpecString,
			String splitKeys, TextRecordSpec recordSpec) throws Exception {
		this(tableName, formatSpecString, splitKeys, recordSpec, EXTERNAL_COLUMN_DELIMITER);
	}

	public HBaseTableSpec(String tableName, String specString,
			String splitKeys, TextRecordSpec recordSpec,
			String columnDelimiter) throws Exception {
		this(tableName, specString, splitKeys, recordSpec, columnDelimiter,
				DEFAUL_INTERNAL_COLUMN_DELIMITER);
	}

	public HBaseTableSpec(String tableName, String specString,
			String splitKeySpec, TextRecordSpec recordSpec,
			String externalColumnDelimiter, String internalColumnDelimiter) throws Exception {
		this(tableName, specString, splitKeySpec, recordSpec, 
				externalColumnDelimiter, internalColumnDelimiter, null, null);
	}

	public HBaseTableSpec(String tableName, String specString,
			String splitKeySpec, TextRecordSpec recordSpec, 
			String externalColumnDelimiter,	String internalColumnDelimiter, 
			String indexTableName,	String indexSplitKeySpec) throws Exception {
		this.tableName = tableName;
		this.specString = specString;
		this.splitKeySpec = splitKeySpec;
		this.externalColumnDelimiter = externalColumnDelimiter;
		this.internalColumnDelimiter = StringUtils
				.isEmpty(internalColumnDelimiter) ? DEFAUL_INTERNAL_COLUMN_DELIMITER
				: internalColumnDelimiter;
		this.escapedExternalColumnDelimiter = ExpressionUtils
				.getEscapedDelimiter(externalColumnDelimiter);
		this.escapedInternalColumnDelimiter = ExpressionUtils
				.getEscapedDelimiter(this.internalColumnDelimiter);
		this.indexTableName = indexTableName;
		this.indexSplitKeySpec = indexSplitKeySpec;
		intilizeFieldSpecs(recordSpec);
	}

	public ColumnSpec getRowKeySpec() {
		return rowKeySpec;
	}

	public String getTableName() {
		return tableName;
	}

	public String getIndexTableName() {
		return indexTableName;
	}

	public String getInternalColumnDelimiter() {
		return internalColumnDelimiter;
	}

	public String getSpecString() {
		return specString;
	}

	public String getSplitKeySpec() {
		return splitKeySpec;
	}
	public String getIndexSplitKeySpec() {
		return indexSplitKeySpec;
	}

	public String getEscapedExternalColumnDelimiter() {
		return escapedExternalColumnDelimiter;
	}

	public String getEscapedInternalColumnDelimiter() {
		return escapedInternalColumnDelimiter;
	}

	public String toString() {
		return tableName + "-" + rowKeySpec.toString() + "-"
				+ columnMap.values().toString();
	}
	
	public void setRowkeyAutoIncreasing(boolean b) {
	  this.rowkeyAutoIncreasing = b;
	}
	
	public void setEmptyStringAsNull(boolean b) {
	  this.emptyStringAsNull = b;
	}
	
	public boolean getRowkeyAutoIncreasing() {
    return this.rowkeyAutoIncreasing;
  }
  
  public boolean getEmptyStringAsNull() {
    return this.emptyStringAsNull;
  }
  
  public void setUseHyperbaseDataType(boolean b) {
    this.useHyperbaseDataType = b;
  }
  
  public boolean getUseHyperbaseDataType() {
    return this.useHyperbaseDataType;
  }
  
  public void setLoadThemis(boolean b) {
    this.loadThemis = b;
  }
  
  public boolean getLoadThemis() {
    return this.loadThemis;
  }

	private void intilizeFieldSpecs(TextRecordSpec recordSpec) throws Exception {
		String normalized = StringUtils.deleteWhitespace(specString);
		String[] columnSpecs = normalized.split(ExpressionUtils
				.getEscapedDelimiter(externalColumnDelimiter));

		// column 0 is always rowKey spec.
		if (columnSpecs.length <= 1)
			throw new Exception("Invalid htable spec: No column found in "
					+ specString);

		ColumnSpec column = null;
		String escapedDelimiter = ExpressionUtils
				.getEscapedDelimiter(COLUMN_KEY_VALUE_DELIMITER);
		for (String columnSpec : columnSpecs) {
			String[] ss = columnSpec.split(escapedDelimiter, 2);
			if (ss.length != 2)
				throw new Exception("Invalid htable spec: Invalid column spec "
						+ columnSpec);

			column = new ColumnSpec(ss[0], ss[1], recordSpec.getFieldMap());
			if (ss[0].equalsIgnoreCase(ROW_KEY_NAME)) {
				rowKeySpec = column;
				continue;
			}
			columnMap.put(new ColumnName(ss[0]), column);
		}

		if (rowKeySpec == null)
			throw new Exception("No rowKey specification is found!");
	}
}
