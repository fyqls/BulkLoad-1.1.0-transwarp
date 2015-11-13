package com.transwarp.hbase.bulkload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.transwarp.hbase.bulkload.common.ExpressionUtils;


public class TextRecordSpec{
	
	public static final String DEFAULT_FIELD_DELIMITER = ",";
	public static final String DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER = ":";
	public static final String DEFAULT_TEXT_ENCODING = "UTF-8";
	
	private String fieldDelimiter;
	private String fieldNameTypeValueDelimiter;
	
	private String escapedFieldDelimiter;
	private String escapedFieldNameTypeValueDelimiter;
	
	private String formatSpecString;
	private String encoding;
	
	List<FieldSpec> fieldList = new ArrayList<FieldSpec>();

	Map<String, FieldSpec> fieldMap = new HashMap<String, FieldSpec>();
	
	public static class FieldSpec {
		
		public static String DEFAULT_FIELD_TYPE = "STRING";
		
		private int fieldIndex;
		private String fieldName;
		private String fieldType;
		
		public FieldSpec(int fieldIndex, String fieldName, String fieldType) {
			this.fieldIndex = fieldIndex;
			this.fieldName = fieldName;
			this.fieldType = fieldType.toUpperCase();
		}
		
		public int getFieldIndex(){
			return fieldIndex;
		}
		
		public String getFieldName() {
			return fieldName;
		}
		public String getFieldType() {
			return fieldType;
		}
		
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(fieldIndex).append(DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER).append(fieldName).append(DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER).append(fieldType);
			return sb.toString();
		}
	}

	public TextRecordSpec(String formatSpecString) {
		this(formatSpecString, DEFAULT_TEXT_ENCODING, DEFAULT_FIELD_DELIMITER);
	}
	
	public TextRecordSpec(String formatSpecString, String encoding, String delimiterBetweenFields) {
    this(formatSpecString, encoding, delimiterBetweenFields, DEFAULT_FIELD_NAME_TYPE_VALUE_DELIMITER);
  }
	
	public TextRecordSpec(String formatSpecString, String encoding, String fieldDelimiter,
	    String fieldNameTypeValueDelimiter) {
		this.formatSpecString = formatSpecString;
		this.encoding = encoding;
		this.fieldDelimiter = fieldDelimiter;
		this.fieldNameTypeValueDelimiter = fieldNameTypeValueDelimiter;
		this.escapedFieldDelimiter = ExpressionUtils.getEscapedDelimiter(fieldDelimiter);
		this.escapedFieldNameTypeValueDelimiter = ExpressionUtils.getEscapedDelimiter(fieldNameTypeValueDelimiter);
		intilizeFieldSpecs();
	}
	
	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	public String getFieldNameTypeValueDelimiter() {
		return fieldNameTypeValueDelimiter;
	}
	
	public String getEscapedFieldDelimiter() {
		return escapedFieldDelimiter;
	}

	public String getEscapedFieldNameTypeValueDelimiter() {
		return escapedFieldNameTypeValueDelimiter;
	}
	
	public String getFormatSpecString() {
		return formatSpecString;
	}
	
	public String getEncoding() {
		return encoding;
	}
	
	public List<FieldSpec> getFieldList() {
		return fieldList;
	}

	public Map<String, FieldSpec> getFieldMap() {
		return fieldMap;
	}
	
	public String toString(){
		return fieldList.toString();
	}
	
	private void intilizeFieldSpecs(){
		String normalized = StringUtils.deleteWhitespace(formatSpecString);
		// Use "," to split field spec
		String[] fieldSpecs = normalized.split(",");
		
		int index = -1;
		FieldSpec field = null;
		for(String fieldSpec : fieldSpecs){
			
			index++;			
			String[] ss = fieldSpec.split(escapedFieldNameTypeValueDelimiter);
			if(ss.length == 1) {
				field = new FieldSpec(index, ss[0], FieldSpec.DEFAULT_FIELD_TYPE);
			}else if(ss.length == 2) {
				field = new FieldSpec(index, ss[0], ss[1]);			
			}
			
			fieldList.add(field);
			fieldMap.put(ss[0], field);
		}
	}
}
