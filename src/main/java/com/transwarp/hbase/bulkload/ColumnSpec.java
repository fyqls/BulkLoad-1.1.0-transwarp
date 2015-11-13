package com.transwarp.hbase.bulkload;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.transwarp.hbase.bulkload.common.BConstants.Function;
import com.transwarp.hbase.bulkload.common.BConstants.ParamType;

public class ColumnSpec {

	private String columnName;
	private String specString;
	private ArrayList<Operation> operationList;
	private Map<String,TextRecordSpec.FieldSpec> fieldMap;
	
	private static String resultMark = "__RESULT__";

	public ColumnSpec(String columnName, String specString, Map<String,TextRecordSpec.FieldSpec> fieldMap) {
		this.columnName = columnName;
		this.specString = specString;
		this.operationList = new ArrayList<Operation>();
		this.fieldMap = fieldMap;
		genOperationList(specString);
	}

	private void genOperationList(String columnSpec) {
		int fieldEnd = columnSpec.indexOf(")");
		if (fieldEnd == -1) {
			return;
		}
		else {
			int fieldBegin = columnSpec.lastIndexOf("(", fieldEnd);
			
			String[] fields = columnSpec.substring(fieldBegin+1, fieldEnd).split(",");
			Parameter[] params = new Parameter[fields.length];
			
			for(int i=0; i < fields.length; i++) {
				
				if (fields[i].startsWith("\"")) {
					params[i] = new Parameter(ParamType.CONST, fields[i].replace("\"", ""));
				} else if (fields[i].startsWith(resultMark)) {
					params[i] = new Parameter(ParamType.RESULT, fields[i].substring(resultMark.length()));
				} else if (fields[i].contains("#")) {
					String[] fStrs = fields[i].split("#");
					
					Parameter[] astypeParams = new Parameter[2];
					int fIndex = fieldMap.get(fStrs[0]).getFieldIndex();
					astypeParams[0] = new Parameter(ParamType.FIELD, String.valueOf(fIndex));
					astypeParams[1] = new Parameter(ParamType.CONST, fStrs[1]);
					
					operationList.add(new Operation("ASTYPE", astypeParams));
					params[i] = new Parameter(ParamType.RESULT, String.valueOf(operationList.size()-1));
				} else if (StringUtils.isNumeric(fields[i])) {
					params[i] = new Parameter(ParamType.CONST, fields[i]);
				} else {
					int fIndex = fieldMap.get(fields[i]).getFieldIndex();
					params[i] = new Parameter(ParamType.FIELD, String.valueOf(fIndex));
				}
			}
			
			// Get function name.
			int funcBegin = 0;
			for (int index = fieldBegin-1; index >= 0; index--) {
				char ch = columnSpec.charAt(index);
				if ((ch == '(') || (ch == ',')) {
					funcBegin = index+1;
					break;
				}
			}
			
			String func = columnSpec.substring(funcBegin, fieldBegin);
			operationList.add(new Operation(func, params));
			
			StringBuffer newSpec = new StringBuffer();
			newSpec.append(columnSpec.substring(0, funcBegin));
			// mark operation result with this.resultMark.
			newSpec.append(resultMark);
			newSpec.append(operationList.size()-1);
			newSpec.append(columnSpec.substring(fieldEnd+1, columnSpec.length()));
			genOperationList(newSpec.toString());
		}
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public String getSpecString() {
		return specString;
	}
	
	public ArrayList<Operation> getOperationList() {
		return operationList;
	}
	
	public String toString() {
//		return columnName + ":" + specString;
		String str =  "";
		for (Operation op : operationList) {
			str += op.toString()+",";
		}
		return str;
	}
	
	public static class Operation {
		private Function func;
		private Parameter[] params;
		
		public Operation(String funcName, Parameter[] fields) {
			this.func = Function.valueOf(funcName.toUpperCase());
			this.params = fields;
		}
		
		public Function getFunction() {
			return this.func;
		}
		
		public Parameter[] getParams() {
			return this.params;
		}
		
		public String toString() {
			StringBuffer sb = new StringBuffer(func.toString());
			sb.append(':');
			sb.append(params[0].getValue());
			for (int i=1; i < params.length; i++) {
				sb.append(",");
				sb.append(params[i].getValue());
			}
			return sb.toString();
		}
	}
	
	public static class Parameter {
		private ParamType type;
		private String value;
		
		public Parameter(ParamType type, String value) {
			this.type = type;
			this.value = value;
		}
		
		public ParamType getType() {
			return this.type;
		}
		
		public String getValue() {
			return this.value;
		}
	}
}
