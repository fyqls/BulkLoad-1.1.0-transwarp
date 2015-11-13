package com.transwarp.hbase.bulkload;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.datatype.DataType;
import org.apache.hadoop.hbase.transaction.columns.ColumnUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.liantong.CUConvertTools;
import com.transwarp.hbase.bulkload.ColumnSpec;
import com.transwarp.hbase.bulkload.ColumnSpec.Operation;
import com.transwarp.hbase.bulkload.ColumnSpec.Parameter;
import com.transwarp.hbase.bulkload.common.BConstants.Function;
//import com.transwarp.hbase.bulkload.common.BConstants.ValueType;
import com.transwarp.hbase.bulkload.common.ExpressionUtils;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class TextRecord2HBaseRowConverter {
	
	private TextRecordSpec recordSpec;
	
	private HBaseTableSpec rowSpec;

  public HBaseTableSpec getRowSpec() {
    return rowSpec;
  }
	
	// Index table rowkey format
	HashMap<String, Pair<ColumnSpec, ColumnSpec>> indexTableRowkeySpecs = new HashMap<String, Pair<ColumnSpec, ColumnSpec>>();
	
	private long timeStamp;
	
	private final int INIT_ALLOC_SIZE = 10;
  private byte[][] operationResults = new byte[INIT_ALLOC_SIZE][];
	
	public TextRecord2HBaseRowConverter(TextRecordSpec recordSpec,
			HBaseTableSpec rowSpec) throws Exception {
		this.recordSpec = recordSpec;
		this.rowSpec = rowSpec;
	}
	
	public TextRecord2HBaseRowConverter(TextRecordSpec recordSpec,
      HBaseTableSpec rowSpec, HashMap<String, Pair<ColumnSpec, ColumnSpec>> indexTableRowkeySpecs) {
    this.recordSpec = recordSpec;
    this.rowSpec = rowSpec;
    this.indexTableRowkeySpecs = indexTableRowkeySpecs;
  }
	
	
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	
	public TextRecordSpec getRecordSpec() {
		return recordSpec;
	}
	
	public HBaseTableSpec getTableSpec() {
		return rowSpec;
	}
	
	private byte[] typeToBytes(String val, String typeStr) throws IOException {
    try {
      DataType type = null;
      String TYPE = typeStr.toUpperCase();
      int fixedLen = -1;
      if (TYPE.startsWith(DataType.VARCHAR.name())) {
        type = DataType.VARCHAR;
        fixedLen = Integer.parseInt(TYPE.substring(DataType.VARCHAR.name().length()));
      } else {
        type = DataType.valueOf(TYPE);
      }
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      if (val.equals("") && !this.rowSpec.getEmptyStringAsNull()) {
        // ignore empty value
        return new byte[0];
      }
      switch (type) {
      case BOOLEAN:
        type.encode(val.equals("") ? null : Boolean.valueOf(val), stream, fixedLen);
        break;
      case SHORT:
        type.encode(val.equals("") ? null : Short.valueOf(val), stream, fixedLen);
        break;
      case INTEGER:
        type.encode(val.equals("") ? null : Integer.valueOf(val), stream, fixedLen);
        break;
      case LONG:
        type.encode(val.equals("") ? null : Long.valueOf(val), stream, fixedLen);
        break;
      case FLOAT:
        type.encode(val.equals("") ? null : Float.valueOf(val), stream, fixedLen);
        break;
      case DOUBLE:
        type.encode(val.equals("") ? null : Double.valueOf(val), stream, fixedLen);
        break;
      case KEY_STRING:
        type.encode(val.equals("") ? null : val, stream, fixedLen);
        break;
      case VALUE_STRING:
        type.encode(val.equals("") ? null : val, stream, fixedLen);
        break;
      case VARCHAR:
        type.encode(val.equals("") ? null : val, stream, fixedLen);
        break;
      default:
        stream.write(Bytes.toBytes(val));
      }
      return stream.toByteArray();
    } catch (IOException e) {
      e.printStackTrace();
      throw e;
    } 
	}
	
	public static String trimByChar(String str, char c) {
    int begin = 0;
    int end = str.length();
    while (begin < end) {
      if (str.charAt(begin) == c) {
        ++begin;
      } else {
        break;
      }
    }
    while (begin < end) {
      if (str.charAt(end - 1) == c) {
        --end;
      } else {
        break;
      }
    }
    return str.substring(begin, end);
	}

  private byte[] funcCalculatewithByte(Function func,byte[][] paramList)
      throws FormatException, IOException {

    switch (func) {
      case TRIM: {
        //byte[] tmpValue= paramList[0];
        String tmpValue=Bytes.toString(paramList[0]);
        return Bytes.toBytes(tmpValue.trim());
      }
      case LENGTH: {
        String tmpValue=Bytes.toString(paramList[0]);
        return Bytes.toBytes(tmpValue.length() + "");
      }
      case SUBSTRFORMAT: {
        String tmpValue=Bytes.toString(paramList[0]);
        int length = Integer.parseInt(Bytes.toString(paramList[2]));
        if (tmpValue.length() > length) {
          return Bytes.toBytes(tmpValue.substring((tmpValue.length() - length)));
        } else {
          return paramList[0];
        }
      }
      case STRINGFORMAT: {
        byte[] concat = new byte[0];
        if (paramList[0].length == 0) {
          concat = Bytes.toBytes("0000");
        } else if (paramList[0].length == 1) {
          concat = Bytes.toBytes("000");
        } else if (paramList[0].length == 2) {
          concat = Bytes.toBytes("00");
        } else if (paramList[0].length == 3) {
          concat = Bytes.toBytes("0");
        }
        return Bytes.add(concat, paramList[0]);
      }
      case TO_SHORT_B: {
        String tmpValue=Bytes.toString(paramList[0]);
        short i = Short.parseShort(tmpValue);
        return Bytes.toBytes(i);
      }
      case TO_INT_B: {
        String tmpValue=Bytes.toString(paramList[0]);
        int i = Integer.parseInt(tmpValue);
        return Bytes.toBytes(i);
      }
      case TO_LONG_B: {
        String tmpValue=Bytes.toString(paramList[0]);
        long i = Long.parseLong(tmpValue);
        return Bytes.toBytes(i);
      }
      case SHORT_B_TO_ST: {
        short l = Bytes.toShort(paramList[0]);
        return Bytes.toBytes(l + "");
      }
      case INT_B_TO_ST: {
        int l = Bytes.toInt(paramList[0]);
        return Bytes.toBytes(l + "");
      }
      case LONG_B_TO_ST: {
        long l = Bytes.toLong(paramList[0]);
        return Bytes.toBytes(l + "");
      }
      case TRIM_QUOTATION : {
        String tmpValue=Bytes.toString(paramList[0]);
        String tmp = trimByChar(tmpValue, '"');
        return Bytes.toBytes(tmp);
      }
      case CONCAT: {
        String tmpValue=Bytes.toString(paramList[0]);
        StringBuffer sb = new StringBuffer(tmpValue);
        for (int i = 1; i < paramList.length; i++) {
          sb.append(rowSpec.getInternalColumnDelimiter());
          sb.append(Bytes.toString(paramList[i]));
        }
        return Bytes.toBytes(sb.toString());
//           byte[] tmpValue = paramList[0];
//           for (int i = 1; i < paramList.length; i++) {
//            // rowSpec.getInternalColumnDelimiter()
//             tmpValue = Bytes.add(tmpValue, paramList[0]);
//           }
//           return tmpValue;
      }
      // RPAD, LPAD, SUBSTR, CONCAT, REVERSE, RAND, CONST
      case RPAD: {
        String targetStr = Bytes.toString(paramList[0]);
        int count = Integer.valueOf(Bytes.toString(paramList[1]));
        if (paramList.length == 3) {
          String result =  StringUtils.rightPad(targetStr, count, Bytes.toString(paramList[1]));
          return Bytes.toBytes(result);
        } else {
          String result = StringUtils.rightPad(targetStr, count);
          return Bytes.toBytes(result);
        }
      }
      case LPAD: {
        String targetStr = Bytes.toString(paramList[0]);
        int count = Integer.valueOf(Bytes.toString(paramList[1]));
        if (paramList.length == 3) {
          String result = StringUtils.leftPad(targetStr, count, Bytes.toString(paramList[2]));
          return Bytes.toBytes(result);
        } else {
          String result = StringUtils.leftPad(targetStr, count);
          return Bytes.toBytes(result);
        }
      }
      case SUBSTR: {
        String targetStr = Bytes.toString(paramList[0]);
        int begin = Integer.valueOf(Bytes.toString(paramList[1]));
        if (paramList.length == 3) {
          if (targetStr.length() >= Integer.valueOf(Bytes.toString(paramList[2]))) {
            String result = targetStr.substring(begin, Integer.valueOf(Bytes.toString(paramList[2])));
            return Bytes.toBytes(result);
          } else {
            String result = targetStr.substring(begin);
            return Bytes.toBytes(result);
          }

        } else {
          String result = targetStr.substring(begin);
          return Bytes.toBytes(result);
        }
      }
      case REVERSE: {
        String result = StringUtils.reverse(Bytes.toString(paramList[0]));
        return Bytes.toBytes(result);
      }
      case RAND: {
        int count = Integer.parseInt(Bytes.toString(paramList[0]));
        String result = RandomStringUtils.randomAlphanumeric(count);
        return Bytes.toBytes(result);
      }
	       /*
	       case ASTYPE: {
	         return typeToString(paramList[0], paramList[1]);
	       }
	       */
      case NOTNULL : {
        if(Bytes.toString(paramList[0]).isEmpty())
          throw new FormatException("Null Field!");
        String result = Bytes.toString(paramList[0]);
        return Bytes.toBytes(result);
      }
      case REPLACE : {
        if (paramList.length == 3) {
          String result = Bytes.toString(paramList[0]).replaceAll(Bytes.toString(paramList[1]),Bytes.toString(paramList[2]));
          return Bytes.toBytes(result);
        } else {
          throw new FormatException("Not 3 parameters!");
        }

      }
      case LIANTONG_TRANSFERURL : {
        if (paramList.length == 1) {
          String converted = CUConvertTools.convertURL(Bytes.toString(paramList[0]));
          int length = Math.min(converted.length(), 80);
          String result = converted.substring(0, length);
          return Bytes.toBytes(result);
        } else {
          throw new FormatException("LIANTONG_DINGZHI must 1 parameters!");
        }
      }
      case LIANTONG_TRANSFERIP : {
        if (paramList.length == 1) {
          String result = CUConvertTools.convertIP(Bytes.toString(paramList[0]));
          return Bytes.toBytes(result);
        } else {
          throw new FormatException("LIANTONG_DINGZHI must 1 parameters!");
        }
      }
      case REMOVE_WHITESPACE : {
        if (paramList.length == 1) {
          String result = Bytes.toString(paramList[0]).replaceAll(" ", "");
          return Bytes.toBytes(result);
        } else {
          throw new FormatException("LIANTONG_DINGZHI must 1 parameters!");
        }
      }
      case DATETOMILLTIME: {
        long time = transintoTimeStamp(Bytes.toString(paramList[0]),"yyyy-MM-dd HH:mm:ss.SSS");
        return Bytes.toBytes(time);
      }
      case TOTIMESTAMP: {
        long time = transintoTimeStamp2(Bytes.toString(paramList[0]),"yyyy-MM-dd HH:mm:ss");
        return Bytes.toBytes(time);
      }
      case TIMEREVERSE: {
        long time =0;
        try {
          time = Long.MAX_VALUE - Bytes.toLong(paramList[0]);
        } catch (java.lang.Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          throw new FormatException(e);
        }
        return Bytes.toBytes(time);
      }
      case COMBINEINDEX: {
          byte[] value = generateCombineRowkey(paramList);
        return value;
       }  
      case CURRENT_TIME: {
        String nwrksj = dateFormat.format(new Date());
        return Bytes.toBytes(nwrksj);
      }
      case TO_DOUBLE: {
        double result = Double.parseDouble(Bytes.toString(paramList[0]));
        return Bytes.toBytes(result);
      }
      
      default: {
        throw new FormatException("Upsupported operation!");
      }
    }
  }
  SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  
  private byte[] generateCombineRowkey(byte[][] paramList) {
  int sumSegmentLength = getSumSegmentLength(paramList);
  byte[] value = new byte[sumSegmentLength];
  int valueOffset = 0;
  byte[] externalValues = new byte[0];
  for (int i=0; i<paramList.length; i++) {
    byte[] val = paramList[i];
    int length = Integer.parseInt(Bytes.toString(paramList[i+1]));
    if (null != val) {
      byte[] encoded = null;
      if (null == encoded) {
         encoded = val;
      }
      if (encoded.length > length) {
        System.arraycopy(encoded, 0, value, valueOffset, length);
        byte[] externalValue = new byte[encoded.length - length];
        System.arraycopy(encoded, length, externalValue, 0, externalValue.length);
        externalValues = Bytes.add(externalValues, Bytes.toBytes((short)encoded.length), externalValue);
      } else {
        System.arraycopy(encoded, 0, value, valueOffset, encoded.length);
        externalValues = Bytes.add(externalValues, Bytes.toBytes((short) encoded.length));
      }
    }
    valueOffset += length;
    i++;
  }
 
  return Bytes.add(value, externalValues);
}
  
  private int getSumSegmentLength(byte[][] paramList) {
    int length = 0;
    for (int i=0; i<paramList.length; i++) {
      if(i % 2 == 1){
        length += Integer.parseInt(Bytes.toString(paramList[i]));
        //length += Bytes.toBytes(paramList[i]).length + 2;
      }
    }
    return length;
  }
  
	private String funcCalculate(Function func, String[] paramList)
		throws FormatException, IOException {

		switch (func) {
			case TRIM: {
				return paramList[0].trim();
			}
			case TRIM_QUOTATION : {
			  return trimByChar(paramList[0], '"');
			}
			case CONCAT: {
				StringBuffer sb = new StringBuffer(paramList[0]);
				for (int i = 1; i < paramList.length; i++) {
				  sb.append(rowSpec.getInternalColumnDelimiter());
					sb.append(paramList[i]);
				}
				return sb.toString();
			}
			// RPAD, LPAD, SUBSTR, CONCAT, REVERSE, RAND, CONST
			case RPAD: {
				String targetStr = paramList[0];
				int count = Integer.valueOf(paramList[1]);
				if (paramList.length == 3) {
					return StringUtils.rightPad(targetStr, count, paramList[2]);
				} else {
					return StringUtils.rightPad(targetStr, count);
				}
			}
			case LPAD: {
				String targetStr = paramList[0];
				int count = Integer.valueOf(paramList[1]);
				if (paramList.length == 3) {
					return StringUtils.leftPad(targetStr, count, paramList[2]);
				} else {
					return StringUtils.leftPad(targetStr, count);
				}
			}
			case SUBSTR: {
				String targetStr = paramList[0];
				int begin = Integer.valueOf(paramList[1]);
				if (paramList.length == 3) {
					if (targetStr.length() >= Integer.valueOf(paramList[2])) {
						return targetStr.substring(begin, Integer.valueOf(paramList[2]));
					} else {
						return targetStr.substring(begin);
					}
					
				} else {
					return targetStr.substring(begin);
				}
			}
			case REVERSE: {
				return StringUtils.reverse(paramList[0]);
			}
			case RAND: {
				int count = Integer.parseInt(paramList[0]);
				return RandomStringUtils.randomAlphanumeric(count);
			}
			/*
			case ASTYPE: {
				return typeToString(paramList[0], paramList[1]);
			}
			*/
			case NOTNULL : {
				if(paramList[0].isEmpty())
					throw new FormatException("Null Field!");

				return paramList[0];
			}
			case REPLACE : {
			  if (paramList.length == 3) {
			    return paramList[0].replaceAll(paramList[1], paramList[2]);
			  } else {
			    throw new FormatException("Not 3 parameters!");
			  }
			  
			}
			case LIANTONG_TRANSFERURL : {
			  if (paramList.length == 1) {
			    String converted = CUConvertTools.convertURL(paramList[0]);
			    int length = Math.min(converted.length(), 80);
			    return converted.substring(0, length);
			  } else {
			    throw new FormatException("LIANTONG_DINGZHI must 1 parameters!");
			  }
			}
			case LIANTONG_TRANSFERIP : {
        if (paramList.length == 1) {
          return CUConvertTools.convertIP(paramList[0]);
        } else {
          throw new FormatException("LIANTONG_DINGZHI must 1 parameters!");
        }
      }
			case REMOVE_WHITESPACE : {
			  if (paramList.length == 1) {
			    return paramList[0].replaceAll(" ", "");
        } else {
          throw new FormatException("LIANTONG_DINGZHI must 1 parameters!");
        }
			}
      case DATETOMILLTIME: {
        long time = transintoTimeStamp(paramList[0],"yyyy-MM-dd HH:mm:ss.SSS");
        return Long.toString(time);
      }
      case TOTIMESTAMP: {
        long time = transintoTimeStamp2(paramList[0],"yyyy-MM-dd HH:mm:ss");
        return Long.toString(time);
      }
      case TIMEREVERSE: {
        long time =0;
        try {
          time = Long.MAX_VALUE - Long.parseLong(paramList[0]);
        } catch (java.lang.Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          throw new FormatException(e);
        }
        return Long.toString(time);
      }
			default: {
				throw new FormatException("Upsupported operation!");
			}
		}
	}

  public static long transintoTimeStamp2(String str,String format) {
    SimpleDateFormat simpleDateFormat =new SimpleDateFormat(format);
    Date date2;
    long timeStemp=0;
    try {
      date2 = simpleDateFormat .parse(str);
      timeStemp = date2.getTime();
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return timeStemp;
  }

  public static long transintoTimeStamp(String str,String format) {
    SimpleDateFormat simpleDateFormat =new SimpleDateFormat(format);
    String dataStr = str.substring(0, str.indexOf(".")+4);
    Date date2;
    long timeStemp=0;
    try {
      date2 = simpleDateFormat .parse(dataStr);
      timeStemp = date2.getTime();
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return timeStemp;
  }
	
	private void ensureOperationResultsSize(int count) {
    if (this.operationResults.length >= count) {
      return;
    } else {
      byte[][] tmp = new byte[this.operationResults.length * 2][];
      for (int i=0; i<this.operationResults.length; ++i) {
        tmp[i] = this.operationResults[i];
      }
      this.operationResults = tmp;
    }
  }
	
	private byte[] calculateWithType(ArrayList<Operation> operationList, ArrayList<String> line)
	    throws FormatException, IOException {

    int operationCount = operationList.size();
    ensureOperationResultsSize(operationCount);
    for (int i=0; i < operationCount; i++) {
      Operation op = operationList.get(i);
      Parameter[] paramsArr = op.getParams();
      byte[][] fields = new byte[paramsArr.length][];
      String[] strFields = new String[paramsArr.length];
      
      for (int j=0; j < paramsArr.length; j++) {
        switch (paramsArr[j].getType()) {
          case CONST: {
            fields[j] = Bytes.toBytes(paramsArr[j].getValue());
            strFields[j] = paramsArr[j].getValue();
            break;
          }
          case RESULT: {
            int off = Integer.valueOf(paramsArr[j].getValue());
            fields[j] = operationResults[off];
            strFields[j] = Bytes.toString(operationResults[off]);
            break;
          }
          default: {
            int off = Integer.valueOf(paramsArr[j].getValue());
            fields[j] = Bytes.toBytes(line.get(off));
            strFields[j] = line.get(off);
            break;
          }
        }
      }
      
      Function f = op.getFunction();
      byte[] b = null;
      switch (f) {
      case ASTYPE:
        b = typeToBytes(Bytes.toString(fields[0]), Bytes.toString(fields[1]));
        break;
      case CONCAT:
        b = fields[0];
        for (int j=1; j < fields.length; ++j) {
          b = Bytes.add(b, fields[j]);
        }
        break;
      default:
        String result = funcCalculate(op.getFunction(), strFields);
        b = Bytes.toBytes(result);
      }
      
      operationResults[i] = b;
    }
    
    return operationResults[operationCount-1];
  
	}
	
	private byte[] calculateWithoutType(ArrayList<Operation> operationList, ArrayList<String> line)
	    throws FormatException, IOException {

    int operationCount = operationList.size();
    ensureOperationResultsSize(operationCount);
    for (int i=0; i < operationCount; i++) {
      Operation op = operationList.get(i);
      Parameter[] paramsArr = op.getParams();
      String[] fields = new String[paramsArr.length];
      byte[][] byteFields = new byte[paramsArr.length][];
      
      for (int j=0; j < paramsArr.length; j++) {
        switch (paramsArr[j].getType()) {
          case CONST: {
            fields[j] = paramsArr[j].getValue();
            byteFields[j] = Bytes.toBytes(paramsArr[j].getValue());
            break;
          }
          case RESULT: {
            int off = Integer.valueOf(paramsArr[j].getValue());
            fields[j] = Bytes.toString(operationResults[off]);
            byteFields[j] = operationResults[off];
            break;
          }
          default: {
            byteFields[j] = Bytes.toBytes(line.get(Integer.valueOf(paramsArr[j].getValue())));
            fields[j] = line.get(Integer.valueOf(paramsArr[j].getValue()));
            break;
          }
        }
      }
      
      Function f = op.getFunction();
      byte[] b = null;
      switch (f) {
      case ASTYPE:
        b = typeToBytes(fields[0], fields[1]);
        break;
      default:
        b = funcCalculatewithByte(op.getFunction(), byteFields);
        /*
        String result = funcCalculate(op.getFunction(), fields);
        b = Bytes.toBytes(result);
        */
      }
      
      operationResults[i] = b;
    }
    
    return operationResults[operationCount-1];
  
  }
	
	public byte[] calculate(ArrayList<Operation> operationList, ArrayList<String> line) 
			throws FormatException, IOException {
	  if (this.rowSpec.getUseHyperbaseDataType()) {
	    return calculateWithType(operationList, line);
	  } else {
	    return calculateWithoutType(operationList, line);
	  }
	}
	
	public byte[] genRowKey(ArrayList<String> line, long uniqID) throws FormatException, IOException {
		byte[] rowKeyValue = calculate(rowSpec.getRowKeySpec().getOperationList(), line);
		
		if (this.rowSpec.getRowkeyAutoIncreasing()) {
		  byte[] rowKeyBytes = null;
		  if (this.rowSpec.getUseHyperbaseDataType()) {
		    ByteArrayOutputStream b = new ByteArrayOutputStream();
	      DataType.LONG.encode(uniqID, b, -1);
	      rowKeyBytes = Bytes.add(rowKeyValue, b.toByteArray());
		  } else {
        rowKeyBytes = Bytes.add(rowKeyValue,
            Bytes.toBytes(rowSpec.getInternalColumnDelimiter()),
            Bytes.toBytes(uniqID));
		  }
		  return rowKeyBytes;
		}
		
		return rowKeyValue;
	}
	
	/**
	 * Generate the rowkey for the index table
	 */
	public byte[] genIndexTableRowKey(String indexTablename, ArrayList<String> line, long uniqID) throws FormatException, IOException {
    byte[] rowKeyValue = calculate(indexTableRowkeySpecs.get(indexTablename).getFirst().getOperationList(), line);
    if (this.rowSpec.getRowkeyAutoIncreasing()) {
      byte[] rowKeyBytes = null;
      if (this.rowSpec.getUseHyperbaseDataType()) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataType.LONG.encode(uniqID, b, -1);
        rowKeyBytes = Bytes.add(rowKeyValue, b.toByteArray());
      } else {
        rowKeyBytes = Bytes.add(rowKeyValue,
            Bytes.toBytes(rowSpec.getInternalColumnDelimiter()),
            Bytes.toBytes(uniqID));
      }
      return rowKeyBytes;
    }
    return rowKeyValue;
  }
	public String getIndexConvert() {
	  StringBuilder sb = new StringBuilder();
	  for (Map.Entry<String, Pair<ColumnSpec, ColumnSpec>> entry : this.indexTableRowkeySpecs.entrySet()) {
	    sb.append(entry.getKey()).append(" row: ").append(entry.getValue().getFirst().getSpecString());
	    sb.append(". ").append("value: ").append(entry.getValue().getSecond().getSpecString());
	  }
	  return sb.toString();
	}
	
	
	public byte[] genIndexTableQualifier(String indexTablename, ArrayList<String> line, long uniqID) throws FormatException, IOException {
    byte[] rowKeyValue = calculate(indexTableRowkeySpecs.get(indexTablename).getSecond().getOperationList(), line);
    return rowKeyValue;
  }
	
	public Put convert(ArrayList<String> line, byte[] rowKeyBytes)
			throws FormatException, IOException {

		Put put = new Put(rowKeyBytes, timeStamp);
		for (Map.Entry<HBaseTableSpec.ColumnName, ColumnSpec> e : rowSpec.columnMap
				.entrySet()) {
		  /*
			String cfColumnName = e.getKey();
			String[] cfColumnArray = cfColumnName.split(ExpressionUtils
					.getEscapedDelimiter(ImportTextFile2HBase.CF_C_DELIMITER));
			byte[] cfBytes = Bytes.toBytes(cfColumnArray[0]);
			byte[] columnBytes = null;
			if (cfColumnArray.length == 2) {
				columnBytes = Bytes.toBytes(cfColumnArray[1]);
			}
			*/
			byte[] cfBytes = Bytes.toBytes(e.getKey().getFamily());
      byte[] columnBytes = e.getKey().getQualifier() == null ? null : Bytes.toBytes(e.getKey().getQualifier());
			byte[] columnValues = calculate(e.getValue().getOperationList(), line);
			if (columnValues.length == 0) {
			  if (this.rowSpec.getEmptyStringAsNull()) {
			    // put.add(cfBytes, columnBytes, columnValues);
			    fillPut(put, cfBytes, columnBytes, columnValues);
			  }
			} else {
			  // put.add(cfBytes, columnBytes, columnValues);
			  fillPut(put, cfBytes, columnBytes, columnValues);
			}
		}
			
		return put;
	}
	
	private void fillPut(Put p, byte[] family, byte[] qualifier, byte[] value) {
	  if (this.rowSpec.getLoadThemis()) {
	    long prewriteTs = 0;
	    p.add(family, qualifier, prewriteTs, value);
	    byte[] bytes = (qualifier == null) ? ColumnUtil.PUT_QUALIFIER_SUFFIX_BYTES
	        : Bytes.add(qualifier, ColumnUtil.PUT_QUALIFIER_SUFFIX_BYTES);
	    p.add(family, bytes, 1, Bytes.toBytes(prewriteTs));
	  } else {
	    p.add(family, qualifier, value);
	  }
	}

  public IndexRequest buildIndexRequest(String indexName, String type, ArrayList<String> line, String rowKey) throws IOException, FormatException {
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
    for (Map.Entry<HBaseTableSpec.ColumnName, ColumnSpec> e : rowSpec.columnMap.entrySet()) {
      byte[] columnBytes = e.getKey().getQualifier() == null ? null : Bytes.toBytes(e.getKey().getQualifier());
      byte[] columnValues = calculate(e.getValue().getOperationList(), line);
      if (columnValues.length != 0) {
        builder.field(Bytes.toString(columnBytes), Bytes.toString(columnValues));
      }
    }

    return new IndexRequest(indexName).type(type).id(rowKey).source(builder);
  }
	
	public PutWritable convertPutWritable(ArrayList<String> line, byte[] rowKeyBytes)
      throws FormatException, IOException {
	  PutWritable put = new PutWritable(rowKeyBytes, timeStamp);
    for (Map.Entry<HBaseTableSpec.ColumnName, ColumnSpec> e : rowSpec.columnMap.entrySet()) {
      /*
      String cfColumnName = e.getKey();
      String[] cfColumnArray = cfColumnName.split(ExpressionUtils
          .getEscapedDelimiter(ImportTextFile2HBase.CF_C_DELIMITER));
      byte[] cfBytes = Bytes.toBytes(cfColumnArray[0]);
      byte[] columnBytes = null;
      if (cfColumnArray.length == 2) {
        columnBytes = Bytes.toBytes(cfColumnArray[1]);
      }
      */
      byte[] cfBytes = Bytes.toBytes(e.getKey().getFamily());
      byte[] columnBytes = e.getKey().getQualifier() == null ? null : Bytes.toBytes(e.getKey().getQualifier());
      byte[] columnValues = calculate(e.getValue().getOperationList(), line);
      if (columnValues.length == 0) {
        if (this.rowSpec.getEmptyStringAsNull()) {
          put.add(cfBytes, columnBytes, columnValues);
        }
      } else {
        put.add(cfBytes, columnBytes, columnValues);
      }
    }
      
    return put;
  }
	
	public static String transformURL (String url) {
	  String converted = CUConvertTools.convertURL(url);
	  int length = Math.min(converted.length(), 80);
	  return converted.substring(0, length);
	}
	
	public static void main(String args[]) {
	  System.err.println(TextRecord2HBaseRowConverter.trimByChar("", '"'));
	  System.err.println(TextRecord2HBaseRowConverter.trimByChar("\"", '"'));
	  System.err.println(TextRecord2HBaseRowConverter.trimByChar("\"hello you\"", '"'));
	  System.err.println(TextRecord2HBaseRowConverter.trimByChar("\"\"\"hello\"\"", '"'));
	  String[] urls = {"http://www.baidu.com/", "http://1.1.1.1/12345fjalsdjfladjf;ladkjf;ladsjf;ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd.e"};
	  for (String url : urls) {
	    System.out.println("Before transfer: " + url);
	    System.out.println("After transfer: " + transformURL(url));
	    System.out.println("Before tranfer: " + url.length() + " After length: " + transformURL(url).length());
	  }
	  System.out.println("1 2 3 4 5".replaceAll(" ", ""));
	  
	}
}
