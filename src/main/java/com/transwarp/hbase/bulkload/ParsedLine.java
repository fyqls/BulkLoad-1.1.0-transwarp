package com.transwarp.hbase.bulkload;

import java.util.ArrayList;


public class ParsedLine {
	
	public static String escapedFieldDelimiter = null;
	private static ArrayList<String> fields = new ArrayList<String>();

  public static ArrayList<String> parse(TextRecordSpec recordSpec,
      String rawRecord) throws FormatException {
    fields.clear();
    if (escapedFieldDelimiter.length() == 1) {
      char delimiterChar = escapedFieldDelimiter.charAt(0);
      char[] rawRecordBuffer = rawRecord.toCharArray();
      int offset = 0, index = 0;
      for (char c : rawRecordBuffer) {
        if (c == delimiterChar) {
          String newString = new String(rawRecordBuffer, offset, index - offset);
          if (newString.equals("null")) {
            newString = "";
          }
          fields.add(newString);
          offset = index + 1;
        }
        index++;
      }
      String newString = new String(rawRecordBuffer, offset, index - offset);
      if (newString.equals("null")) {
        newString = "";
      }
      fields.add(newString);
    } else {
      String[] fieldArray = rawRecord.split(escapedFieldDelimiter, -1);
      for (String field : fieldArray) {
        fields.add(field.equals("null") ? "" : field);
      }
    }

    int fieldNumber = recordSpec.fieldList.size();

    if (fieldNumber > fields.size())
      throw new FormatException("Invalid line: actual fields " + fields.size()
          + " expected fields " + fieldNumber + "            delimited: " + escapedFieldDelimiter + "  length: " + rawRecord.split(escapedFieldDelimiter).length + " value: " + rawRecord);

    return fields;
  }
}
