package com.transwarp.hbase.bulkload.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.transwarp.hbase.bulkload.FormatException;

public class ExpressionUtils {
	
	public static boolean isAtomicExpression(String columnSpec){
		if (StringUtils.indexOf(columnSpec, "(") < 0 && StringUtils.indexOf(columnSpec, ")") < 0 && StringUtils.indexOf(columnSpec, ",") < 0){
			return true;			
		}
		
		return false;
	}
	
	public static String[] getExpressions(String commaSeperatedExpressions) throws FormatException{
		
		List<String> expressions = new ArrayList<String>();
		
		spawnExpressions(commaSeperatedExpressions, expressions);
		
		return expressions.toArray(new String[expressions.size()]);	
	}
	
	//return the postion of the top closing parenthesis in the first expression in a common seperated expressions
	public static int getTopParenthesisClosingPositionOfFirstExpression(String commaSeperatedExpressions) throws FormatException{
		
		int openPostion = StringUtils.indexOf(commaSeperatedExpressions, "(");
		if (openPostion < 0) return -1;	//no parenthesis
		
		int commaPostion = StringUtils.indexOf(commaSeperatedExpressions, ",");
		
		if (commaPostion<openPostion) return -1;//no parenthesis in the first expression
		
		int index = openPostion + 1;
		int netCount = 1;
		while(index < commaSeperatedExpressions.length()){
			char c = commaSeperatedExpressions.charAt(index);
			if (c == '('){
				netCount++;
			}else if(c == ')'){
				netCount--;
				if(netCount == 0) return index;//arrive at the closing parenthesis of the first open parenthesis
			}
				
			index++;
		}
		
		throw new FormatException("Parenthesis is not closed properly: " + commaSeperatedExpressions);		
	}

	//return the first comma postion in the top level in a common seperated expressions
	public static int getFirstCommaPostionOnTopLevel(String commaSeperatedExpressions) throws FormatException{
		
		int openPostion = StringUtils.indexOf(commaSeperatedExpressions, "(");
		if (openPostion < 0) return -1;	//no parenthesis
		
		int commaPostion = StringUtils.indexOf(commaSeperatedExpressions, ",");
		
		if (commaPostion<openPostion) return -1;//no parenthesis in the first expression
		
		int index = openPostion + 1;
		int netCount = 1;
		while(index < commaSeperatedExpressions.length()){
			char c = commaSeperatedExpressions.charAt(index);
			if (c == '('){
				netCount++;
			}else if(c == ')'){
				netCount--;
				if(netCount == 0) return index;//arrive at the closing parenthesis of the first open parenthesis
			}
				
			index++;
		}
		
		throw new FormatException("Parenthesis is not closed properly: " + commaSeperatedExpressions);		
	}

	
	private static void spawnExpressions(String commaSeperatedExpressions, List<String> expressions) throws FormatException{
		
		int closingParenthesisPosition = getTopParenthesisClosingPositionOfFirstExpression(commaSeperatedExpressions);
		
		
		if (closingParenthesisPosition < 0) {//no parenthesis in first expression
			
			int firstCommaPostionOnTopLevel = StringUtils.indexOf(commaSeperatedExpressions, ",");
		
			if (firstCommaPostionOnTopLevel < 0) {//only one expression
				String firstExpression = commaSeperatedExpressions;
				expressions.add(firstExpression);
				return;
			}
		
			String firstExpression = commaSeperatedExpressions.substring(0,firstCommaPostionOnTopLevel);
			expressions.add(firstExpression);		
			String remaining = commaSeperatedExpressions.substring(firstCommaPostionOnTopLevel + 1);//skip the "," after the first expression
			spawnExpressions(remaining, expressions);
			return;
		}
	
		//first expression with parenthesis
		
		String firstExpression = commaSeperatedExpressions.substring(0,closingParenthesisPosition +1);
		expressions.add(firstExpression);
	
		if (firstExpression.length() == commaSeperatedExpressions.length()) return;//only one expression
	
		String remaining = commaSeperatedExpressions.substring(closingParenthesisPosition + 2);//skip the "," after the first expression
		spawnExpressions(remaining, expressions);	
	}


	
	public static String getEscapedDelimiter(String delimiter){
		StringBuilder sb = new StringBuilder();
		
		for(char c : delimiter.toCharArray()){
			if(c == '|' || c == '$'){
				sb.append("\\").append(c);
			}else{
				sb.append(c);
			}
		}
		
		return sb.toString();
	}
}
