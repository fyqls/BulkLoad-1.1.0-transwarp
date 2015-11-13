//import static org.junit.Assert.*;
//
//import java.util.List;
//
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.junit.Test;
//
//import com.transwarp.hbase.bulkload.HBaseTableSpec;
//import com.transwarp.hbase.bulkload.ParsedLine;
//import com.transwarp.hbase.bulkload.TextRecord2HBaseRowConverter;
//import com.transwarp.hbase.bulkload.TextRecordSpec;
//
//
//public class TestTextRecord2HBaseRowConverter {
//
//	
//	@Test
//	public void testConcat() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("firstName:STRING,secondName,age:INT");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"test",
//				"rowKey:concat(firstName,secondName);value:concat(firstName,secondName,age)", null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "Gong,Shaocheng,33");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"Gong|Shaocheng");
//		
//		 List<KeyValue> kvs = put.get(Bytes.toBytes("f"),Bytes.toBytes("value"));
//		assertEquals(Bytes.toString(kvs.get(0).getValue()),"Gong|Shaocheng|33");
//
//	}
//	
//	@Test
//	public void testTrim() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("firstName:STRING,secondName,age:INT");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"test",
//				"rowKey:concat(firstName,secondName);value:trim(age)", null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "Gong, Shaocheng, 33 ");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"Gong| Shaocheng");
//		
//		 List<KeyValue> kvs = put.get(Bytes.toBytes("f"),Bytes.toBytes("value"));
//		assertEquals(Bytes.toString(kvs.get(0).getValue()),"33");
//
//	}
//	
//	@Test
//	public void testRPad() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("firstName:STRING,secondName,age:INT");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"test",
//				"rowKey:concat(firstName,secondName);value:rpad(age,5,#)", null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "Gong,Shaocheng,33");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"Gong|Shaocheng");
//		
//		 List<KeyValue> kvs = put.get(Bytes.toBytes("f"),Bytes.toBytes("value"));
//		assertEquals(Bytes.toString(kvs.get(0).getValue()),"33###");
//	}
//	
//	@Test
//	public void testLPad() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("firstName:STRING,secondName,age:INT");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"test",
//				"rowKey:concat(firstName,secondName);value:lpad(age,5,#)", null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "Gong,Shaocheng,33");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"Gong|Shaocheng");
//		
//		 List<KeyValue> kvs = put.get(Bytes.toBytes("f"),Bytes.toBytes("value"));
//		assertEquals(Bytes.toString(kvs.get(0).getValue()),"###33");
//	}
//	
//	@Test
//	public void testReverse() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("firstName:STRING,secondName,age:INT");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"test",
//				"rowKey:concat(firstName,secondName);value:reverse(age)", null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "Gong,Shaocheng,23");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"Gong|Shaocheng");
//		
//		 List<KeyValue> kvs = put.get(Bytes.toBytes("f"),Bytes.toBytes("value"));
//		assertEquals(Bytes.toString(kvs.get(0).getValue()),"32");
//	}
//	
//	@Test
//	public void testSubstr() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("firstName:STRING,secondName,age:INT");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"test",
//				"rowKey:concat(firstName,secondName);value:substr(age,1,3)", null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "Gong,Shaocheng,033");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"Gong|Shaocheng");
//		
//		 List<KeyValue> kvs = put.get(Bytes.toBytes("f"),Bytes.toBytes("value"));
//		assertEquals(Bytes.toString(kvs.get(0).getValue()),"33");
//	}
//	
//	@Test
//	public void testParseRecordSubstringInConcat() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("firstName:STRING,secondName,age:INT");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"test",
//				"rowKey:concat(firstName,secondName);value:concat(substr(firstName,0,4),substr(secondName,4,9),age)", null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "Gong,Shaocheng,33");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"Gong|Shaocheng");
//		
//		 List<KeyValue> kvs = put.get(Bytes.toBytes("f"),Bytes.toBytes("value"));
//		assertEquals(Bytes.toString(kvs.get(0).getValue()),"Gong|cheng|33");
//	}
//	
//	@Test
//	public void testParseRecordConcatSubstrings() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("firstName:STRING,secondName,age:INT");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"test",
//				"rowKey:concat(substr(firstName,0,1),substr(secondName,0,1));value:concat(firstName,secondName,age)", null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "Gong,Shaocheng,33");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"G|S");
//		
//		 List<KeyValue> kvs = put.get(Bytes.toBytes("f"),Bytes.toBytes("value"));
//		assertEquals(Bytes.toString(kvs.get(0).getValue()),"Gong|Shaocheng|33");
//	}
//	
//	@Test
//	public void testParseApshRecord() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("APSHPROCOD|!APSHACTNO|!APSHTRDAT|!APSHJRNNO|!APSHSEQNO|!APSHPRDNO|!APSHFRNJRN|!APSHTRTM|!APSHVCHNO|!APSHTRPROCCOD|!APSHTRBK|!APSHTRCOD|!APSHTRAMT|!APSHCSHTFR|!APSHRBIND|!APSHTRAFTBAL|!APSHERRDAT|!APSHDBKTYP|!APSHDBKPRO|!APSHBATNO|!APSHDBKNO|!APSHAPROCOD|!APSHAACNO|!APSHABS|!APSHREM|!APSHTRCHL|!APSHTRFRM|!APSHTRPLA|!APSHECIND|!APSHPRTIND|!APSHSUP1|!APSHSUP2","gb2312","|!");
//		
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"apsh",
//				"rowKey:concat(trim(APSHPROCOD),trim(APSHACTNO),lpad(trim(APSHPRDNO),19,0),trim(APSHTRDAT),trim(APSHJRNNO),trim(APSHSEQNO));TRDAT:trim(APSHTRDAT);JRNNO:trim(APSHJRNNO);MISC:concat(trim(APSHFRNJRN),trim(APSHTRTM),trim(APSHVCHNO),trim(APSHTRPROCCOD),trim(APSHTRBK),trim(APSHTRCOD),trim(APSHTRAMT),trim(APSHCSHTFR),trim(APSHRBIND),trim(APSHTRAFTBAL),trim(APSHERRDAT),trim(APSHDBKTYP),trim(APSHDBKPRO),trim(APSHBATNO),trim(APSHDBKNO),trim(APSHAPROCOD),trim(APSHAACNO),trim(APSHABS),trim(APSHREM),trim(APSHTRCHL),trim(APSHTRFRM),trim(APSHTRPLA),trim(APSHECIND),trim(APSHPRTIND),trim(APSHSUP1),trim(APSHSUP2))",null);
//	
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "50|!101001100021092|!20110131|!384535120  |!1     |!101000460001793    |!            |!165051|!777737ah|!50|!3700|!3958  |! 0000000000002560.84|!1|!0|! 0000000000002586.50|!        |!62         |!  |!01|!0          |!50|!370001941000213|!转存      |!                              |!BTER|!          |!3700                          |!0|!1|!    |!    ");
//		
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"50|101001100021092|0000101000460001793|20110131|384535120|1");
//		
//		List<KeyValue> trdat = put.get(Bytes.toBytes("f"),Bytes.toBytes("TRDAT"));
//		assertEquals(Bytes.toString(trdat.get(0).getValue()),"20110131");
//		
//		List<KeyValue> jrnno = put.get(Bytes.toBytes("f"),Bytes.toBytes("JRNNO"));
//		assertEquals(Bytes.toString(jrnno.get(0).getValue()),"384535120");
//		
//		List<KeyValue> misc = put.get(Bytes.toBytes("f"),Bytes.toBytes("MISC"));
//		assertEquals(Bytes.toString(misc.get(0).getValue()),"|165051|777737ah|50|3700|3958|0000000000002560.84|1|0|0000000000002586.50||62||01|0|50|370001941000213|转存||BTER||3700|0|1||");
//	}
//	
//	@Test
//	public void testParseGmccRecord() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("f0@#$f1@#$f2@#$f3@#$f4@#$f5@#$f6@#$f7@#$f8@#$f9@#$f10@#$f11@#$f12@#$f13@#$f14@#$f15@#$f16@#$f17@#$f18@#$f19@#$f20@#$f21@#$f22@#$f23@#$f24@#$f25@#$f26@#$f27@#$f28@#$f29@#$f30@#$f31@#$f32@#$f33@#$f34@#$f35@#$f36@#$f37@#$f38","gb2312","@#$");
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"gmcc",
//				"rowKey:concat(reverse(trim(f6)),trim(f24),trim(f0));session_dur:f12;url:f13;misc:concat(f1,f2,f3,f4,f5,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f25,f26,f27,f28,f29,f30,f31,f32,f33,f34,f35,f36,f37,f38)","3B43,7D6D,C0A8");
//
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "201212070530@#$@#$757@#$2@#$61@#$2012-12-07 05:30:16@#$29DDED5E6B68D468A50780FD1495F231@#$@#$@#$@#$@#$@#$@#$120.196.210.101:14000@#$120.196.210.101@#$@#$@#$@#$@#$@#$0@#$@#$@#$@#$10.27.25.178@#$81085@#$166058@#$@#$Socket@#$200@#$13@#$20@#$@#$@#$@#$@#$0@#$0@#$20121201");
//	
//		Put put = converter.convert(record);
//		
//		assertEquals(Bytes.toString(put.getRow()),"132F5941DF08705A864D86B6E5DEDD92|10.27.25.178|201212070530");
//		
//		List<KeyValue> session_dur = put.get(Bytes.toBytes("f"),Bytes.toBytes("session_dur"));
//		assertEquals(Bytes.toString(session_dur.get(0).getValue()),"");
//		
//		List<KeyValue> url = put.get(Bytes.toBytes("f"),Bytes.toBytes("url"));
//		assertEquals(Bytes.toString(url.get(0).getValue()),"120.196.210.101:14000");
//		
//		List<KeyValue> misc = put.get(Bytes.toBytes("f"),Bytes.toBytes("misc"));
//		assertEquals(Bytes.toString(misc.get(0).getValue()),"|757|2|61|2012-12-07 05:30:16|||||||120.196.210.101:14000|120.196.210.101||||||0||||81085|166058||Socket|200|13|20|||||0|0|20121201");
//	}
//	
//	@Test
//	public void testParseGmccRandRecord() throws Exception {
//		
//		TextRecordSpec textSpec = new TextRecordSpec("f0@#$f1@#$f2@#$f3@#$f4@#$f5@#$f6@#$f7@#$f8@#$f9@#$f10@#$f11@#$f12@#$f13@#$f14@#$f15@#$f16@#$f17@#$f18@#$f19@#$f20@#$f21@#$f22@#$f23@#$f24@#$f25@#$f26@#$f27@#$f28@#$f29@#$f30@#$f31@#$f32@#$f33@#$f34@#$f35@#$f36@#$f37@#$f38","gb2312","@#$");
//	
//		HBaseTableSpec hbaseSpec = new HBaseTableSpec(
//				"gmcc",
//				"rowKey:concat(reverse(trim(f6)),trim(f24),trim(f0),rand(10));session_dur:f12;url:f13;misc:concat(f1,f2,f3,f4,f5,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f25,f26,f27,f28,f29,f30,f31,f32,f33,f34,f35,f36,f37,f38)","3B43,7D6D,C0A8");
//
//		TextRecord2HBaseRowConverter converter = new TextRecord2HBaseRowConverter(textSpec, hbaseSpec);
//		
//		ParsedLine record = new ParsedLine(textSpec, "201212070530@#$@#$757@#$2@#$61@#$2012-12-07 05:30:16@#$29DDED5E6B68D468A50780FD1495F231@#$@#$@#$@#$@#$@#$@#$120.196.210.101:14000@#$120.196.210.101@#$@#$@#$@#$@#$@#$0@#$@#$@#$@#$10.27.25.178@#$81085@#$166058@#$@#$Socket@#$200@#$13@#$20@#$@#$@#$@#$@#$0@#$0@#$20121201");
//	
//		Put put = converter.convert(record);
//		
//		String rowKey = Bytes.toString(put.getRow());
//		String expected = rowKey.substring(0, rowKey.length() - 11);
//		
//		assertEquals(expected,"132F5941DF08705A864D86B6E5DEDD92|10.27.25.178|201212070530");
//		
//		List<KeyValue> session_dur = put.get(Bytes.toBytes("f"),Bytes.toBytes("session_dur"));
//		assertEquals(Bytes.toString(session_dur.get(0).getValue()),"");
//		
//		List<KeyValue> url = put.get(Bytes.toBytes("f"),Bytes.toBytes("url"));
//		assertEquals(Bytes.toString(url.get(0).getValue()),"120.196.210.101:14000");
//		
//		List<KeyValue> misc = put.get(Bytes.toBytes("f"),Bytes.toBytes("misc"));
//		assertEquals(Bytes.toString(misc.get(0).getValue()),"|757|2|61|2012-12-07 05:30:16|||||||120.196.210.101:14000|120.196.210.101||||||0||||81085|166058||Socket|200|13|20|||||0|0|20121201");
//	}
//	
//}
