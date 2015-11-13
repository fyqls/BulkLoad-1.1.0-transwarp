import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.transwarp.hbase.bulkload.HBaseTableSpec;
import com.transwarp.hbase.bulkload.TextRecordSpec;

public class TestHTableSpec {

	@Test
	public void testSimpleSpec() {

		try {
			HBaseTableSpec spec = new HBaseTableSpec(
					"test",
					"rowKey:concat(firstName,secondName);value:concat(firstName, secondName, age)",null
					, new TextRecordSpec("firstName:STRING,secondName,age:INT"));

			assertTrue("test-rowKey:concat(firstName,secondName)-[value:concat(firstName,secondName,age)]"
					.equals(spec.toString()));

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	
	@Test
	public void testApshSpec() {

		try {
			HBaseTableSpec spec = new HBaseTableSpec(
					"apsh",
					"rowKey:concat(trim(APSHPROCOD),trim(APSHACTNO),trim(APSHPRDNO),trim(APSHTRDAT),trim(APSHJRNNO),trim(APSHSEQNO));TRDAT:trim(APSHTRDAT);JRNNO:trim(APSHJRNNO);MISC:concat(trim(APSHFRNJRN),trim(APSHTRTM),trim(APSHVCHNO),trim(APSHTRPROCCOD),trim(APSHTRBK),trim(APSHTRCOD),trim(APSHTRAMT),trim(APSHCSHTFR),trim(APSHRBIND),trim(APSHTRAFTBAL),trim(APSHERRDAT),trim(APSHDBKTYP),trim(APSHDBKPRO),trim(APSHBATNO),trim(APSHDBKNO),trim(APSHAPROCOD),trim(APSHAACNO),trim(APSHABS),trim(APSHREM),trim(APSHTRCHL),trim(APSHTRFRM),trim(APSHTRPLA),trim(APSHECIND),trim(APSHPRTIND),trim(APSHSUP1),trim(APSHSUP2))",null
					, new TextRecordSpec("firstName:STRING,secondName,age:INT"));

			assertTrue("apsh-rowKey:concat(trim(APSHPROCOD),trim(APSHACTNO),trim(APSHPRDNO),trim(APSHTRDAT),trim(APSHJRNNO),trim(APSHSEQNO))-[TRDAT:trim(APSHTRDAT), MISC:concat(trim(APSHFRNJRN),trim(APSHTRTM),trim(APSHVCHNO),trim(APSHTRPROCCOD),trim(APSHTRBK),trim(APSHTRCOD),trim(APSHTRAMT),trim(APSHCSHTFR),trim(APSHRBIND),trim(APSHTRAFTBAL),trim(APSHERRDAT),trim(APSHDBKTYP),trim(APSHDBKPRO),trim(APSHBATNO),trim(APSHDBKNO),trim(APSHAPROCOD),trim(APSHAACNO),trim(APSHABS),trim(APSHREM),trim(APSHTRCHL),trim(APSHTRFRM),trim(APSHTRPLA),trim(APSHECIND),trim(APSHPRTIND),trim(APSHSUP1),trim(APSHSUP2)), JRNNO:trim(APSHJRNNO)]"
					.equals(spec.toString()));

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	
	@Test
	public void testGmccSpec() {

		try {
			HBaseTableSpec spec = new HBaseTableSpec(
					"gmcc",
					"rowKey:concat(reverse(trim(f6)),trim(f24),trim(f0));session_dur:f12;url:f13;misc:concat(f1,f2,f3,f4,f5,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f25,f26,f27,f28,f29,f30,f31,f32,f33,f34,f35,f36,f37,f38)",null
					, new TextRecordSpec("firstName:STRING,secondName,age:INT"));

			assertTrue("gmcc-rowKey:concat(reverse(trim(f6)),trim(f24),trim(f0))-[session_dur:f12, url:f13, misc:concat(f1,f2,f3,f4,f5,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f22,f23,f25,f26,f27,f28,f29,f30,f31,f32,f33,f34,f35,f36,f37,f38)]"
					.equals(spec.toString()));

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

}
