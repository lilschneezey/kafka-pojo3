package io.schneezey.pojo;

import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.DateAsLongEncoding;
import org.apache.avro.reflect.Nullable;

/*
 * Notes:
 *
 * Avro does not support DateAsLongEncoding & Nullable at the same time.  This is documented in @AvroEncode.  It prevents the schema from being created with Nullable.
 */
public class TestPojo {
	
	private Integer id = 0;

	
	// Primitives
	private float testPFloat = (float) 0.0;
	private double testPDouble = 0.0;
	private int testPInt = 2000;
	private long testPLong = 3000L;
	private boolean testPBoolean = true;	
	private byte[] testPBytes = new byte[0];
	private String testPString = new String();

	@Nullable private Float testNFloat = null;
	@Nullable private Double testNDouble = null;
	@Nullable private Integer testNInt = null;
	@Nullable private Long testNLong = null;
	@Nullable private Boolean testNBoolean = null;	
	@Nullable private byte[] testNBytes = null;
	@Nullable private String testNString = null;

	@Nullable private Float testN1Float = new Float(1.0);
	@Nullable private Double testN1Double = new Double(2.0);
	@Nullable private Integer testN1Int = new Integer(3);
	@Nullable private Long testN1Long = new Long(50000L);
	@Nullable private Boolean testN1Boolean = new Boolean(false);	
	@Nullable private byte[] testN1Bytes = new byte[5];
	@Nullable private String testN1String = new String();
	
	@AvroEncode(using=DateAsLongEncoding.class) 	
	private Date testDate = new Date();

	//@Nullable
	//@AvroEncode(using=DateAsLongEncoding.class) 	
	//private Date testNDate = null;
	
	@AvroEncode(using=DateAsLongEncoding.class) 	
	private Date testN1Date = new Date();

	public enum TEST_ENUM { TYPEA, TYPEB, TYPEC };
	private TEST_ENUM testenum = TEST_ENUM.TYPEA;
	
	@Nullable
	private ArrayList<TestSubPojo> subPojos = null;

	@AvroIgnore private int testIgnore = 0;

	public String toString() {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		StringBuffer buffer = new StringBuffer();
		buffer.append("id: ").append(id).append("\n")
		
		.append("testPFloat: ").append(testPFloat).append("\n")
		.append("testPDouble: ").append(testPDouble).append("\n")
		.append("testPInt: ").append(testPInt).append("\n")
		.append("testPLong: ").append(testPLong).append("\n")
		.append("testPBoolean: ").append(testPBoolean).append("\n")
		.append("testPBytes: ").append(testPBytes==null ? "null" : testPBytes).append("\n")
		.append("testPString: ").append(testPString==null ? "null" : testPString).append("\n")


		.append("testNFloat: ").append(testNFloat==null ? "null" : testNFloat ).append("\n")
		.append("testNDouble: ").append(testNDouble==null ? "null" : testNDouble).append("\n")
		.append("testNInt: ").append(testNInt==null ? "null" : testNInt).append("\n")
		.append("testNLong: ").append(testNLong==null ? "null" : testNLong).append("\n")
		.append("testNBoolean: ").append(testNBoolean==null ? "null" : testNBoolean).append("\n")
		.append("testNBytes: ").append(testNBytes==null ? "null" : testNBytes).append("\n")
		.append("testNString: ").append(testNString==null ? "null" : testNString).append("\n")
		
		.append("testN1Float: ").append(testN1Float==null ? "null" : testN1Float ).append("\n")
		.append("testN1Double: ").append(testN1Double==null ? "null" : testN1Double).append("\n")
		.append("testN1Int: ").append(testN1Int==null ? "null" : testN1Int).append("\n")
		.append("testN1Long: ").append(testN1Long==null ? "null" : testN1Long).append("\n")
		.append("testN1Boolean: ").append(testN1Boolean==null ? "null" : testN1Boolean).append("\n")
		.append("testN1Bytes: ").append(testN1Bytes==null ? "null" : testN1Bytes).append("\n")
		.append("testN1String: ").append(testN1String==null ? "null" : testN1String).append("\n")
		
		.append("testDate: ").append(testDate==null ? "null" : df.format(testDate)).append("\n")
		//.append("testNDate: ").append(testNDate==null ? "null" : df.format(testNDate)).append("\n")
		.append("testN1Date: ").append(testN1Date==null ? "null" : df.format(testN1Date)).append("\n")

		.append("testenum").append(testenum.toString()).append("\n")
		.append("Subpojos: ").append(subPojos == null ? 0 : subPojos.size()).append("\n")
		;
		if ( subPojos != null) for (TestSubPojo subPojo : subPojos) subPojo.toString(buffer);
		return buffer.toString();
	}
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}

	public float getTestPFloat() {
		return testPFloat;
	}

	public void setTestPFloat(float testPFloat) {
		this.testPFloat = testPFloat;
	}

	public double getTestPDouble() {
		return testPDouble;
	}

	public void setTestPDouble(double testPDouble) {
		this.testPDouble = testPDouble;
	}

	public int getTestPInt() {
		return testPInt;
	}

	public void setTestPInt(int testPInt) {
		this.testPInt = testPInt;
	}

	public long getTestPLong() {
		return testPLong;
	}

	public void setTestPLong(long testPLong) {
		this.testPLong = testPLong;
	}

	public boolean isTestPBoolean() {
		return testPBoolean;
	}

	public void setTestPBoolean(boolean testPBoolean) {
		this.testPBoolean = testPBoolean;
	}

	public byte[] getTestPBytes() {
		return testPBytes;
	}

	public void setTestPBytes(byte[] testPBytes) {
		this.testPBytes = testPBytes;
	}

	public ArrayList<TestSubPojo> getSubPojos() {
		return subPojos;
	}

	public void setSubPojos(ArrayList<TestSubPojo> subPojos) {
		this.subPojos = subPojos;
	}

	public Float getTestNFloat() {
		return testNFloat;
	}

	public void setTestNFloat(Float testNFloat) {
		this.testNFloat = testNFloat;
	}

	public Double getTestNDouble() {
		return testNDouble;
	}

	public void setTestNDouble(Double testNDouble) {
		this.testNDouble = testNDouble;
	}

	public Integer getTestNInt() {
		return testNInt;
	}

	public void setTestNInt(Integer testNInt) {
		this.testNInt = testNInt;
	}

	public Long getTestNLong() {
		return testNLong;
	}

	public void setTestNLong(Long testNLong) {
		this.testNLong = testNLong;
	}

	public Long getTestN1Long() {
		return testN1Long;
	}

	public void setTestN1Long(Long testN1Long) {
		this.testN1Long = testN1Long;
	}

	public Boolean isTestNBoolean() {
		return testNBoolean;
	}

	public void setTestNBoolean(Boolean testNBoolean) {
		this.testNBoolean = testNBoolean;
	}

	public byte[] getTestNBytes() {
		return testNBytes;
	}

	public void setTestNBytes(byte[] testNBytes) {
		this.testNBytes = testNBytes;
	}

	public Float getTestN1Float() {
		return testN1Float;
	}

	public void setTestN1Float(Float testN1Float) {
		this.testN1Float = testN1Float;
	}

	public Double getTestN1Double() {
		return testN1Double;
	}

	public void setTestN1Double(Double testN1Double) {
		this.testN1Double = testN1Double;
	}

	public Integer getTestN1Int() {
		return testN1Int;
	}

	public void setTestN1Int(Integer testN1Int) {
		this.testN1Int = testN1Int;
	}

	public Boolean isTestN1Boolean() {
		return testN1Boolean;
	}

	public void setTestN1Boolean(Boolean testN1Boolean) {
		this.testN1Boolean = testN1Boolean;
	}

	public byte[] getTestN1Bytes() {
		return testN1Bytes;
	}

	public void setTestN1Bytes(byte[] testN1Bytes) {
		this.testN1Bytes = testN1Bytes;
	}

	public Date getTestDate() {
		return testDate;
	}

	public void setTestDate(Date testDate) {
		this.testDate = testDate;
	}
/*
	public Date getTestNDate() {
		return testNDate;
	}

	public void setTestNDate(Date testNDate) {
		this.testNDate = testNDate;
	}
*/
	public Date getTestN1Date() {
		return testN1Date;
	}

	public void setTestN1Date(Date testN1Date) {
		this.testN1Date = testN1Date;
	}


	public TEST_ENUM getTestenum() {
		return testenum;
	}

	public void setTestenum(TEST_ENUM testenum) {
		this.testenum = testenum;
	}

	public String getTestPString() {
		return testPString;
	}

	public void setTestPString(String testPString) {
		this.testPString = testPString;
	}

	public String getTestNString() {
		return testNString;
	}

	public void setTestNString(String testNString) {
		this.testNString = testNString;
	}

	public String getTestN1String() {
		return testN1String;
	}

	public void setTestN1String(String testN1String) {
		this.testN1String = testN1String;
	}

}
