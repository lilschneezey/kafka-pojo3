package io.schneezey.pojo;

import java.util.ArrayList;

import org.apache.avro.reflect.Nullable;

/*

 */
public class TestSubPojo {
	

	// Primitives
	private float subPFloat = (float) 0.0;
	private double subPDouble = 0.0;
	private int subPInt = 2000;
	private long subPLong = 3000L;
	private boolean subPBoolean = true;	
	private byte[] subPBytes = new byte[0];
	private String subPString = new String();
	
	@Nullable
	private ArrayList<TestSub2Pojo> subjos = null; 

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		return toString(buffer).toString();
	}
	public StringBuffer toString(StringBuffer buffer) {
		buffer.append("  subFloat: ").append(subPFloat).append("\n")
		.append("  subDouble: ").append(subPDouble).append("\n")
		.append("  subInt: ").append(subPInt).append("\n")
		.append("  subLong: ").append(subPLong).append("\n")
		.append("  subBoolean: ").append(subPBoolean).append("\n")
		.append("  subBytes: ").append(subPBytes==null ? "null" : subPBytes).append("\n")
		.append("  subsubRecords: ").append(subjos == null ? 0 : subjos.size()).append("\n")
		.append("  subString: ").append(subPString).append("\n")
		;
		if ( subjos != null) for (TestSub2Pojo subjo : subjos) subjo.toString(buffer);
		return buffer;
	}

	public ArrayList<TestSub2Pojo> getSubjos() {
		return subjos;
	}

	public void setSubjos(ArrayList<TestSub2Pojo> subjos) {
		this.subjos = subjos;
	}
	public float getSubPFloat() {
		return subPFloat;
	}
	public void setSubPFloat(float subPFloat) {
		this.subPFloat = subPFloat;
	}
	public double getSubPDouble() {
		return subPDouble;
	}
	public void setSubPDouble(double subPDouble) {
		this.subPDouble = subPDouble;
	}
	public int getSubPInt() {
		return subPInt;
	}
	public void setSubPInt(int subPInt) {
		this.subPInt = subPInt;
	}
	public long getSubPLong() {
		return subPLong;
	}
	public void setSubPLong(long subPLong) {
		this.subPLong = subPLong;
	}
	public boolean isSubPBoolean() {
		return subPBoolean;
	}
	public void setSubPBoolean(boolean subPBoolean) {
		this.subPBoolean = subPBoolean;
	}
	public byte[] getSubPBytes() {
		return subPBytes;
	}
	public void setSubPBytes(byte[] subPBytes) {
		this.subPBytes = subPBytes;
	}
	public String getSubPString() {
		return subPString;
	}
	public void setSubPString(String subPString) {
		this.subPString = subPString;
	}

}