package io.schneezey.pojo;

/*

 */
public class TestSub2Pojo {
	

	// Primitives
	private float subPFloat = (float) 0.0;
	private double subPDouble = 0.0;
	private int subPInt = 2000;
	private long subPLong = 3000L;
	private boolean subPBoolean = true;	
	private byte[] subPBytes = new byte[0];
	private String subPString = new String();

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		return toString(buffer).toString();
	}

	public StringBuffer toString(StringBuffer buffer) {
		buffer.append("    subsubFloat: ").append(subPFloat).append("\n")
		.append("    subsubDouble: ").append(subPDouble).append("\n")
		.append("    subsubInt: ").append(subPInt).append("\n")
		.append("    subsubLong: ").append(subPLong).append("\n")
		.append("    subsubBoolean: ").append(subPBoolean).append("\n")
		.append("    subsubBytes: ").append(subPBytes==null ? "null" : subPBytes).append("\n")
		.append("    subsubString: ").append(subPString).append("\n")
		;
		return buffer;
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