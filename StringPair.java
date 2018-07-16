package com.sogou.bin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringPair implements WritableComparable<StringPair> {
	
	String first;
	String second;
	
	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		first = arg0.readUTF();
		second = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(first);
		arg0.writeUTF(second);
	}

	@Override
	public int compareTo(StringPair arg0) {
		// TODO Auto-generated method stub
		if (first != arg0.first) {
			return first.compareTo(arg0.first);
		} else if (second != arg0.second) {
			return second.compareTo(arg0.second);
		} else {
			return 0;
		}
	}

	@Override
	public boolean equals(Object arg0) {
		// TODO Auto-generated method stub
		if (arg0 == null) {
			return false;
		}
		if (this == arg0) {
			return true;
		}
		if (arg0 instanceof StringPair) {
			StringPair sp = (StringPair) arg0;
			return sp.first == first && sp.second == second;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return first;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}
	
}
