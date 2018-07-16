package com.sogou.bin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{

	protected GroupingComparator() {
		super(StringPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		StringPair sp1 = (StringPair) a;
		StringPair sp2 = (StringPair) b;
		return sp1.getFirst().compareTo(sp2.getFirst());
	}
	
	

}
