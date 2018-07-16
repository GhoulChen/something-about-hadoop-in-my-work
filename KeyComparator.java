package com.sogou.bin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator{

	protected KeyComparator() {
		super(StringPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		StringPair sp1 = (StringPair) a;
		StringPair sp2 = (StringPair) b;
		int cmp = sp1.getFirst().compareTo(sp2.getFirst());
		if (cmp != 0) {
			return cmp;
		}
		return -sp1.getSecond().compareTo(sp2.getSecond());
	}
}
