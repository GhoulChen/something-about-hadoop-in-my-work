package com.sogou.bin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner<K1, V1> extends Partitioner<K1, V1>{

	@Override
	public int getPartition(K1 key, V1 value, int numPartitions) {
		// TODO Auto-generated method stub
		StringPair sp = (StringPair) key;
		Text tmpValue = new Text(sp.getFirst());
		return (tmpValue.hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
