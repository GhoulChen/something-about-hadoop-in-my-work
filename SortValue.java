import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortValue extends Configured implements Tool {

	public static class ReqMapper extends
			Mapper<Object, Text, StringPair, Text> {

		private StringPair skey = new StringPair();

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, StringPair, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
                //some code
                skey.setFirst("aaa");
                skey.setSecond("bbb");
                context.write(skey, mapOutputValue);
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
		}
	}

	public static class ReqReducer extends
			Reducer<StringPair, Text, Text, Text> {

		@Override
		protected void reduce(StringPair key, Iterable<Text> values,
				Reducer<StringPair, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//some code
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int ret = ToolRunner.run(new SortValue(), args);
		System.exit(ret);
	}

	public static boolean isIn(String str, String[] source) {
		if (source == null || source.length == 0) {
			return false;
		}
		for (int i = 0; i < source.length; i++) {
			if (str.equals(source[i])) {
				return true;
			}
		}
		return false;

	}

	public static class StringPair implements WritableComparable<StringPair> {

		String first;
		String second;

		@Override
		public void readFields(DataInput in) throws IOException {
			first = in.readUTF();
			second = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(first);
			out.writeUTF(second);
		}

		@Override
		public int compareTo(StringPair o) {
			if (first != o.first) {
				return first.compareTo(o.first);
			} else if (second != o.second) {
				return second.compareTo(second);
			} else {
				return 0;
			}
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (this == obj) {
				return true;
			}
			if (obj instanceof StringPair) {
				StringPair sp = (StringPair) obj;
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

	}

	public static class FirstPartitioner<K1, V1> extends Partitioner<K1, V1> {

		@Override
		public int getPartition(K1 key, V1 value, int numPartitions) {
			// TODO Auto-generated method stub
			StringPair sp = (StringPair) key;
			Text tmpValue = new Text(sp.getFirst());
			return (tmpValue.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class GroupingComparator extends WritableComparator {

		protected GroupingComparator() {
			super(StringPair.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StringPair sp1 = (StringPair) a;
			StringPair sp2 = (StringPair) b;
			return sp1.getFirst().compareTo(sp2.getFirst());
		}

	}

	public static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(StringPair.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			StringPair sp1 = (StringPair) a;
			StringPair sp2 = (StringPair) b;
			int cmp = sp1.getFirst().compareTo(sp2.getFirst());
			if (cmp != 0) {
				return cmp;
			}
			return -sp1.getSecond().compareTo(sp2.getSecond());
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job(getConf(), "SortValue");

		job.setJarByClass(SortValue.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(ReqMapper.class);
		job.setReducerClass(ReqReducer.class);
		job.setNumReduceTasks(30);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(StringPair.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupingComparator.class);
		job.setPartitionerClass(FirstPartitioner.class);

		boolean flag = job.waitForCompletion(true);
		System.err.println("output: " + out);
		if (flag && job.isSuccessful()) {
			return 0;
		} else {
			return 2;
		}
	}

}
