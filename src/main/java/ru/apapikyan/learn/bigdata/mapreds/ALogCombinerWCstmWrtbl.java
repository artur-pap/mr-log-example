package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ALogCombinerWCstmWrtbl extends Reducer<Text, SumCountWritable, Text, SumCountWritable> {

	@Override
	protected void reduce(Text key, Iterable<SumCountWritable> values, Context context) throws IOException,
	        InterruptedException {
		SumCountWritable scwCombined = new SumCountWritable();

		for (SumCountWritable value : values) {
			scwCombined.addSumCount(value);
		}

		context.write(key, scwCombined);
	}
}