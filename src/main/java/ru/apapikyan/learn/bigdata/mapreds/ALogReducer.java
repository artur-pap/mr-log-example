package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class ALogReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

	static Logger LOG = Logger.getLogger(ALogReducer.class);
	private final static FloatWritable one = new FloatWritable(1);
	private Text word = new Text();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
	        InterruptedException {
		
		int maxValue = Integer.MIN_VALUE;
		
		long totalBytes = 0L;

		for (IntWritable value : values) {
			totalBytes += value.get();
		}

		context.write(key, new FloatWritable(maxValue));
	}
}