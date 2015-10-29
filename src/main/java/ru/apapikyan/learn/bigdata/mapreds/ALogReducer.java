package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class ALogReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

	static Logger LOG = Logger.getLogger(ALogReducer.class);

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
	        InterruptedException {

		float totalBytes = 0f;

		for (IntWritable value : values) {
			totalBytes += value.get();
		}

		context.write(key, new FloatWritable(totalBytes));
	}
}