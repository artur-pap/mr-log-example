package ru.apapikyan.learn.bigdata.mapreds;
//package ru.apapikyan.learn.bigdata.mapreds;
//
//import java.io.IOException;
//
//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//public class ALogCombiner extends Reducer<Text, IntWritable, Text, ArrayWritable> {
//	@Override
//	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
//	        InterruptedException {
//
//		int sum = 0;
//		int count = 0;
//
//		for (IntWritable value : values) {
//			sum += value.get();
//			count++;
//		}
//
//		IntWritable[] innerArray = { new IntWritable(sum), new IntWritable(count) };
//		ArrayWritable arr = new ArrayWritable(IntWritable.class, innerArray);
//
//		context.write(key, arr);
//	}
//}