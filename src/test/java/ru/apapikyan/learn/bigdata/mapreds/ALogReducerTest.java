package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Test;

public class ALogReducerTest {

	@Test
	public void returnsAvgBytesPerIP() throws IOException, InterruptedException {
		org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> somreducer = null;
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		        .withReducer(somreducer)
		        .withInput(new Text("IP7678"),
		                Arrays.asList(new IntWritable(234), new IntWritable(12456), new IntWritable(236789)))
		        .withOutput(new Text("IP7678"), new IntWritable(249245)).runTest();
	}

	@Test
	public void returnsTotalBytesPerIP() throws IOException, InterruptedException {

		org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> somreducer = null;
		new ReduceDriver<Text, IntWritable, Text, IntWritable>().withReducer(somreducer)
		        .withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
		        .withOutput(new Text("1950"), new IntWritable(249245)).runTest();
	}
}
