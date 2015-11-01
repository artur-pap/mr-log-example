package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class ALogReducerTest {

	/*
	 * @formatter:off
	 */
	@Test
	public void returnsAvgBytesPerIP() throws IOException, InterruptedException {

		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		        .withReducer(new ALogReducer())
		        .withInput(new Text("IP7678"), Arrays.asList(new IntWritable(234), new IntWritable(12456), new IntWritable(236789)))
		        .withOutput(new Text("IP7678"), new IntWritable(249479)).runTest();
	}

	@Test
	public void returnsTotalBytesPerIP() throws IOException, InterruptedException {

		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		        .withReducer(new ALogReducer())
		        .withInput(new Text("IP7678"), Arrays.asList(new IntWritable(234), new IntWritable(12456), new IntWritable(236789)))
		        .withOutput(new Text("IP7678"), new IntWritable(249479)).runTest();
	}
	/*
	 * @formatter:on
	 */
}
