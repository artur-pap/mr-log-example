package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import ru.apapikyan.learn.bigdata.mapreds.ALogMapper;
import ru.apapikyan.learn.bigdata.mapreds.ALogReducer;

public class ALogMapperTest {

	/**
	 * @formatter:off
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		String inpString = "ip2 - - ";
		Text value = new Text(
		        "ip2 - - " +
		        // IP ^^^^
		        "[24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 " +
		        // ignore ^^^^
		        "14917 " +
		        // Bytes ^^^^
		        "\"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"");
				// ignore rest
		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new ALogMapper())
			.withInput(new LongWritable(0), value)
			.withOutput(new Text("ip2"), new IntWritable(14917))
			.runTest();
	}
	/**
	 * 
	 * @formatter:on
	 *
	 */

	/*
	@Test
	public void parsesBadLogRecord() throws IOException, InterruptedException {
		String inpString = "ip2 - - ";
		Text value = new Text(
		        "ip2 - - " +
		        // IP ^^^^
		        "[24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 " +
		        // ignore ^^^^
		        "14917 " +
		        // Bytes ^^^^
		        "\"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"");
				// ignore rest
		
		Counters counters = new Counters();
		
		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new ALogMapper())
			.withInput(new LongWritable(0), value)
			.withOutput(new Text("ip2"), new IntWritable(14917))
			.runTest();
		
		Counter c = counters.findCounter(ALogMapper.LogRecord.BAD);
		assertThat(c.getValue(), is(1L));
	}
	*/
}
