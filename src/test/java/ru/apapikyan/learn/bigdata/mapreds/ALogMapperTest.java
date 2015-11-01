package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class ALogMapperTest {

	/**
	 * @formatter:off
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text(
		        "ip2 - - " +
		        //IP
		        "[24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 " +
		        //ignore ^^^^
		        "14917 " +
		        //Bytes
		        "\"http://www.stumbleupon.com/refer.php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"");
				//ignore

		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new ALogMapper())
			.withInput(new LongWritable(0), value)
			.withOutput(new Text("ip2"), new IntWritable(14917))
			.runTest();
	}
	
	@Test
	public void parsesBadLogRecord() throws IOException, InterruptedException {
		Text value = new Text(
                "ip2" +
		        //IP
                " - -\"\"\"\"\" [24/Apr/2011:04:20:11 -0400] \"GET /sun_ss5/ HTTP/1.1\" 200 " +
                //ignore
                "\"1sdf49s\"\"df17" +
                //Bytes
                " \"h\"ttp://www\".\"stumbleupon\".\"com\"/\"refer\".php?url=http%3A%2F%host1%2Fsun_ss5%2F\" \"Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"");
                //ignore rest

		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new ALogMapper())
		    .withInput(new LongWritable(0), value)
		    .runTest();
	}
	
	@Test
	public void processRecordWithoutBytes() throws IOException, InterruptedException {
		Text value = new Text(
                "ip86"+
		        //IP
                " - - [24/Apr/2011:09:01:35 -0400] \"GET /sunFAQ/serial/ HTTP/1.0\" 200 " +
                //ignore
                "0" +
                //Bytes
                " \"-\" \"Mozilla/5.0 (compatible; MJ12bot/v1.3.3; http://www.majestic12.co.uk/bot.php?+)\"");
                //ignore rest

		new MapDriver<LongWritable, Text, Text, IntWritable>()
			.withMapper(new ALogMapper())
		    .withInput(new LongWritable(0), value)
		    .withOutput(new Text("ip86"), new IntWritable(0))
		    .runTest();
	}
	/*
	 * @formatter:on
	 */
}
