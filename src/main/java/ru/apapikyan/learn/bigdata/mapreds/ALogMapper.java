package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class ALogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	static Logger LOG = Logger.getLogger(ALogMapper.class);

	enum LogRecord {
		BAD
	}

	private ALogRecordParser parser = new ALogRecordParser();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	        throws IOException, InterruptedException {
		parser.parse(value);

		if (!parser.hasParseError()) {
			context.write(new Text(parser.getIP()), new IntWritable(parser.getBytes()));
		}

	}
}