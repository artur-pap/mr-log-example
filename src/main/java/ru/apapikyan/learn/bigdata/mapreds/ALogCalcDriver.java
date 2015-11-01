package ru.apapikyan.learn.bigdata.mapreds;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ALogCalcDriver extends Configured implements Tool {

	@SuppressWarnings("deprecation")
    @Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		Configuration jobConf = getConf();
		Job job = new Job(jobConf, "ACCESS LOG COUNTER");
		job.setJarByClass(getClass());
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		//
		jobConf.set(TextOutputFormat.SEPERATOR, ",");
		//// 	
		job.setMapperClass(ALogMapper.class);
		//
		job.setCombinerClass(ALogReducer.class);
		//
		job.setReducerClass(ALogReducer.class);
		////
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ALogCalcDriver(), args);
		System.exit(exitCode);
	}
}
