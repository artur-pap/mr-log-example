package ru.apapikyan.learn.bigdata.mapreds;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

		// CSV, KEY|VALLUE DELIMITER
		jobConf.set(TextOutputFormat.SEPERATOR, ","); // WORKS!!!!
		// jobConf.set("mapred.textoutputformat.separatorText", ","); //DIDN'T
		// WORK

		Job job = new Job(jobConf, "ACCESS LOG COUNTER");
		job.setJarByClass(getClass());

		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SumCountWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);//w/o SumCountWrtable

		job.setNumReduceTasks(0);

		// SequenceFileOutputFormat.setCompressOutput(job, true);
		// SequenceFileOutputFormat.getOutputCompressorClass(job,
		// SnappyCodec.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// //
		// job.setMapperClass(ALogMapper.class);//w/o SumCountWrtable
		job.setMapperClass(ALogMapperWcstmWrtbl.class);
		// //
		// job.setCombinerClass(ALogReducer.class);//w/o SumCountWrtable
		job.setCombinerClass(ALogCombinerWCstmWrtbl.class);
		// //job.setCombinerClass(ALogCombiner.class);
		// //job.setReducerClass(ALogReducer2.class);
		// job.setReducerClass(ALogReducer.class);//w/o SumCountWrtable
		job.setReducerClass(ALogReducerWcstmWrtbl.class);
		// //

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ALogCalcDriver(), args);
		System.exit(exitCode);
	}
}
