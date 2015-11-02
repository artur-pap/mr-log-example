package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ALogReducerWcstmWrtbl extends Reducer<Text, SumCountWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<SumCountWritable> values, Context context) throws IOException,
	        InterruptedException {

		SumCountWritable totalSumCount = new SumCountWritable();

		for (SumCountWritable sumCount : values) {
			// sums all came from different mappers|combiners
			// thisl all make sense only in case mapper > 1
			// as a mapper -> one combiner
			totalSumCount.addSumCount(sumCount);
		}

		Double sum = totalSumCount.getSum().get();
		Integer count = totalSumCount.getCount().get();

		String result = "";
		result = String.format("%s,%s", sum / count, sum);

		context.write(key, new Text(result));

	}
}