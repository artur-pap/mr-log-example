package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ALogMapperWcstmWrtbl extends Mapper<LongWritable, Text, Text, SumCountWritable> {

	private ALogRecordParser parser = new ALogRecordParser();

	enum LogRecord {
		BAD, INAVLID_BYTES_VALUE
	}

	enum Browser {
		aghaven, baiduspider, blackberry, compatible, contype, coralwebprx, docomo, envolk, facebookexternalhit, findlinks, gigabot, googlebot, gosospider, holmes, hot, huaweisymantecspider, ia, ichiro, jakarta, java, libwww, linkscan, mediapartners, mlbot, mot, mozilla, msnbot, netestate, nokia, nph, opera, perlcrawler, python, safari, saladspoon, seznambot, sitebar, sogou, sosoimagespider, sosospider, talktalk, turnitinbot, urlresolver, webmoney, wget, yahoo, yeti, zmeu
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parser.parse(value);

		if (!parser.hasParseError()) {
			String browser = parser.getBrowser();
			//
			if (!browser.isEmpty()) {
				Browser b;
				b = Browser.valueOf(browser);
				context.getCounter(b).increment(1);
			}
			if (parser.isInvalidBytesValue()) {
				context.getCounter(LogRecord.INAVLID_BYTES_VALUE).increment(1);
			}
			//
			SumCountWritable scw = new SumCountWritable(parser.getBytesDouble(), 1);
			context.write(new Text(parser.getIP()), scw);
		} else {
			System.err.println("Ignoring possibly corrupt input: " + value);
			context.getCounter(LogRecord.BAD).increment(1);
		}
	}
}