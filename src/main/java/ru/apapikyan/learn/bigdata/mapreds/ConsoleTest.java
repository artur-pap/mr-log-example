package ru.apapikyan.learn.bigdata.mapreds;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.io.IntWritable;

public class ConsoleTest {

	private static BufferedReader br;

	enum browsers {
		chrome, mozilla
	}

	public static void main(String[] args) throws IOException {
		String inputRecord = "ip43 - - [24/Apr/2011:06:37:01 -0400] \"GET /sun_ss10/ HTTP/1.1\" 200 13596 \"-\" \"Java/1.6.0_04\"";
		final String APACHE_LOG_REGXP = "^(\\S+)\\s+(\\S+\\s+\\S+)\\s+\\[([^]]+)\\]\\s+\"(GET|POST|HEAD) ([^ ?\"]+)\\??([^ ?\"]+)? HTTP/[0-9.]+\"\\s+([0-9]+)\\s+([-0-9]+)\\s+\"(?:[^\"]*)\"\\s+\"(?:[\\(]?)([A-Za-z]*)(?:[^\"]*)\"$";

		IntWritable iw1 = new IntWritable(234);
		IntWritable iw2 = new IntWritable(12456);
		IntWritable iw3 = new IntWritable(236789);

		for (browsers br : browsers.values()) {
			System.out.println(br.name() + "-" + br.ordinal() + "-" + br.name().equals("Chrome".toLowerCase()));
		}
		// System.exit(0);

		File f = new File("000000.log");

		List<String> matchList = new ArrayList<String>();

		FileInputStream fs = null;

		fs = null;

		try {
			fs = new FileInputStream(f);

			br = new BufferedReader(new InputStreamReader(fs));
			String line = "";
			SortedSet<String> brws = new TreeSet<String>();

			while ((line = br.readLine()) != null) {

				ALogRecordParser parser = new ALogRecordParser();
				parser.parse(line);

				System.out.println(parser.getIP() + " - " + parser.getBytes() + " - " + parser.getBrowser() + " - "
				        + parser.isInvalidBytesValue() + " - " + parser.hasParseError());

				if (parser.getBrowser() != null && !brws.contains(parser.getBrowser())) {
					brws.add(parser.getBrowser());
				}
			}

			//Iterator<String> itt = brws.iterator();
			// while (itt.hasNext()) {
			// System.out.println("0browser: " + itt.next());
			// }
			//
			// for (String browser : brws) {
			// System.out.println("browser: " + browser);
			// }
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			fs.close();
		}

		System.exit(0);

		float res = iw1.get() + iw2.get() + iw3.get();
		System.out.println((iw1.get() + iw2.get() + iw3.get()) / 3 + " - " + res);
		// System.exit(0);
		// String inputRecord =
		// "ip7 - - [24/Apr/2011:04:31:22 -0400] \"GET /sgi_indy/indy_inside.jpg HTTP/1.1\" 200 23285 \"http://machinecity-hello.blogspot.com/2008_01_01_archive.html\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.8.131 Version/11.10\"";
		// String inputRecord =
		// "ip43 - - [24/Apr/2011:06:37:01 -0400] \"GET /sun_ss10/ HTTP/1.1\" 200 13596 \"-\" \"Java/1.6.0_04\"";
		// String APACHE_LOG_REGXP =
		// "^(\\S+)\\s+(\\S+\\s+\\S+)\\s+\\[([^]]+)\\]\\s+\"(?:GET|POST|HEAD) ([^ ?\"]+)\\??([^ ?\"]+)? HTTP/[0-9.]+\"\\s+([0-9]+)\\s+([-0-9]+)\\s+\"([^\"]*)\"\\s+\"([^\"]*)\"$";

		matchList = new ArrayList<String>();

		try {
			Pattern regex = Pattern.compile(APACHE_LOG_REGXP, Pattern.DOTALL | Pattern.CASE_INSENSITIVE
			        | Pattern.UNICODE_CASE | Pattern.MULTILINE | Pattern.COMMENTS);

			Matcher regexMatcher = regex.matcher(inputRecord);

			while (regexMatcher.find()) {
				matchList.add(regexMatcher.group(ALogRecordParser.IP_GROUP));
				matchList.add(regexMatcher.group(ALogRecordParser.BYTES_GRPOUP));
			}

		} catch (PatternSyntaxException ex) {
			ex.printStackTrace();
		}

		for (String s : matchList) {
			System.out.println(s);
		}
	}
}
