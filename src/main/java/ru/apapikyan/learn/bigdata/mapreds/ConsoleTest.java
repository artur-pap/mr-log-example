package ru.apapikyan.learn.bigdata.mapreds;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class ConsoleTest {

		public static void main(String[] args) {
		String inputRecord = "ip7 - - [24/Apr/2011:04:31:22 -0400] \"GET /sgi_indy/indy_inside.jpg HTTP/1.1\" 200 23285 \"http://machinecity-hello.blogspot.com/2008_01_01_archive.html\" \"Opera/9.80 (Windows NT 5.1; U; en) Presto/2.8.131 Version/11.10\"";
		String APACHE_LOG_REGXP = "^(\\S+)\\s+(\\S+\\s+\\S+)\\s+\\[([^]]+)\\]\\s+\"(?:GET|POST|HEAD) ([^ ?\"]+)\\??([^ ?\"]+)? HTTP/[0-9.]+\"\\s+([0-9]+)\\s+([-0-9]+)\\s+\"([^\"]*)\"\\s+\"([^\"]*)\"$";

		List<String> matchList = new ArrayList<String>();

		try {
			Pattern regex = Pattern.compile(APACHE_LOG_REGXP, Pattern.DOTALL | Pattern.CASE_INSENSITIVE
			        | Pattern.UNICODE_CASE | Pattern.MULTILINE | Pattern.COMMENTS);

			Matcher regexMatcher = regex.matcher(inputRecord);

			while (regexMatcher.find()) {
				matchList.add(regexMatcher.group(IP_GROUP));
				matchList.add(regexMatcher.group(BYTES_GRPOUP));
			}

		} catch (PatternSyntaxException ex) {
			ex.printStackTrace();
		}

		for (String s : matchList) {
			System.out.println(s);
		}
	}
}
