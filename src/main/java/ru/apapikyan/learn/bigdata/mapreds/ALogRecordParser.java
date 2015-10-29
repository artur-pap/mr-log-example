package ru.apapikyan.learn.bigdata.mapreds;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.io.Text;

public class ALogRecordParser {
	private static final String APACHE_LOG_REGXP = "^(\\S+)\\s+(\\S+\\s+\\S+)\\s+\\[([^]]+)\\]\\s+\"(?:GET|POST|HEAD) ([^ ?\"]+)\\??([^ ?\"]+)? HTTP/[0-9.]+\"\\s+([0-9]+)\\s+([-0-9]+)\\s+\"([^\"]*)\"\\s+\"([^\"]*)\"$";
	private static final int BYTES_GRPOUP = 7;
	private static final int IP_GROUP = 1;

	public void parse(String record) {
		try {
			Pattern regex = Pattern.compile(APACHE_LOG_REGXP, Pattern.DOTALL | Pattern.CASE_INSENSITIVE
			        | Pattern.UNICODE_CASE | Pattern.MULTILINE | Pattern.COMMENTS);

			Matcher regexMatcher = regex.matcher(record);

			if (regexMatcher.matches()) {
				ip = regexMatcher.group(IP_GROUP);
				bytes = Integer.parseInt(regexMatcher.group(BYTES_GRPOUP));
			}

		} catch (PatternSyntaxException ex) {
			ex.printStackTrace();
			hasParseError = true;
		}
	}

	private String ip;
	private int bytes;
	boolean hasParseError = false;

	public void parse(Text record) {
		parse(record.toString());
	}
	
	public boolean hasParseError() {
		return hasParseError;
	}

	public String getIP() {
		return ip;
	}

	public int getBytes() {
		return bytes;
	}
}