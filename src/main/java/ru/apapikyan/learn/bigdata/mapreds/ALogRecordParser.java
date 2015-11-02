package ru.apapikyan.learn.bigdata.mapreds;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

public class ALogRecordParser {
	// private static final String APACHE_LOG_REGXP =
	// "^(\\S+)\\s+(\\S+\\s+\\S+)\\s+\\[([^]]+)\\]\\s+\"(?:GET|POST|HEAD) ([^ ?\"]+)\\??([^ ?\"]+)? HTTP/[0-9.]+\"\\s+([0-9]+)\\s+([-0-9]+)\\s+\"([^\"]*)\"\\s+\"([^\"]*)\"$";
	private static final String APACHE_LOG_REGXP = "^(\\S+)\\s+(\\S+\\s+\\S+)\\s+\\[([^]]+)\\]\\s+\"(GET|POST|HEAD) ([^ ?\"]+)\\??([^ ?\"]+)? HTTP/[0-9.]+\"\\s+([0-9]+)\\s+([-0-9]+)\\s+\"(?:[^\"]*)\"\\s+\"(?:[\\(]?)([A-Za-z]*)(?:[^\"]*)\"$";
	public static final int IP_GROUP = 1;
	public static final int BYTES_GRPOUP = 8;
	public static final int BROWSER_GRPOUP = 9;

	private String ip;
	private int bytes = 0;
	private String browser;
	private boolean hasParseError = false;
	private boolean isInvalidBytesValue = false;

	public void parse(String record) {
		String bytesString = "";

		// try {
		Pattern regex = Pattern.compile(APACHE_LOG_REGXP, Pattern.DOTALL | Pattern.CASE_INSENSITIVE
		        | Pattern.UNICODE_CASE | Pattern.MULTILINE | Pattern.COMMENTS);

		Matcher regexMatcher = regex.matcher(record);

		if (regexMatcher.matches()) {
			// ip address
			ip = regexMatcher.group(IP_GROUP);

			// bytes value
			bytesString = regexMatcher.group(BYTES_GRPOUP);
			try {
				bytes = Integer.parseInt(bytesString);
			} catch (NumberFormatException nfe) {
				isInvalidBytesValue = true;
			}

			// browser string. extract first word
			browser = regexMatcher.group(BROWSER_GRPOUP).toLowerCase();
		} else {
			hasParseError = true;
		}
	}

	public void parse(Text record) {
		parse(record.toString());
	}

	public boolean hasParseError() {
		return hasParseError;
	}

	public boolean isInvalidBytesValue() {
		return isInvalidBytesValue;
	}

	public String getIP() {
		return ip;
	}

	public int getBytes() {
		return bytes;
	}

	public Double getBytesDouble() {
		return Double.parseDouble(String.valueOf(bytes));
	}

	public String getBrowser() {
		return browser;
	}
}