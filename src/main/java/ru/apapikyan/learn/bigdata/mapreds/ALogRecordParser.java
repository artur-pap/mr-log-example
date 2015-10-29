package ru.apapikyan.learn.bigdata.mapreds;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

public class ALogRecordParser {
	private static final String APACHE_LOG_REGXP = "^(\\S+)\\s+(\\S+\\s+\\S+)\\s+\\[([^]]+)\\]\\s+\"(?:GET|POST|HEAD) ([^ ?\"]+)\\??([^ ?\"]+)? HTTP/[0-9.]+\"\\s+([0-9]+)\\s+([-0-9]+)\\s+\"([^\"]*)\"\\s+\"([^\"]*)\"$";
	public static final int BYTES_GRPOUP = 7;
	public static final int IP_GROUP = 1;

	public void parse(String record) {
		String bytesString  = "";
		
		try {
			Pattern regex = Pattern.compile(APACHE_LOG_REGXP, Pattern.DOTALL | Pattern.CASE_INSENSITIVE
			        | Pattern.UNICODE_CASE | Pattern.MULTILINE | Pattern.COMMENTS);

			Matcher regexMatcher = regex.matcher(record);

			if (regexMatcher.matches()) {
				ip = regexMatcher.group(IP_GROUP);
				
				bytesString = regexMatcher.group(BYTES_GRPOUP);
				bytes = Integer.parseInt(bytesString);
			} else {
				hasParseError = true;
			}

		} catch (Exception ex) {
			System.out.println("Error parsing [" +ip+ "], ["+ bytesString +"] from record: " + record);
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