
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.*;

public class MapDailyUser extends Mapper<Object, Text, Text, Text> {
		private Text day = new Text();
		private Text ip = new Text();
		private String logEntryPattern="^([\\d.]+)[^\\[]+\\[([\\w/]+)[\\d:]+\\] \"(?:GET|POST)\\s([^\\s]+)[^\"]+\".+";
		private Pattern p = Pattern.compile(logEntryPattern);
		
        @Override
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
			String[] entries = value.toString().split("\r?\n"); 
			for (int i=0, len=entries.length; i<len; i+=1) {
				Matcher matcher = p.matcher(entries[i]);
				if (matcher.find()) {
					day.set(matcher.group(2));
					ip.set(matcher.group(1));
					context.write(day,ip);
				}
			}
		}
}

