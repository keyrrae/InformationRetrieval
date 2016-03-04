
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.*;


public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable total = new IntWritable();

        @Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
			int sum = 0;
		    for (IntWritable value : values) {
		    	sum += value.get();
		    }
		    total.set(sum);
		    context.write(key, total);
		}
}
	

