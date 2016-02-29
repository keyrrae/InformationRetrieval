
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.*;
import java.util.regex.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LogAnalyzer {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		PrintWriter writer = new PrintWriter("result.txt", "UTF-8");
		
		
		if (args.length != 2) {
			System.err.println("Usage: loganalyzer <in> <out>");
			System.exit(2);
		}
		long unixTime = System.currentTimeMillis() / 1000L;
		writer.println("Jobs starts at:"+unixTime);
		
		Job job_dailyUser = new Job(conf, "dailyUser");
		Job job_dailyView = new Job(conf, "dailyView");
		Job job_urlFrequency = new Job(conf, "urlFrequency");
		Job job_userFrequency = new Job(conf, "userFrequency");
		
		job_dailyUser.setJarByClass(LogAnalyzer.class);
		job_dailyUser.setMapperClass(MapDailyUser.class);
		job_dailyUser.setReducerClass(ReduceDailyUser.class);
		job_dailyUser.setMapOutputKeyClass(Text.class);
		job_dailyUser.setMapOutputValueClass(Text.class);
		job_dailyUser.setOutputKeyClass(Text.class);
		job_dailyUser.setOutputValueClass(IntWritable.class);
		for (int i=1; i<2; i++)
			FileInputFormat.addInputPath(job_dailyUser, new Path(args[0]+"/apache"+i+".splunk.com/access_combined.log"));
		FileOutputFormat.setOutputPath(job_dailyUser, new Path(args[1]+"/dailyUser"));
		
		job_dailyView.setJarByClass(LogAnalyzer.class);
		job_dailyView.setMapperClass(MapDailyView.class);
		job_dailyView.setReducerClass(Reduce.class);
		job_dailyView.setOutputKeyClass(Text.class);
		job_dailyView.setOutputValueClass(IntWritable.class);
		for (int i=1; i<2; i++)
			FileInputFormat.addInputPath(job_dailyView, new Path(args[0]+"/apache"+i+".splunk.com/access_combined.log"));
		FileOutputFormat.setOutputPath(job_dailyView,new Path(args[1]+"/dailyView"));

		job_urlFrequency.setJarByClass(LogAnalyzer.class);
		job_urlFrequency.setMapperClass(MapUrlFrequency.class);
		job_urlFrequency.setReducerClass(Reduce.class);
		job_urlFrequency.setMapOutputKeyClass(Text.class);
		job_urlFrequency.setMapOutputValueClass(IntWritable.class);
		job_urlFrequency.setOutputKeyClass(IntWritable.class);
		job_urlFrequency.setOutputValueClass(Text.class);
		for (int i=1; i<2; i++)
			FileInputFormat.addInputPath(job_urlFrequency, new Path(args[0]+"/apache"+i+".splunk.com/access_combined.log"));
		FileOutputFormat.setOutputPath(job_urlFrequency,new Path(args[1]+"/urlFrequency"));

		job_userFrequency.setJarByClass(LogAnalyzer.class);
		job_userFrequency.setMapperClass(MapUserFrequency.class);
		job_userFrequency.setReducerClass(Reduce.class);
		job_userFrequency.setMapOutputKeyClass(Text.class);
		job_userFrequency.setMapOutputValueClass(IntWritable.class);
		job_userFrequency.setOutputKeyClass(IntWritable.class);
		job_userFrequency.setOutputValueClass(Text.class);
		for (int i=1; i<2; i++)
			FileInputFormat.addInputPath(job_userFrequency, new Path(args[0]+"/apache"+i+".splunk.com/access_combined.log"));
		FileOutputFormat.setOutputPath(job_userFrequency,new Path(args[1]+"/userFrequency"));
		
		boolean complete=job_dailyUser.waitForCompletion(true)&&job_dailyView.waitForCompletion(true)&&job_urlFrequency.waitForCompletion(true)&&job_userFrequency.waitForCompletion(true);

		//         HashMap<String, Integer> hmap = new HashMap<String, Integer>();
		// if(complete){
		// 	FileReader f = new FileReader(args[1]+"/urlFrequency/part-r-00000");
		// }
		//
		// writer.println("reading in file");
		//         BufferedReader br = new BufferedReader(f);
		// 	          String line;
		// 	    	    while ((line = br.readLine()) != null) {
		// 	            String delims = "[\\s]+";
		// 	      			String[] parts = line.split(delims);
		// 	      		  hmap.put(parts[0],Integer.parseInt(parts[1]));
		// 	  writer.println(parts[0]);
		// 	          }
		//         Map<String, Integer> map = sortByValues(hmap);
		//         Set set = map.entrySet();
		//         Iterator iterator = set.iterator();
		// int i = 0;
		//         while(iterator.hasNext() && i < 5 ) {
		//              Map.Entry me = (Map.Entry)iterator.next();
		//              writer.println(me.getKey() + ": " + me.getValue());
		// 	 ++i;
		//         }
		unixTime = System.currentTimeMillis() / 1000L;
		writer.println("Jobs ends at:"+unixTime);
		writer.close();
		System.exit(complete? 0 : 1);
	}

  private static HashMap sortByValues(HashMap map) { 
       List list = new LinkedList(map.entrySet());
       // Defined Custom Comparator here
       Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
               return ((Comparable) ((Map.Entry) (o2)).getValue())
                  .compareTo(((Map.Entry) (o1)).getValue());
            }
       });

       // Here I am copying the sorted list in HashMap
       // using LinkedHashMap to preserve the insertion order
       HashMap sortedHashMap = new LinkedHashMap();
       for (Iterator it = list.iterator(); it.hasNext();) {
              Map.Entry entry = (Map.Entry) it.next();
              sortedHashMap.put(entry.getKey(), entry.getValue());
       } 
       return sortedHashMap;
  }
}
