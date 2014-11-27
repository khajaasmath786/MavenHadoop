package com.cts.lineNumber;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;

public class LineIndex {
	 public static void main(String[] args) throws IOException { 
		 JobConf conf = new JobConf(LineIndex.class); 
		 conf.setJobName("LineIndexer"); 
		 // The keys are words (strings): 
		 conf.setOutputKeyClass(Text.class); 
		 // The values are offsets+line (strings): 
		 conf.setOutputValueClass(Text.class); 
		 
		 conf.setMapperClass(LineIndexerMapper.class); 
		 conf.setReducerClass(LineIndexerReducer.class); 
		 if (args.length < 2) { 
		 System.out.println("Usage: LineIndexer <input path> <output path>"); 
		 System.exit(0); 
		 } 
		 
		 FileInputFormat.setInputPaths(conf, new Path(args[0]));
		 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 
		
		 /*conf.setInputPath(new Path(args[0])); 
		 conf.setOutputPath(new Path(args[1])); */
		 
		 JobClient.runJob(conf); 
		 } 


}
