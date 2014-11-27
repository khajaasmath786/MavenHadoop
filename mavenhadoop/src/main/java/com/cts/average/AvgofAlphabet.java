package com.cts.average;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cts.average.*;
import com.cts.count.WordCountReducer;


public class AvgofAlphabet
{

    public static void main(String[] args) throws Exception
    {
	if (args.length != 2)
	{
	    System.out.printf("Usage: WordCount <input dir> <output dir>\n");
	    System.exit(-1);
	}

	/*
	 * 1.Instantiates the job.Rest of the job information is uploaded from folder etc/hadoop/conf of the cluster.
	 */
	Job job = new Job();
	job.setJarByClass(AvgofAlphabet.class);
	job.setJobName("Word Count");

	/* * 2.Specify the Input and OutPutPath. This can be local or HDFS.
	 * 
	 * Reads all the files from the HDFS directories or can be at least text file. Default is text file so no need to mention txt extension.
	 * 
	 * Directories with _ or - or omitted.
	 * 
	 * More Input files can be added from different locations using setInputPaths
	 */
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	// FileInputFormat.setInputPaths(job, new Path(args[5])); --> Add Multiple files using this method.
	// FileInputFormat.setInputPaths(job, new Path(args[7])); --> Add Multiple files using this method.
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	/* 3.Specify the Input and Output formatters. Default input is text. Default output plain text. */
	/*
	 * job.setInputFormatClass(InputFormat.class);
	 * 
	 * job.setOutputFormatClass(TextOutputFormat.class);
	 */

	/* 4.Specify the Mapper,Reducer and Combiner classes. */
	job.setMapperClass(AvgofAlphabetMapper.class);
	//job.setCombinerClass(AvgofAlphabetReducer.class);
	job.setReducerClass(AvgofAlphabetReducer.class);

	/* 5. Specify the output of Mapper i.e. output key and output value of the Mapper. Output value should always be Writable */
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	


	/* 6. Specify the output of Reducer i.e. output key and output value of the Reducer.Output value should always be Writable */
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);

	/*
	 * 7.Job can be submitted either by job.waitForCompletion(true) or job.submit().
	 * 
	 * WaitForCompletion will wait until the job is finished but using submit job will not wait but process other actions.
	 */
	boolean success = job.waitForCompletion(true);
	// job.submit(); --> Job can be submitted using job.submit also.
	System.exit(success ? 0 : 1);

	// Context object is used to store intermediate values.
    }
}
