package com.cts.custominputandoutputformat;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

    /*
     * The map method runs once for each line of text in the input file. The method receives a key of type LongWritable, a value of type Text, and a Context object.
     */
	public void map(LongWritable key, Text value,Context context) throws java.io.IOException ,InterruptedException
	{
	    String lines = value.toString();
	    String []lineArr = lines.split("\n");
	    int lcount = lineArr.length;
	    context.write(new Text(new Integer(lcount).toString()),new IntWritable(1));
	 }
}