package com.cts.readfilesfromhdfs;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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

public class ReadHDFSFileMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	@Override
	public void setup(Context context)
	{
		Path[] cacheFiles;
		try {
			System.out.println("Reading and Writing Files in HDFS--->");
			readHDFSFile(context);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void readHDFSFile(Context context) {
	    
		 FSDataInputStream in = null;
		 BufferedReader br = null;
		 try {
		  FileSystem fs = FileSystem.get(context.getConfiguration());
		  Path path = new Path("Input/ReadHDFSFile/ReadHDFSFile");
		  in = fs.open(path);
		  br  = new BufferedReader(new InputStreamReader(in));
		 } catch (FileNotFoundException e1) {
		  e1.printStackTrace();
		  System.out.println("read from distributed cache: file not found!");
		 } catch (IOException e1) {
		  e1.printStackTrace();
		  System.out.println("read from distributed cache: IO exception!");
		 }
		 try {
			 HashMap idmap = new HashMap< String, String >();
			 System.out.println("Reading file that is stored in HDFS");
		  String line = "";
		  while ( (line = br.readLine() )!= null) {
		   String[] arr = line.split("\t");
		   System.out.println(line);
		   if (arr.length == 2)
		    idmap.put(arr[1], arr[0]);
		  }
		  in.close();
		 } catch (IOException e1) {
		  e1.printStackTrace();
		  System.out.println("read from distributed cache: read length and instances");
		 }
		   }
		
    /*
     * The map method runs once for each line of text in the input file. The method receives a key of type LongWritable, a value of type Text, and a Context object.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {

	
	String line = value.toString();
	//String line = value.toString(); --> This get count without Ignore
	/*
	 * The line.split("\\W+") call uses regular expressions to split the line up by non-word characters.
	 * 
	 * If you are not familiar with the use of regular expressions in Java code, search the web for "Java Regex Tutorial."
	 */
	
	// Use the string tokeniser
	
	/*StringTokenizer words= new StringTokenizer( value.toString().toLowerCase().replaceAll("\\p {Punct}|\\d", ""));
	while (words.hasMoreElements())
	{
		String word=words.nextToken();
	}*/
	
	
	for (String word : line.split("\\W+"))
	{
	    if (word.length() > 0)
	    {

		/*
		 * Call the write method on the Context object to emit a key and a value from the map method.
		 */
		context.write(new Text(word.toLowerCase()), new IntWritable(1));
		//context.write(new Text(word)), new IntWritable(1));  --> Gets duplicates without case
	    }
	}
    }
}