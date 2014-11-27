package com.cts.logfileanaylsis;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cts.count.WordCountReducer;

public class TestLogFileAnaylsisWithMapandReducers
{
	  MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	  MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	//MapReduceDriver<MapInputkey,MapInputKeyValue,MapOutPutKey,MapOutputkeyValue,RecuceOutPutKey,ReducceOutputKeyValue> mapreduceDriver;
	
	//--> use SetUp and test methods
	
	@Before
	public void setUp()
	{
		// Test for only mapper
		mapDriver = new MapDriver<LongWritable,Text,Text,IntWritable>();
		LogFileAnaylsisMapper mapper=new LogFileAnaylsisMapper();
		mapDriver.setMapper(mapper);
		
		// Test for only Reducer
		reduceDriver= new ReduceDriver<Text,IntWritable,Text,IntWritable>();
		WordCountReducer reducer= new WordCountReducer();
		reduceDriver.setReducer(reducer);
		
		// Test for Map and Reduce
		mapReduceDriver=new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		//-------------------> ********************** Set Both mapper and reducer here -------------->
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		
	}
	
		 /*
	   * Test the mapper.
	   */
	  @Test
	  public void testMapper() {

	    /*
	     * For this test, the mapper's input will be "1 cat cat dog" 
	     */
	    mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));

	    /*
	     * The expected output is "cat 1", "cat 1", and "dog 1".
	     */
	    mapDriver.withOutput(new Text("cat"), new IntWritable(1));
	    mapDriver.withOutput(new Text("cat"), new IntWritable(1));
	    mapDriver.withOutput(new Text("dog"), new IntWritable(1));

	    /*
	     * Run the test.
	     */
	    mapDriver.runTest();
	  }

	  /*
	   * Test the reducer.
	   */
	  @Test
	  public void testReducer() {

	    List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));

	    /*
	     * For this test, the reducer's input will be "cat 1 1".
	     */
	    reduceDriver.withInput(new Text("cat"), values);

	    /*
	     * The expected output is "cat 2"
	     */
	    reduceDriver.withOutput(new Text("cat"), new IntWritable(2));

	    /*
	     * Run the test.
	     */
	    reduceDriver.runTest();
	  }

	  /*
	   * Test the mapper and reducer working together.
	   */
	  @Test
	  public void testMapReduce() {

	    /*
	     * For this test, the mapper's input will be "1 cat cat dog" 
	     */
	    mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));

	    /*
	     * The expected output (from the reducer) is "cat 2", "dog 1". 
	     */
	    mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
	    mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));

	    /*
	     * Run the test.
	     */
	    mapReduceDriver.runTest();
	  }
	  @Test
	  public void testMapandReduce() {

	    /*
	     * For this test, the mapper's input will be "1 cat cat dog" 
	     */
	    mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));

	    /*
	     * The expected output (from the reducer) is "cat 2", "dog 1". 
	     */
	    mapReduceDriver.withOutput(new Text("cat"), new IntWritable(2));
	    mapReduceDriver.withOutput(new Text("dog"), new IntWritable(1));

	    /*
	     * Run the test.
	     */
	    mapReduceDriver.runTest();
	  }
}
