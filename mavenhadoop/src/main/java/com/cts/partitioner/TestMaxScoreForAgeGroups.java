package com.cts.partitioner;
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

public class TestMaxScoreForAgeGroups
{
	  MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
	//MapReduceDriver<MapInputkey,MapInputKeyValue,MapOutPutKey,MapOutputkeyValue,RecuceOutPutKey,ReducceOutputKeyValue> mapreduceDriver;
	
	//--> use SetUp and test methods
	
	@Before
	public void setUp()
	{
		
		MaxScoreForAgeGroupsMapper mapper=new MaxScoreForAgeGroupsMapper();		
		MaxScoreForAgeGroupsReducer reducer= new MaxScoreForAgeGroupsReducer();
		
		
		// Test for Map and Reduce
		mapReduceDriver=new MapReduceDriver<LongWritable,Text,Text,Text,Text,Text>();
		//-------------------> ********************** Set Both mapper and reducer here -------------->
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
		
	}
	
	   /*
	   * Test the mapper.
	   */
	 @Test
	  public void testMapandReduce() {

	    /*
	     * For this test, the mapper's input will be "Asmath Asmath is Learning Hadoop" 
	     */
	    mapReduceDriver.withInput(new LongWritable(1), new Text("Connor	25	male	27"));

	    /*
	     * The expected output (from the reducer) is "Asmath 2", "Learning 1" ,"is 1", "Hadoop 1" 
	     */
	    mapReduceDriver.withOutput(new Text("Connor"), new Text("age- 25	male	score-27"));
	    

	    /*
	     * Run the test.
	     */
	    mapReduceDriver.runTest();
	  }
}
