package com.cts.logfileanaylsispartitioner;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */
public class LogFileAnaylsisReducer extends Reducer<Text, Text, Text, IntWritable>
 {

    /*
     * The reduce method runs once for each key received from the shuffle and sort phase of the MapReduce framework. The method receives a key of type Text, a set of values of type IntWritable, and a
     * Context object.
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    	 int wordCount = 0;

    	    /*
    	     * The following code lets you iterate through the values without 
    	     * referencing them. We're not interested in referencing the
    	     * values - we only want to count them.  
    	     * Using
    	     *   for (Text value : values)  
    	     * produces a compiler warning unless you reference the value. 
    	     */
    	    while (values.iterator().hasNext()) {
    	      values.iterator().next();
    	      wordCount++;
    	    }
    	    context.write(key, new IntWritable(wordCount));
}
 }