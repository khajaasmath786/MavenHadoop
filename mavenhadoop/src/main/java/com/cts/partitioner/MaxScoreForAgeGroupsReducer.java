package com.cts.partitioner;

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
public class MaxScoreForAgeGroupsReducer extends Reducer<Text, Text, Text, Text>
 {

    /*
     * The reduce method runs once for each key received from the shuffle and sort phase of the MapReduce framework. The method receives a key of type Text, a set of values of type IntWritable, and a
     * Context object.
     */
  
    @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		int maxScore = Integer.MIN_VALUE;
		
		String name = " ";
		String age = " ";
		String gender = " ";
		int score = 0;
		//iterating through the values corresponding to a particular key
		for(Text val: values){
		System.out.println("Reducer String-->"+val.toString());
			String [] valTokens = val.toString().split("\t");
			System.out.println("Inside Reducer with Age -->"+valTokens[2]);
			score = Integer.parseInt(valTokens[2]);
		
            //if the new score is greater than the current maximum score, update the fields as they will be the output of the reducer after all the values are processed for a particular key			
			    if(score > maxScore){
				name = valTokens[0];
				age = valTokens[1];
				gender = key.toString();
                maxScore = score;
			}
		}
		context.write(new Text(name), new Text("age- "+age+"\t"+gender+"\tscore-"+maxScore));
	}
}