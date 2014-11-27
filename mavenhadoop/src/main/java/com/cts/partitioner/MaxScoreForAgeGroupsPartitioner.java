package com.cts.partitioner;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

public class MaxScoreForAgeGroupsPartitioner extends Partitioner<Text, Text>
{
	@Override
	/* No of partitioner must be equal to number of reducers
	 * It must return values from 0 to (No:of reducers -1)
	 *
	 */
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		String[] tokens =value.toString().split("\t");
		String name=tokens[0];
		int age=Integer.parseInt(tokens[1]);
		String score=tokens[2];
		if(numReduceTasks==0)
		{
			return 0;
		}
		else if(age<=20)
		{
			return 0;
		}
		else if(age>20 && age<=50)
		{
			return 1 %numReduceTasks;
		}
		else if(age>50)
		{
			return 2 %numReduceTasks;
		}
		else
		{
			return 2% numReduceTasks;
		}
	}
}