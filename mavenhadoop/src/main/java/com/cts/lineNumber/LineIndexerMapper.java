package com.cts.lineNumber;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("unchecked")
public class LineIndexerMapper extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, Text>
{ 
 private final static Text word = new Text(); 
 private final static Text summary = new Text(); 
 public void map(WritableComparable key, Writable val, 
 OutputCollector output, Reporter reporter) 
 throws IOException { 
 String line = val.toString(); 
 summary.set(key.toString() + ":" + line); 
 StringTokenizer itr = new StringTokenizer(line.toLowerCase()); 
 while(itr.hasMoreTokens()) { 
 word.set(itr.nextToken());  output.collect(word, summary); 
 }
 } 
 }