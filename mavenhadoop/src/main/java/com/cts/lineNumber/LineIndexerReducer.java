package com.cts.lineNumber;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
public class LineIndexerReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<WritableComparable, Writable, Text, Text> { 
 public void reduce(WritableComparable key, Iterator values, 
 OutputCollector output, Reporter reporter)  throws IOException {
 boolean first = true; 
 StringBuilder toReturn = new StringBuilder(); 
 while(values.hasNext()){ 
 if(!first) 
 toReturn.append('^'); 
 first=false; 
 toReturn.append(values.next().toString()); 
 } 
 output.collect(key, new Text(toReturn.toString())); 
 } 
 } 


