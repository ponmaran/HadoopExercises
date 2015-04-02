package com.ponmaran.WordCount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends 
	Reducer<Text,IntWritable,Text, IntWritable>{
	protected void Reduce(Text text, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException{
		Iterator<IntWritable> iterator = iterable.iterator();
		int total = 0;
		while(iterator.hasNext()){
			total += iterator.next().get();
		}
		context.write(text, new IntWritable(total));
	}
}
