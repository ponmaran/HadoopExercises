package com.ponmaran.WordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends 
	Mapper<LongWritable,Text,Text,IntWritable>{
	
	protected void Map(LongWritable inKey,Text inText, Context context) throws IOException, InterruptedException{
		StringTokenizer tokenizer = new StringTokenizer(inText.toString());
		Text text = new Text();
		while(tokenizer.hasMoreElements()){
			text.set(tokenizer.nextToken());
			System.out.println(text.toString());
			context.write(text, new IntWritable(1));
		}
	}
}
