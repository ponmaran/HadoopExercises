package com.ponmaran.MaxTemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper  extends
		Mapper<Text, Text, Text, IntWritable> {
	
	protected void map(Text year, Text temp, Context context)
			throws IOException, InterruptedException {
		context.write(year, new IntWritable(Integer.parseInt(temp.toString().trim())));	

	}
}