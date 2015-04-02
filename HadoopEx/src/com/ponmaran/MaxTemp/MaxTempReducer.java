package com.ponmaran.MaxTemp;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends
		Reducer<Text,IntWritable,Text,IntWritable>{

    protected void reduce(Text year, Iterable<IntWritable> temp,
            Context context) throws IOException, InterruptedException {
    	IntWritable maxTemp = new IntWritable(0);
    	Iterator<IntWritable> itr = temp.iterator();
    	while(itr.hasNext()){
    		int tempInt = itr.next().get();
    		if(maxTemp.get() < tempInt)
    			maxTemp.set(tempInt);
    	}
    	context.write(year, maxTemp);
    }

}
