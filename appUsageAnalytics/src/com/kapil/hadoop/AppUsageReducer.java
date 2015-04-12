package com.kapil.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AppUsageReducer extends Reducer<Text, IntWritable, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String lanIds = "";
		// process values
		for (Text val : values) {
			lanIds.concat(val.toString()+",");
		}
		int len = lanIds.length();
		lanIds.substring(0, len-1).trim();
		context.write(key, new Text(lanIds));
	}
}
