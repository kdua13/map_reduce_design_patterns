package com.kapil.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AppUsageDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "AppUsageJob");
		job.setJarByClass(com.kapil.hadoop.AppUsageDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(com.kapil.hadoop.AppUsageMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(com.kapil.hadoop.AppUsageReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("/home/hduser/datasets/auth.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/tmp/mapreduce/authfile9"));

		if (!job.waitForCompletion(true))
			return;
	}
}