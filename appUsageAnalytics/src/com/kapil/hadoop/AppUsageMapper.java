package com.kapil.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AppUsageMapper extends Mapper<LongWritable, Text, Text, Text> {
	private final static IntWritable one = new IntWritable(1);
	Text ip_address = new Text();
	Text lan_id = new Text();
	
	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		
		String ip[] = ivalue.toString().split("IP_ADDRESS=");
		String ipAddress = ip[1].split(",")[0];
		String lan[] = ivalue.toString().split("LAN_ID =");
		String lanId = lan[1].split(",")[0];
		ip_address.set(ipAddress);
		lan_id.set(lanId);
		
		context.write(ip_address, lan_id);

	}
}
