package com.yenzichun.etu;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StockVolumnPartitioner extends Partitioner<Text, IntWritable>{
	
	
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		//String[] word = value.toString().split("[,]");
		int date = Integer.parseInt(key.toString());
		
		// For a job without reducers, we just return 0
		if (numReduceTasks == 0)
			return 0;
		
		if (date <=  20140310){
			System.out.printf("date is: %d", date);
			return 3 % numReduceTasks;//goes to part-r-00000
		}
		if (date <= 20140320){
			System.out.printf("date is: %d", date);
			return 1 % numReduceTasks;//goes to part-r-00001
		}
		return 2 % numReduceTasks;//goes to part-r-00002
	}
}


	
