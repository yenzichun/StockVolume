package com.yenzichun.etu;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StockVolumnReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	private IntWritable totalVolumn = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int volumn = 0;
		Iterator<IntWritable> it = values.iterator();
		
		while (it.hasNext()) {
			volumn += it.next().get();
		}
		totalVolumn.set(volumn);
		context.write(key, totalVolumn);
	}
}