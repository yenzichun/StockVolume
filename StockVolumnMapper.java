package com.yenzichun.etu;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StockVolumnMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable(); 
	//private final static IntWritable ONE = new IntWritable(1);
	
	@Override
	public void setup(Context c) {
		// I am equivalent to the configure() method in the old API
	}

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String s = value.toString();
//		for (String word : s.split("\\W+")) {
//			if (word.length() > 0) {
//				outputKey.set(word);
//				context.write(outputKey, ONE);
//			}
//		}
		String[] word = s.split("[,]");
		System.out.printf("%s", word[0]);
		outputKey.set(word[0]);
		outputValue.set(Integer.parseInt(word[3]));
		context.write(outputKey, outputValue);
	}
	
	@Override
	public void cleanup(Context c) {
		// I am equivalent to the close() method in the old API
		// In the new API, I no longer have to save a reference to context object in the
		// setup() call
	}
}
