package com.yenzichun.etu;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

import com.yenzichun.etu.StockVolumnMapper;
import com.yenzichun.etu.StockVolumnReducer;
import com.yenzichun.etu.StockVolumnPartitioner;


public class StockVolumnDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) { 
			System.out.printf(
					"Usage: %s [generic options] <input dir> <output dir>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		
//		Create a new Job
//	    Job job = new Job(new Configuration());
//	    Job job = new Job(new Configuration(), "lab10_" + this.getClass().getName());
		
//		configuration and configurad 差別：
//		import configuration 是一個類別 可以自定義hadoop設定檔,例如 Configuration conf = getConf()
//		import configurad 可以呼叫getConf() 然後幫你把從command line下的參數解析後裝進去
//		需要用getConf()從command line 下  mapred.reduce.tasks=3 才有用
//		相關參數在在/opt/hadoopmr/conf/*.xml  可以查
		 
//		Configuration conf = getConf();
		 
	    Job job = new Job(getConf(), "Toal Stock Volume:"+this.getClass().getName()); //argument2 indicates the name of this task
	     
//	    Job job = new Job();
	    job.setJarByClass(StockVolumnDriver.class);
	     
	    // Specify various job-specific parameters     
	    //job.setJobName("Total Stock Volumn");
	     
	    //job.setInputPath(new Path("in")); //old api?
	    //job.setOutputPath(new Path("out")); //old api?
	     
	    job.setMapperClass(StockVolumnMapper.class);
	    job.setReducerClass(StockVolumnReducer.class);
	    job.setCombinerClass(StockVolumnReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	     
	    job.setPartitionerClass(StockVolumnPartitioner.class);
	    //job.setNumReduceTasks(3);
	     
	    FileInputFormat.setInputPaths(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	     
	    // Submit the job, then poll for progress until the job is complete
	    job.waitForCompletion(true);		
		
	    return 0;
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int code = ToolRunner.run(new StockVolumnDriver(), args);
		System.exit(code);
	}

}
