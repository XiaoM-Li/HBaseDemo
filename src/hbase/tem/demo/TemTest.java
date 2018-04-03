package hbase.tem.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TemTest extends Configured implements Tool{
	
	public static void main(String[] args) throws  Exception {
		
		Configuration conf=HBaseConfiguration.create();
		//conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		ToolRunner.run(conf, new TemTest(), args);
		
				
	}
	

	@Override
	public int run(String[] args) throws Exception {
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		job.setMapperClass(TemMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(CombineTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		TableMapReduceUtil.initTableReducerJob("observations", TemReducer.class, job);
		
		return job.waitForCompletion(true)?0:1;	
		
	}
	
	
	public  static class TemMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private NcdcRecordParser parser=new NcdcRecordParser();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String line=value.toString();
			parser.parse(line);
			if(parser.isValidTemperature()){
				String rowKey=parser.getStationId()+parser.getTime();
				int airTem=parser.getAirTemperature();
				context.write(new Text(rowKey), new IntWritable(airTem));
			}
		}
	}
	
	public static class TemReducer extends TableReducer<Text, IntWritable, Text>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			for(IntWritable value:values){
				Put put=new Put(key.toString().getBytes());
				put.addColumn("data".getBytes(), "airTem".getBytes(), String.valueOf(value.get()).getBytes());
				context.write(key, put);
			}
		}
	}
}
