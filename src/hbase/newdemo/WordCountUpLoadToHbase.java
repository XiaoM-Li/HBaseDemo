package hbase.newdemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountUpLoadToHbase extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		Configuration configuration=HBaseConfiguration.create();
		//configuration.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		ToolRunner.run(configuration, new WordCountUpLoadToHbase(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		TableMapReduceUtil.initTableReducerJob("wordCount", WCReducer.class, job);	
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String line=value.toString();
			String[] words=StringUtils.split(line,'\t');
			for(String word:words){
				context.write(new Text(word), new IntWritable(1));
			}
			
		}
	}
	
	public static class WCReducer extends TableReducer<Text, IntWritable, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum=0;
			Put put=new Put(key.toString().getBytes());
			for(IntWritable value:values){
				sum+=value.get();
			}
			put.addColumn("data".getBytes(), "number".getBytes(), Bytes.toBytes(String.valueOf(sum)));
			context.write(NullWritable.get(), put);
		}
	}

}
