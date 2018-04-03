package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WriteToHbase extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		ToolRunner.run(conf, new WriteToHbase(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job job=Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		job.setMapperClass(TestMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Put.class);
		
		FileInputFormat.setInputPaths(job, new Path("D://test.txt"));
		
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,"xiaoming");
		
		job.setNumReduceTasks(0);
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class TestMapper extends Mapper<LongWritable, Text, NullWritable, Put>{
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String line=value.toString();
			String[] fields=line.split("\t");
			Put put=new Put(fields[0].getBytes());
			put.addColumn("info".getBytes(), "name".getBytes(), fields[1].getBytes());
			put.addColumn("info".getBytes(), "password".getBytes(), fields[2].getBytes());
			context.write(NullWritable.get(), put);
		}
	}

}
