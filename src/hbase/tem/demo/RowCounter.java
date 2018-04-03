package hbase.tem.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/*
 * 这个类以HBASE中的表为输入，对行数进行计数
 */

public class RowCounter {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf=HBaseConfiguration.create();
		//conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		Job job=Job.getInstance(conf);
		job.setJarByClass(RowCounter.class);
		
		Scan scan=new Scan();
		TableMapReduceUtil.initTableMapperJob(args[0].getBytes(), scan, RowCounterMapper.class, Text.class, Text.class, job);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(NullOutputFormat.class);
		
		job.waitForCompletion(true);
	}
	
	public static class RowCounterMapper extends TableMapper<Text, Text>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context)throws IOException, InterruptedException {
			context.getCounter("My Counters","RowNumbers").increment(1);
		}
	}

}
