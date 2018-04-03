package hbase.tem.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ScanTem {

	public static void main(String[] args) throws Exception {
		Configuration conf=HBaseConfiguration.create();
		// TODO Auto-generated method stub
		conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		Job job=Job.getInstance(conf);
		job.setJarByClass(ScanTem.class);
		Scan scan=new Scan();
		TableMapReduceUtil.initTableMapperJob("observations".getBytes(), scan, ScanTemMapper.class, Text.class, NullWritable.class, job);
		
		FileOutputFormat.setOutputPath(job, new Path("D://scanRes"));
		
		job.waitForCompletion(true);
		
		
	}
	
	public static class ScanTemMapper  extends TableMapper<Text, NullWritable>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,Context context)throws IOException, InterruptedException {
			byte[] val = value.getValue("data".getBytes(), "airTem".getBytes());
			context.write(new Text(val),NullWritable.get());
		}
	}

}
