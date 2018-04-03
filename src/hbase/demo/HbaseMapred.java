package hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HbaseMapred {

	public static class HbaseMapper extends TableMapper<Text, Text>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,Context context)throws IOException, InterruptedException {
			
			String out=Bytes.toString(value.getValue("base_info".getBytes(), "name".getBytes()));
			  String row=Bytes.toString(value.getRow());
			  context.write(new Text(row), new Text(out));
		}			
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		//conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(HbaseMapred.class);
		
		Scan scan=new Scan();
		TableMapReduceUtil.initTableMapperJob("users", scan, HbaseMapper.class, Text.class, Text.class, job);
		
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		
		job.waitForCompletion(true);
		
	}
	
}
