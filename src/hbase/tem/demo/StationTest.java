package hbase.tem.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class StationTest extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		ToolRunner.run(conf, new StationTest(), args);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job=Job.getInstance(getConf());
		job.setJarByClass(getClass());
		
		job.setMapperClass(StationMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		TableMapReduceUtil.initTableReducerJob("stations", StationReducer.class, job);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class StationMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class StationReducer extends TableReducer<LongWritable, Text, NullWritable>{
		private StationMetaDataParser parser=new StationMetaDataParser();
		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			for(Text value:values){
				parser.parse(value);
				Put put=new Put(parser.getStationID().getBytes());
				put.addColumn("info".getBytes(), "name".getBytes(), parser.getStationName().getBytes());
				put.addColumn("info".getBytes(), "location".getBytes(), parser.getLocation().getBytes());
				put.addColumn("info".getBytes(), "description".getBytes(), parser.getDescription().getBytes());
				context.write(NullWritable.get(), put);
			}
		}
	}
}
