package hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;

public class TestTwo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		
		Connection connection=ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("users"));
		Scan scan=new Scan();
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result result:resultScanner){
			String row=Bytes.toString(result.getRow());
			String s1=Bytes.toString(result.getValue("base_info".getBytes(), "age".getBytes()));
			String s2=Bytes.toString(result.getValue("base_info".getBytes(), "name".getBytes()));
			String s3=Bytes.toString(result.getValue("extra_info".getBytes(), "job".getBytes()));
			String s4=Bytes.toString(result.getValue("extra_info".getBytes(), "hobby".getBytes()));	
			System.out.println(row+"\t"+s1+"\t"+s2+"\t"+s3+"\t"+s4);
		}
		
		
	}

}
