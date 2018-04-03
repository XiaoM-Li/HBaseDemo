package hbase.demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

/*
 * 主要是Hbase数据库常用API实现
 */

public class TestOne {
	
	static Configuration conf;
	public static void init(){
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
	}
	
	
	public static void create() throws  Exception{
		Connection connection=ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();
		HTableDescriptor desc=new HTableDescriptor(TableName.valueOf("users"));
		desc.addFamily(new HColumnDescriptor("base_info"));
		desc.addFamily(new HColumnDescriptor("extra_info"));
		admin.createTable(desc);
	}
	
	public static void main(String[] args) throws Exception {
		init();
		//create();
		Connection connection=ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("users"));
		List<Put> puts=new ArrayList<>();
		
		Put put1=new Put("0001".getBytes());
		put1.addColumn("base_info".getBytes(), "name".getBytes(), "Wang".getBytes());
		put1.addColumn("base_info".getBytes(), "age".getBytes(), "21".getBytes());
		put1.addColumn("extra_info".getBytes(), "job".getBytes(), "woker".getBytes());
		
		Put put2=new Put("0002".getBytes());
		put2.addColumn("base_info".getBytes(), "name".getBytes(), "Li".getBytes());
		put2.addColumn("base_info".getBytes(), "age".getBytes(), "23".getBytes());
		put2.addColumn("extra_info".getBytes(), "job".getBytes(), "teacher".getBytes());
		
		Put put3=new Put("0003".getBytes());
		put3.addColumn("base_info".getBytes(), "name".getBytes(), "Zhang".getBytes());
		put3.addColumn("base_info".getBytes(), "age".getBytes(), "26".getBytes());
		put3.addColumn("extra_info".getBytes(), "hobby".getBytes(), "reading".getBytes());
		
		Put put4=new Put("0004".getBytes());
		put4.addColumn("base_info".getBytes(), "name".getBytes(), "Liu".getBytes());
		put4.addColumn("base_info".getBytes(), "age".getBytes(), "33".getBytes());
		put4.addColumn("extra_info".getBytes(), "hobby".getBytes(), "game".getBytes());
		put4.addColumn("extra_info".getBytes(), "address".getBytes(), "Beijing".getBytes());
		
		
		puts.add(put1);
		puts.add(put4);
		puts.add(put3);
		puts.add(put2);
		
		table.put(puts);
		
		table.close();
		
	}
}
