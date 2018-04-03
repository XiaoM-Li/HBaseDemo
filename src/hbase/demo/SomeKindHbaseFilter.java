package hbase.demo;
/*
 * 包含多种HBASE的预装过滤器
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class SomeKindHbaseFilter {
	
	private Configuration conf;
	private Connection connection;
	private Table table;
	
	
	@Before
	public void init() throws IOException{
		conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		connection=ConnectionFactory.createConnection(conf);
		table=connection.getTable(TableName.valueOf("xiaoming".getBytes()));
	}
	
	@Test
	//行过滤器,一种预装的比较过滤器，支持基于行键过滤数据
	public void TestRowFilter() throws IOException{
		Filter filter1=new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator("0003".getBytes()));
		Scan scan=new Scan();
//		scan.setFilter(filter1);
//		ResultScanner resultScanner = table.getScanner(scan);
//		for(Result result:resultScanner){
//			System.out.println(Bytes.toString(result.getRow())+"\t"+Bytes.toString(result.getValue("info".getBytes(), "name".getBytes())));
//		}
		
		Filter filter2=new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^a"));
		scan.setFilter(filter2);
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result result:resultScanner){
			System.out.println(Bytes.toString(result.getRow())+"\t"+Bytes.toString(result.getValue("info".getBytes(), "name".getBytes())));
		}
	}
	
	@Test
	//前缀过滤器，行过滤器的一种特例，基于行键的前缀值进行过滤
	public void TestPrefixFilter() throws IOException{
		String prefix="b";
		Scan scan=new Scan();
		scan.setFilter(new PrefixFilter(prefix.getBytes()));
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result result:resultScanner){
			System.out.println(Bytes.toString(result.getRow())+"\t"+Bytes.toString(result.getValue("info".getBytes(), "name".getBytes())));
		}
		
	}
	
	@Test
	//限定符过滤器，用于匹配列限定符而不是行键
	public void TestQualifierFilter() throws IOException{
		Scan scan=new Scan();
		//scan.addColumn("info".getBytes(), "password".getBytes());
		Filter filter=new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("name".getBytes()));
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result result:resultScanner){
			System.out.println(Bytes.toString(result.getRow())+"\t"+Bytes.toString(result.getValue("info".getBytes(), "name".getBytes())));
		}
	}
	
	@Test
	//值过滤器
	public void TestValueFilter() throws IOException{
		Scan scan=new Scan();
		scan.addColumn("info".getBytes(), "name".getBytes());
		Filter filter=new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator("l".getBytes()));
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result result:resultScanner){
			System.out.println(Bytes.toString(result.getRow())+"\t"+Bytes.toString(result.getValue("info".getBytes(), "name".getBytes())));
		}
	}
	
	@Test
	//时间戳过滤器
	public void TestTimestampsFilter() throws IOException{
		Scan scan=new Scan();
		List<Long> times=new ArrayList<>();
		times.add(1517143242076L);
		Filter filter=new TimestampsFilter(times);
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result result:resultScanner){
			System.out.println(Bytes.toString(result.getRow())+"\t"+Bytes.toString(result.getValue("info".getBytes(), "name".getBytes())));
		}
	}
	
	//还有过滤器列表，可以加入多个过滤器
}
