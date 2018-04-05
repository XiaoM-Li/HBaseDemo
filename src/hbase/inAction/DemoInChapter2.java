package hbase.inAction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class DemoInChapter2 {
	Configuration conf;
	Connection connection;
	Admin admin;

	@Before
	public void Init() throws IOException {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		connection = ConnectionFactory.createConnection(conf);
		admin = connection.getAdmin();
	}

	@Test
	public void create() throws IOException {

		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("users"));
		HColumnDescriptor family = new HColumnDescriptor("info");
		table.addFamily(family);
		admin.createTable(table);
	}

	@Test
	public void put() throws IOException {

		Table table = connection.getTable(TableName.valueOf("users"));
		Put put=new Put("TheRealMT".getBytes());
		
		put.addColumn("info".getBytes(), "name".getBytes(), "Mark Twain".getBytes());
		put.addColumn("info".getBytes(), "email".getBytes(), "samuel@clemens.org".getBytes());
		put.addColumn("info".getBytes(), "password".getBytes(), "Langhorne".getBytes());
		
		table.put(put);
		table.close();
		
	}
	
	@Test
	public void get() throws IOException{
		Table table = connection.getTable(TableName.valueOf("users"));
		Get get=new Get("TheRealMT".getBytes());
		Result result = table.get(get);
		byte[] value = result.getValue("info".getBytes(),"name".getBytes());
		System.out.println(Bytes.toString(value));
		
	}

}
