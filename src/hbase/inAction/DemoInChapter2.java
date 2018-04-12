package hbase.inAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
		
		Put put1=new Put("001".getBytes());
		put1.addColumn("info".getBytes(), "name".getBytes(), "Mark Twain".getBytes());
		put1.addColumn("info".getBytes(), "email".getBytes(), "samuel@clemens.org".getBytes());
		put1.addColumn("info".getBytes(), "password".getBytes(), "Langhorne".getBytes());
		
		Put put2=new Put("002".getBytes());
		put2.addColumn("info".getBytes(), "name".getBytes(), "Lora Cloud".getBytes());
		put2.addColumn("info".getBytes(), "email".getBytes(), "Lora@clemens.org".getBytes());
		put2.addColumn("info".getBytes(), "password".getBytes(), "123456a".getBytes());
		
		Put put3=new Put("003".getBytes());
		put3.addColumn("info".getBytes(), "name".getBytes(), "Hames Rode".getBytes());
		put3.addColumn("info".getBytes(), "email".getBytes(), "qwer@clemens.org".getBytes());
		put3.addColumn("info".getBytes(), "password".getBytes(), "asxcdfv".getBytes());
		put3.addColumn("info".getBytes(), "hobby".getBytes(),"baketball".getBytes());
		
		Put put4=new Put("004".getBytes());
		put4.addColumn("info".getBytes(), "name".getBytes(), "QQ BZCK ".getBytes());
		put4.addColumn("info".getBytes(), "email".getBytes(), "163@clemens.org".getBytes());
		put4.addColumn("info".getBytes(), "password".getBytes(), "zaq123".getBytes());
		put4.addColumn("info".getBytes(), "gender".getBytes(), "male".getBytes());
		
		List<Put> puts=new ArrayList<>();
		
		puts.add(put1);
		puts.add(put2);
		puts.add(put3);
		puts.add(put4);
		
		table.put(puts);
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
