package hbase.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

public class HbaseFilter {

	public static void main(String[] args) throws IOException {
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop04:2181,hadoop05:2181,hadoop09:2181");
		Connection connection=ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("xiaoming".getBytes()));
		Scan scan=new Scan();
		scan.addColumn("info".getBytes(), "password".getBytes());
//		Filter filter=new PasswordStrengthFilter(4);
//		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for(Result result:resultScanner){
			System.out.println(new String(result.getValue("info".getBytes(), "password".getBytes())));
		}
	

	}
	
	public static class PasswordStrengthFilter extends FilterBase{

		private int len;
		
		public PasswordStrengthFilter() {
			// TODO Auto-generated constructor stub
			super();
		}
		
		public PasswordStrengthFilter(int len) {
			// TODO Auto-generated constructor stub
			this.len=len;
		}
		@Override
		public ReturnCode filterKeyValue(Cell cell) throws IOException {
			if (cell.getValueLength()>=len) {
				return ReturnCode.SKIP;
			}
			else{
				return ReturnCode.INCLUDE;
			}			
		}
		
		
	}

}
