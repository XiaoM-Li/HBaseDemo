package hbase.demo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {

	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub
		String time="1517041283147";
		long ltime=Long.parseLong(time);
		Date date=new Date(ltime);
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
		 String string = format.format(date);
		System.out.println(string);
	}

}
