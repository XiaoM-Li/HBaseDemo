package hbase.tem.demo;
/*
 * 获取日期和气温，并且判别气温是否是有效值的工具类
 */

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
	private static final int MISSING_TEMPERATURE = 9999;

	private String year;
	private String time;
	private int airTemperature;
	private String quality;


	private String stationId;

	public void parse(String record) {
		year = record.substring(15, 19);
		time=record.substring(15, 27);
		String airTemperatureString;
		if (record.charAt(87) == '+') {
			airTemperatureString = record.substring(88, 92);
		} else {
			airTemperatureString = record.substring(87, 92);
		}
		airTemperature = Integer.parseInt(airTemperatureString);
		quality = record.substring(92, 93);
		stationId=record.substring(4, 10)+"-"+record.substring(10, 15);
	}

	public void parse(Text record) {
		parse(record.toString());
	}
	
	public boolean isValidTemperature(){
		return airTemperature!=MISSING_TEMPERATURE&&quality.matches("[01459]");
	}
	
	public boolean isMissingTemperature(){
		return airTemperature==MISSING_TEMPERATURE;
	}
	public String getYear(){
		return year;
	}
	
	public int getAirTemperature(){
		return airTemperature;
	}
	
	public String getStationId() {
		return stationId;
	}
	
	public String getQuality() {
		return quality;
	}
	
	public String getTime(){
		return time;
	}

}
