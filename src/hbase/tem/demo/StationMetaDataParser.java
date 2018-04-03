package hbase.tem.demo;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class StationMetaDataParser {

	private String stationID;
	private String stationName;
	private String location;
	private String description;

	public StationMetaDataParser() {

	}

	public void parse(Text record) {
		stationID = record.toString().substring(0, 6) + "-" + record.toString().substring(7, 12);
		String name = record.toString().substring(13, 43).trim();
		String LAT = record.toString().substring(57, 64);
		String LON = record.toString().substring(65, 74);
		String lo = LAT +" "+ LON;
		if(name.matches(" *")){
			stationName="Unknown";
		}
		else{
			stationName=name;
		}
		if(lo.matches(" *")){
			location="Null";
		}
		else{
			location=lo;
		}
		description = record.toString().substring(82, 99);

	}

	public String getStationID() {
		return stationID;
	}

	public String getStationName() {
		return stationName;
	}

	public String getLocation() {
		return location;
	}

	public String getDescription() {
		return description;
	}
	
//	@Test
//	public void test(){
//		String line="010015 99999 BRINGELAND                    NO            +61.383 +005.867 +0327.0 19870117 20111020";
//		parse(new Text(line));
//		System.out.println(stationID);
//		System.out.println(stationName);
//		System.out.println(location);
//		System.out.println(description);
//	}

}
