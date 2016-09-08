package avro;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;
import io.parser.avro.phoenix.AvroToPhoenixMap;

public class TestAvroPhoenixMap {	
	private Connection conn = null;
	private ResultSet rs = null;
	private Statement smt = null;
	private PreparedStatement smt1 = null;

	@Before
	public void init() throws Exception {
		String dbURL = "org.apache.derby.jdbc.EmbeddedDriver";

		try {
			Class.class.forName(dbURL).newInstance();
			conn = DriverManager.getConnection("jdbc:derby:" + "droolsDB;create=true");
		} catch (Exception except) {
			except.printStackTrace();
		}

	}
	
	@After
	public void close() {
		try {
			if (rs != null) {
				rs.close();
			}
			if (smt1 != null) {
				smt1.close();
			}
			
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {

		}
	}
	
	@Test public void testSpecificJavaObjectSerialize() throws Exception{
		
		Schema schema = new Schema.Parser().parse(Class.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		
		//create avro
		byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//avro to map
		AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
		Map<String,?> map =  parser.parse(avrodata, schema);
		
		try{
			smt = conn.createStatement();
			smt.executeUpdate("CREATE TABLE droolstbl (name VARCHAR(26), rule CLOB(100M))");
		}catch(Exception e){
			
		}
		System.out.println("Table created");
		
		smt1 = conn.prepareStatement("insert into droolstbl values (?,  ? )");
		
		AvroToPhoenixMap phoenixMapper = new AvroToPhoenixMap();
		phoenixMapper.translate(smt1, map, schema);
		
		//Assert.assertEquals(mesg.get("payload").toString(),smt1.get("payload").toString());
		
	}
}
