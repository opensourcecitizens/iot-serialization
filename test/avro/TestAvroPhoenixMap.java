package avro;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;
import io.parser.avro.jdbc.AvroPreparedStatementMapper;

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
		
		AvroPreparedStatementMapper phoenixMapper = new AvroPreparedStatementMapper();
		phoenixMapper.translate(smt1, map, schema);
		
		//Assert.assertEquals(mesg.get("payload").toString(),smt1.get("payload").toString());
		
	}
	
	@Test public void testPhoenixJavaObjectSerialize() throws Exception{
		
		
		Schema schema_remoteReq = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/registry-to-spark/versions/current/remoterequest.avsc").openStream());
		
		Schema  schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/versions/current/NeustarMessage.avsc").openStream());

		GenericRecord remotemesg = new GenericData.Record(schema_remoteReq);	
		remotemesg.put("path", "/a/light");
		remotemesg.put("payload","{\"value\":\"true\"}");
		remotemesg.put("deviceId","RaspiLightUUID-Demo");
		remotemesg.put("header","hub-request");
		remotemesg.put("txId","a37183ac-ba57-4213-a7f3-1c1608ded09e");
		remotemesg.put("statusCode",0);
		remotemesg.put("verb","POST");
		
		/**
		 *  {"path":"/a/light","verb":"POST","payload":"{\"value\":true}","header":"hub-request","txId":"a37183ac-ba57-4213-a7f3-1c1608ded09e","deviceId":"RaspiLightUUID-Demo"}*
		 */
		GenericRecord mesg = new GenericData.Record(schema);	
		mesg.put("sourceid", "kaniu");
		mesg.put("payload", "{\"value\":\"true\"}");
		mesg.put("registrypayload", remotemesg);
		mesg.put("messagetype", "TELEMETRY");
		mesg.put("createdate",  DateFormat.getDateTimeInstance(DateFormat.FULL, DateFormat.FULL).format(new Date()));
		mesg.put("messageid", UUID.randomUUID()+"");
		//create avro
		
		byte[] avrodata = AvroUtils.serializeJava(mesg, schema);
	
		//avro to map
		AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
		Map<String,?> map =  parser.parse(avrodata, schema);
		
		Set<String> keyset = map.keySet();
		int datasize = keyset.size();
		
		char[] qm = new char[datasize];
		for(int i = 0; i < datasize; i++){
			qm[i]='?';
		}
		
		ObjectMapper jsonmapper = new ObjectMapper();
		String rawjson = jsonmapper.writeValueAsString(map);
		
		String tablename = "TestTable1";
		try{
			smt = conn.createStatement();
			smt.executeUpdate("CREATE TABLE "+tablename+" (messageid VARCHAR(26), sourceid VARCHAR(26), payload VARCHAR(26), registrypayload VARCHAR(26), messagetype VARCHAR(26), createdate VARCHAR(26),CREATED_TIME VARCHAR(26), RAW_JSON VARCHAR(26))");
		}catch(Exception e){
			//e.printStackTrace();
		}
		
			//PreparedStatement prepStmt = conn.createStatement();
			//smt.executeUpdate("CREATE TABLE droolstbl (name VARCHAR(26), rule CLOB(100M))");
			String sqlstr ="INSERT INTO "+tablename+" ( "+Arrays.toString(keyset.toArray()).replace("[", "").replace("]", "")+",CREATED_TIME, RAW_JSON) "
					+ "VALUES("+Arrays.toString(qm).replace("[", "").replace("]", "")+", ? ,?)";
			
			System.out.println("SQL = "+sqlstr);
			
			PreparedStatement prepStmt = conn.prepareStatement(sqlstr);
			
			AvroPreparedStatementMapper sqlMapping = new AvroPreparedStatementMapper();
			
			sqlMapping.translate(prepStmt, map, schema);
			
			prepStmt.setTime(datasize+1, new Time(System.currentTimeMillis()));
			prepStmt.setString(datasize+2, rawjson);
			//log.debug("SQL = "+prepStmt.toString());
			System.out.println("SQL = "+prepStmt.toString());


		System.out.println("Table created");
		
		//Assert.assertEquals(mesg.get("payload").toString(),smt1.get("payload").toString());
		
	}
}
