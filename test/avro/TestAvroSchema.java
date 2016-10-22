package avro;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;

public class TestAvroSchema {
	Schema schema  = null;
	Schema schema_remoteReq = null;
	
	@Before
	public void init() throws MalformedURLException, IOException{
		//Schema schema_remoteReq = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/RemoteRequest2.avsc"));
				 schema_remoteReq = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/registry-to-spark/versions/current/remoterequest.avsc").openStream());
				
				 schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/versions/current/NeustarMessage.avsc").openStream());
			 	//Schema schema = new Schema.Parser().parse(new File("/Users/kndungu/Documents/workspace/iot-serialization/resources/NeustarMessage.avsc"));
			 	
	}
	
	@Test
	public void test() throws Exception{
	
		
		GenericRecord remotemesg = new GenericData.Record(schema_remoteReq);	
		remotemesg.put("path", "/api/v1/devices");
		remotemesg.put("payload","{\"value\":\"false\"}");
		remotemesg.put("deviceId","000000a9-2c7a-4654-8f34-f6e1d1ad8ad7/YS9saWdodA==");
		remotemesg.put("header","{\"API-KEY\": \"0\",\"Content-Type\": \"application/json\"}");
		remotemesg.put("txId","someTextid");
		remotemesg.put("verb","update");
		
		byte[] payloadavro = AvroUtils.serializeJava(remotemesg, schema_remoteReq);
		GenericRecord genericPayload = AvroUtils.avroToJava(payloadavro, schema_remoteReq);
		
			
	 	GenericRecord mesg = new GenericData.Record(schema);	
		mesg.put("sourceid", "device1");
		mesg.put("registrypayload", genericPayload);
		mesg.put("payload", "");
		mesg.put("messagetype", "TELEMETRY");
		mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");

		System.out.println(mesg.toString());
		//create avro
		//byte[] avrodata = AvroUtils.serializeJson(mesg.toString(), schema);
		byte[] avrodata = AvroUtils.serializeJava(mesg, schema);
		
		String json = AvroUtils.avroToJson(avrodata, schema);
		System.out.println(json);
		//avro to map
		AvroParser<Map<String,?>> parser = new AvroParser<Map<String,?>>(schema);
		
		Map<String,?> avromap =  parser.parse(avrodata, schema);
		
	}
	


}
