package avro;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Test;

import io.parser.avro.AvroParser;
import io.parser.avro.AvroUtils;

import org.junit.Assert;

public class TestAvro {	
	
	@Test public void testAvroJsonConversion() throws IOException{
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		
		//create json
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("sourceid", "device1");
		mesg.put("payload", "{\"device\":\"6c21e538-4b7b-4e87-958f-b8c868f50f2e\",\"event\":\"switch\",\"value\":\"on\",\"date\":\"2016-09-01T21:47:06.061Z\",\"name\":\"Maya's Room Camera\"}");
		mesg.put("messagetype", "EXCEPTION");
		mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");

		System.out.println(mesg.toString().trim());
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		///send byte array in mqtt
		
		///receive data
		String json = AvroUtils.avroToJson(avro, schema);
		
		System.out.println(json);
		
		//Assert.assertEquals(mesg.toString().trim(), json.trim());
		}
	
	enum messagetype{REGISTRY , NOTIFICATION , TELEMETRY, EXCEPTION };
	
	@Test public void testAvroMapConversion() throws IOException{
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		
		//create json
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("sourceid", "device1");
		mesg.put("payload", "{\"device\":\"6c21e538-4b7b-4e87-958f-b8c868f50f2e\",\"event\":\"switch\",\"value\":\"on\",\"date\":\"2016-09-01T21:47:06.061Z\",\"name\":\"Maya's Room Camera\"}");
		mesg.put("messagetype", "EXCEPTION");
		mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");

		System.out.println(mesg.toString());
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		///send byte array in mqtt
		
		///receive data
		String data =  AvroUtils.avroToJson(avro, schema);
		ObjectMapper mapper = new ObjectMapper();
		Map<String,Object> map = mapper.readValue(data, new TypeReference<Map<String, Object>>(){});
        
		Assert.assertEquals(map.get("payload"), mesg.get("payload"));
		Assert.assertEquals(map.get("id"), mesg.get("id"));
		}
	
	
	@Test public void testAvroJavaConversion() throws IOException{
		
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("sourceid", "device1");
		mesg.put("payload", "{\"device\":\"6c21e538-4b7b-4e87-958f-b8c868f50f2e\",\"event\":\"switch\",\"value\":\"on\",\"date\":\"2016-09-01T21:47:06.061Z\",\"name\":\"Maya's Room Camera\"}");
		mesg.put("messagetype", "EXCEPTION");
		mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");
		System.out.println(mesg.toString());
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		GenericDatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);	        
		Decoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
		GenericRecord msg1 = reader.read(null, decoder);
	    
		Assert.assertEquals(msg1, mesg);
		
	}
	
	@Test public void testParser() throws Exception{
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("sourceid", "device1");
		mesg.put("payload", "{\"device\":\"6c21e538-4b7b-4e87-958f-b8c868f50f2e\",\"event\":\"switch\",\"value\":\"on\",\"date\":\"2016-09-01T21:47:06.061Z\",\"name\":\"Maya's Room Camera\"}");
		mesg.put("messagetype", "EXCEPTION");
		mesg.put("createdate",  DateFormat.getDateInstance().format(new Date())+"");
		mesg.put("messageid", UUID.randomUUID()+"");
		System.out.println(mesg.toString());
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		AvroParser<Map<String,?>> avroParser = new AvroParser<Map<String,?>>(schema);
		Map<String,?> map = avroParser.parse(avro, new HashMap<String,Object>() );
		System.out.println(map.toString());
		Assert.assertEquals(map.get("payload"), mesg.get("payload"));
	}
	
	
}
