package avro;

import java.io.IOException;
import java.util.Map;

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

import io.parser.avro.AvroUtils;

import org.junit.Assert;

public class TestAvro {	
	
	@Test public void testAvroJsonConversion() throws IOException{
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		
		//create json
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		mesg.put("messagetype","NOTIFICATION");

		System.out.println(mesg.toString());
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		///send byte array in mqtt
		
		///receive data
		String json = AvroUtils.avroToJson(avro, schema);
		
		System.out.println(json);
		
		//Assert.assertEquals(mesg.toString(), json);
		}
	
	enum messagetype{REGISTRY , NOTIFICATION , TELEMETRY, EXCEPTION };
	
	@Test public void testAvroMapConversion() throws IOException{
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		
		//create json
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		mesg.put("messagetype","REGISTRY");

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
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		System.out.println(mesg.toString());
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		GenericDatumReader<CustomMessage> reader = new SpecificDatumReader<CustomMessage>(schema);	        
		Decoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
		CustomMessage msg1 = reader.read(null, decoder);
	    
		CustomMessage message = new CustomMessage();
		message.setId("device1");
		message.setPayload("{'type':'internal json'}");
		
		Assert.assertEquals(msg1, message);
		
	}
	

	

}
