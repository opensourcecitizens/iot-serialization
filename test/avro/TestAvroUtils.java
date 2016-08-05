package avro;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import io.parser.avro.AvroUtils;

public class TestAvroUtils {
	
	@Test public void testGeneratedJavaObjectCreate() throws IOException{
		
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//avro to java
		CustomMessage msg1 =  AvroUtils.avroToJava(avro, schema);
		
		Assert.assertEquals(msg1.get("payload").toString(),mesg.get("payload").toString());
	}
	
	
	@Test public void testGenericJavaObjectCreate() throws IOException{
		
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//avro to java
		GenericRecord msg1 = AvroUtils.avroToJava(avro, schema);
		
		Assert.assertEquals(msg1.get("payload").toString(),mesg.get("payload").toString());
	}
	
	@Test public void testSpecificJavaObjectCreate() throws IOException{
		
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//avro to java
		CustomSimpleMessage msg1 = AvroUtils.avroToJava(avro, schema, CustomSimpleMessage.class);
		Assert.assertEquals(msg1.getPayload().toString(),mesg.get("payload").toString());
		
	}
	
	
	
	@Test public void testGeneratedJavaObjectSerialize() throws IOException{
		
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//avro to java
		CustomMessage msg1 =  AvroUtils.avroToJava(avro, schema);
		
		Assert.assertEquals(msg1.get("payload").toString(),mesg.get("payload").toString());
		
		byte[] avro2 = AvroUtils.serializeJava(msg1, schema);
		
		Assert.assertNotEquals(avro,avro2);//tests data abstraction and encryption??
		
		GenericRecord genericTest = AvroUtils.avroToJava(avro2, schema);

		Assert.assertEquals(mesg.get("payload").toString(),genericTest.get("payload").toString());
		
	}
	
	
	@Test public void testGenericJavaObjectSerialize() throws IOException{
		
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//avro to java
		GenericRecord msg1 = AvroUtils.avroToJava(avro, schema);
		
		Assert.assertEquals(msg1.get("payload").toString(),mesg.get("payload").toString());
		
		byte[] avro2 = AvroUtils.serializeJava(msg1, schema);
		
		Assert.assertNotEquals(avro,avro2);//tests data abstraction 
		
		GenericRecord genericTest = AvroUtils.avroToJava(avro2, schema);
		
		Assert.assertEquals(mesg.get("payload").toString(),genericTest.get("payload").toString());
	}
	
	@Test public void testSpecificJavaObjectSerialize() throws IOException{
		
		Schema schema = new Schema.Parser().parse(TestAvro.class.getResourceAsStream("/CustomMessage.avsc"));
		GenericRecord mesg = new GenericData.Record(schema);		
		mesg.put("id", "device1");
		mesg.put("payload", "{'type':'internal json'}");
		
		//create avro
		byte[] avro = AvroUtils.serializeJson(mesg.toString(), schema);
		
		//avro to java
		CustomSimpleMessage msg1 = AvroUtils.avroToJava(avro, schema, CustomSimpleMessage.class);
		Assert.assertEquals(msg1.getPayload().toString(),mesg.get("payload").toString());
		
		byte[] avro2 = AvroUtils.serializeJava(msg1, schema);
		
		Assert.assertNotEquals(avro,avro2);//tests data abstraction 
		
		GenericRecord genericTest = AvroUtils.avroToJava(avro2, schema);
		
		Assert.assertEquals(mesg.get("payload").toString(),genericTest.get("payload").toString());
		
	}
	
	
	

}
