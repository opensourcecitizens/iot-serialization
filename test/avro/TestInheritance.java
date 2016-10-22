package avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.util.Utf8;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;


public class TestInheritance {

		private Schema user;
        private Schema FBUser;
        private Schema base;
        private Schema ext1;
        private Schema ext2;

        //@Before
        @Test
        public void setUp() throws Exception {
        	 
        	user = AvroUtils.parseSchema(
                    new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookUser.avro"));

        	ext1 = AvroUtils.parseSchema(
                     new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookSpecialUserExtension1.avro"));
             ext2 = AvroUtils.parseSchema(
                     new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookSpecialUserExtension2.avro"));

             FBUser = AvroUtils.parseSchema(new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookSpecialUser1.avro"));
             base = AvroUtils.parseSchema(new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookSpecialUser.avro"));
             

                                
        }
        
        @Test
        public void setUpDependencies() throws JsonProcessingException, IOException{
        	ObjectMapper mapper = new ObjectMapper();
        	
        	JsonNode fbExt1json = mapper.readTree(new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookSpecialUserExtension1.avro"));
        	JsonNode fbExt2json = mapper.readTree(new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookSpecialUserExtension2.avro"));
        	JsonNode basejson = mapper.readTree(new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookSpecialUser.avro"));
        	JsonNode fbUsrjson = mapper.readTree(new File("/Users/kndungu/Documents/workspace/iot-serialization/test/avro/resource/FacebookSpecialUser1.avro"));
        }

        @Test
        public void testInheritanceBinary() throws Exception{
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                 GenericDatumWriter<GenericRecord> writer =
                         new GenericDatumWriter<GenericRecord>(FBUser);
                // Encoder encoder = new BinaryEncoder(outputStream);
                 Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream,null);
                 
                 
                 
                 GenericRecord baseRecord = new GenericData.Record(base);
                 baseRecord.put("name", new Utf8("Doctor Who"));
                 baseRecord.put("num_likes", 1);
                 baseRecord.put("num_photos", 0);
                 baseRecord.put("num_groups", 423);
                 GenericRecord FBrecord = new GenericData.Record(FBUser);
                 FBrecord.put("type", "base");
                 FBrecord.put("user", baseRecord);

                 writer.write(FBrecord, encoder);

                 baseRecord = new GenericData.Record(base);
                 baseRecord.put("name", new Utf8("Doctor WhoWho"));
                 baseRecord.put("num_likes", 1);
                 baseRecord.put("num_photos", 0);
                 baseRecord.put("num_groups", 423);
                 GenericRecord extRecord = new GenericData.Record(ext1);
                 extRecord.put("specialData1", 1);
                 FBrecord = new GenericData.Record(FBUser);
                 FBrecord.put("type", "extension1");
                 FBrecord.put("user", baseRecord);
                 FBrecord.put("extension", extRecord);

                 writer.write(FBrecord, encoder);
                 
                 GenericRecord userRecord = new GenericData.Record(user);
                 userRecord.put("age", null);
                 
                 baseRecord = new GenericData.Record(base);
                 baseRecord.put("name", new org.apache.avro.util.Utf8("Doctor WhoWhoWho"));
                 baseRecord.put("num_likes", 2);
                 baseRecord.put("num_photos", 0);
                 baseRecord.put("num_groups", 424);
                 extRecord = new GenericData.Record(ext2);
                 extRecord.put("specialData2", 2);
                 FBrecord = new GenericData.Record(FBUser);
                 FBrecord.put("type", "extension2");
                 FBrecord.put("user", userRecord);
                 FBrecord.put("extension", extRecord);

                 writer.write(FBrecord, encoder);

                 encoder.flush();

                 byte[] data = outputStream.toByteArray();
                 ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
                 Decoder decoder =
                        DecoderFactory.defaultFactory().createBinaryDecoder(inputStream, null);
                 GenericDatumReader<GenericRecord> reader =
                        new GenericDatumReader<GenericRecord>(FBUser);
                 while(true){
                        try{
                               GenericRecord result = reader.read(null, decoder);
                               System.out.println(result);
                        }
                        catch(EOFException eof){
                               break;
                        }
                        catch(Exception ex){
                               ex.printStackTrace();
                        }
                 }
        }
}