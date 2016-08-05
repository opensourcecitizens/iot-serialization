package io.parser.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.codehaus.jackson.map.ObjectMapper;

public class AvroUtils <T> {

	 /**
     * Convert JSON to avro binary array.
     * 
     * @param json
     * @param schema
     * @return
     * @throws IOException
     */
    public static byte[] serializeJson(String json, Schema schema) throws IOException {
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        Object datum = reader.read(null, decoder);
        writer.write(datum, encoder);
        encoder.flush();
        return output.toByteArray();
    }
    
    
	 /**
     * Convert JSON to avro binary array.
     * 
     * @param json
     * @param schema
     * @return byte[] serialized avro object
     * @throws IOException
     */
    public static byte[] serializeJson(GenericRecord msg, Schema schema) throws IOException {
        DatumReader<Object> reader = new GenericDatumReader<>(schema);
        GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, msg.toString());
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
        Object datum = reader.read(null, decoder);
        writer.write(datum, encoder);
        encoder.flush();
        return output.toByteArray();
    }

    /**
     * Convert Avro binary byte array back to JSON String.
     * 
     * @param avro
     * @param schema
     * @return
     * @throws IOException
     */
    public static String avroToJson(byte[] avro, Schema schema) throws IOException {
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(schema, output);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
        Object datum = reader.read(null, decoder);
        writer.write(datum, encoder);
        encoder.flush();
        output.flush();
        return new String(output.toByteArray(), "UTF-8");
    }
    
    
    /**
     * Convert avro binary to Java object.
     * 
     * @param avro - avro binary
     * @param schema - avro schema
     * @return Returns <T> type which specifies the java object type being operated on. It specified at this class' instantiation.
     * @throws IOException
     */
    public static <GenericDataRecord>  GenericDataRecord avroToJava(byte[] avro, Schema schema) throws IOException {

        GenericDatumReader<GenericDataRecord> reader = new SpecificDatumReader<GenericDataRecord>(schema);  
        Decoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
        GenericDataRecord genericDatum = reader.read(null, decoder);
  
        return genericDatum;
    }
    
    /**
     * Convert avro binary to Java object.
     * 
     * @param avro - avro binary
     * @param schema - avro schema
     * @return Returns <T> type which specifies the java object type being operated on. It specified at this class' instantiation.
     * @throws IOException
     */
	public static <T> T avroToJava(byte[] avro, Schema schema, Class<T> clazz) throws IOException {

        String json = avroToJson(avro,schema);
        
        ObjectMapper mapper = new ObjectMapper();
        T specificDatum =  mapper.readValue(json, clazz);
        return specificDatum;
    }
    
    /**
     * Convert Java object to json to avro binary array.
     * 
     * @param object - Java data object 
     * @param schema - avro schema
     * @return Returns byte[] - serialized avro data.
     * 
     * @throws IOException
     */
    public static <T>  byte[] serializeJava(T object, Schema schema) throws IOException {

    	ObjectMapper mapper = new ObjectMapper();
    	String json = mapper.writeValueAsString(object);
    	
    	return serializeJson(json , schema);
    }
    
   
    
    /**
     *  Convert Java object to avro binary array.
     * 
     * @param object - Java data object created by Avro from schema
     * @param schema - avro schema
     * @return Returns byte[] - serialized avro data.
     * 
     * @throws IOException
     */
    public static byte[] serializeJava(GenericRecord object, Schema schema) throws IOException {

        GenericDatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream(); 
        
        Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);

        writer.write(object, encoder);
        encoder.flush();
        
        return output.toByteArray();
    }
	
	
}
