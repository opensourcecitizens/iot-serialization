package io.parser.avro;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class AvroParser<T>{

    private final ObjectMapper mapper = new ObjectMapper();
    private Schema schema = null;

    public AvroParser(Schema schema) {
        this.schema = schema;}
    
    public void setSchema(Schema schema) {
        this.schema = schema;
    }
    
    /**
     * 
     * @param avroSchemaFile Avro Schema as File.
     * @throws IOException
     */
    public void setSchema(File avroSchemaFile) throws IOException  {
    	this.schema = new Schema.Parser().parse(avroSchemaFile);
    }

    /**
     * 
     * @param avroSchemaString Avro Schema as String.
     * @throws IOException
     */
    public void setSchema(String avroSchemaString)  {
    	this.schema = new Schema.Parser().parse(avroSchemaString);
    }
    
    /**
     * 
     * @param avroSchemaStream Avro Schema as InputStream.
     * @throws IOException
     */
    public void setSchema(InputStream avroSchemaStream) throws IOException  {
    	this.schema = new Schema.Parser().parse(avroSchemaStream);
    }

    public Map<String, ?> parse(byte[] avrodata, Map<String,?> returntype) throws Exception {
        try {
        	
        	String data =  AvroUtils.avroToJson(avrodata, schema);
            return mapper.readValue(data, new TypeReference<Map<String, ?>>(){});
            
            //return AvroUtils.avroToJava(data, schema, new TypeReference<Map<String, ?>>(){});
        } catch (IOException e) {
            throw new Exception("Error trying to parse data.", e);
        }
    }
    
    public String parse(byte[] avrodata, String returntype) throws Exception {
        try {
        	
        	String data =  AvroUtils.avroToJson(avrodata, schema);
        	
            return data;
            
        } catch (IOException e) {
            throw new Exception("Error trying to parse data.", e);
        }
    }
    
    public GenericRecord parse(byte[] avrodata, GenericRecord returntype) throws Exception {
    	
        try {
        	
        	GenericRecord data =  AvroUtils.avroToJava(avrodata, schema);
            return data;
            
        } catch (IOException e) {
            throw new Exception("Error trying to parse data.", e);
        }
        
    }
    
    
    public Map<String, ?> parse(byte[] avrodata, Schema schema) throws Exception {
       setSchema(schema);
       return parse(avrodata, new HashMap<String , Object>());
    }
    
    
}
