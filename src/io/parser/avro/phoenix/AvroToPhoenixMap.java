package io.parser.avro.phoenix;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class AvroToPhoenixMap {
	Log logger = LogFactory.getLog(AvroToPhoenixMap.class);
	//Schema.Type[] avroTypes = Schema.Type.values();
	
	public String translate(Type type){
		//RECORD ENUM ARRAY MAP UNION FIXED STRING BYTES STRING INT LONG DOUBLE FLOAT BOOLEAN NULL
		String ret = null;
		switch(type.getName()){
		case "RECORD":	ret=""; break;	
		case "ENUM":	ret=""; break;	
		case "ARRAY":	ret=""; break;	
		case "MAP":		ret=""; break;	
		case "UNION":	ret=""; break;	
		case "FIXED":	ret=""; break;	
		case "STRING":	ret=""; break;	
		case "BYTES":	ret=""; break;	
		case "INT":		ret=""; break;	
		case "LONG":	ret=""; break;	
		case "DOUBLE":	ret=""; break;	
		case "FLOAT":	ret=""; break;	
		case "BOOLEAN":	ret=""; break;	
		case "NULL": 	ret=""; break;	
		default: ret = null;
		}
		
		return ret;
	}
	
	public void translate(PreparedStatement prepStmt, Map<String, ?> map, Schema avroSchema){
		
		Set<String> keys = map.keySet();
		int i = 1;
		
		for(String key: keys){
			Type filedType = avroSchema.getField(key).schema().getType();
			System.out.println(filedType.getName());
			try{
			switch(filedType.getName().toUpperCase()){
			case "RECORD":	prepStmt.setString(i, toJson(map.get(key)))	; break;	
			case "ENUM":	prepStmt.setString(i, ((Enum<?>)map.get(key)).toString()); break;	
			case "ARRAY":	/*ignore*/; break;	
			case "MAP":		prepStmt.setString(i, toJson(map.get(key))); break;	
			case "UNION":	/*ignore*/; break;	
			case "FIXED":	/*ignore*/; break;	
			case "STRING":	prepStmt.setString(i, (String)map.get(key)); break;		
			case "BYTES":	prepStmt.setBytes(i, (byte[]) map.get(key)); break;		
			case "INT":		prepStmt.setInt(i, (Integer) map.get(key)); break;	
			case "LONG":	prepStmt.setLong(i, (Long) map.get(key)); break;	
			case "DOUBLE":	prepStmt.setDouble(i, (Double) map.get(key)); break;		
			case "FLOAT":	prepStmt.setFloat(i, (Float) map.get(key)); break;		
			case "BOOLEAN":	prepStmt.setBoolean(i, (Boolean) map.get(key)); break;		
			case "NULL": 	/*ignore*/; break;	
			}
			}catch(Exception e){
				try {
					prepStmt.setString(i, "Translation Error caused by: "+e.getMessage());
				} catch (SQLException e1) {
					//e1.printStackTrace();
				} finally{
					logger.error(e,e);
				}
			}
			
			i++;
		}
	}
	

	private String toJson(Object map) throws JsonGenerationException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(map);
	}

	
}
