package io.parser.avro.phoenix;

import java.sql.PreparedStatement;

import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

public class AvroToPhoenixMap {
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
			case "RECORD":	/*ignore*/; break;	
			case "ENUM":	prepStmt.setString(i, ((Enum<?>)map.get(key)).toString()); break;	
			case "ARRAY":	/*ignore*/; break;	
			case "MAP":		/*ignore*/; break;	
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
				//TODO better exception handling
				e.printStackTrace();
			}
			
			i++;
		}
	}

}
