package protobuf;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import protobuf.syntax2.CatProto;
import protobuf.syntax2.CatProto.Cat;

public class TestProtobuf {

	public static void main(String[] args) {
		 
	    // creating the cat
	    Cat pusheen= CatProto.Cat.newBuilder()
	            .setAge(3)
	            .setName("Pusheen") 
	            .build();
	    try {
	       // write
	        FileOutputStream output = new FileOutputStream("cat.ser");
	        pusheen.writeTo(output);
	        output.close();
	 
	       // read
	        Cat catFromFile = Cat.parseFrom(new FileInputStream("cat.ser"));
	        System.out.println(catFromFile);
	 
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	

}
