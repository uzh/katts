package ch.uzh.ddis.katts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.NumberFormat;

import org.apache.commons.collections.MapIterator;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;

public class TestLogging {
	private static MultiKeyMap messageCounter = new MultiKeyMap();
	
	public static void main(String[] args) throws IOException, InterruptedException {

		
		messageCounter.put(18, 1, 50);
		messageCounter.put(18, 2, 51);
		messageCounter.put(18, 9, 52);
		messageCounter.put(18, 8, 53);
		messageCounter.put(18, 7, 54);
		
		
		messageCounter.put(16, 1, 55);
		messageCounter.put(16, 2, 56);
		messageCounter.put(16, 9, 57);
		messageCounter.put(16, 8, 58);
		messageCounter.put(16, 7, 59);
		
		MapIterator it = messageCounter.mapIterator();
		while(it.hasNext()) {
			MultiKey next = (MultiKey) it.next();
			
			for (Object key : next.getKeys()) {
				System.out.print(key);
				System.out.print(" ");
			}
			System.out.print(messageCounter.get(next));
			System.out.println();
			
			
		}
		
		
		
	}

}
