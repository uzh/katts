package ch.uzh.ddis.katts;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;

public class Aggregator {
	
	private String pathToEvaluationFolder;
	
	private TaskResolver taskResolver;
	
	public Aggregator(String pathToEvaluationFolder){
		
		this.pathToEvaluationFolder = pathToEvaluationFolder;
		
		try {
			taskResolver = new TaskResolver(pathToEvaluationFolder + "/tasks");
		} catch (IOException e) {
			throw new RuntimeException("The task mapping file could not be read.", e);
		}
		
	}
	
	public void aggregateMessagePerHost() {
		
		CSVReader csvReader;
		try {
			csvReader = new CSVReader(new FileReader(pathToEvaluationFolder + "/final_message_count"));
		} catch (FileNotFoundException e) {
			throw new RuntimeException("The final_message_count file count not be read.", e);
		}
		
		List<String[]> lines;
		try {
			lines = csvReader.readAll();
		} catch (IOException e) {
			throw new RuntimeException("The final_message_count file count not be read.", e);
		}
		
		HashMap<String, Counter> receivedMessagesPerHost = new HashMap<String, Counter>();
		HashMap<String, Counter> sendMessagesPerHost = new HashMap<String, Counter>();
		
		for (String[] line : lines) {
			
			// The final message count file structure is: sourceTaskId, destinationTaskId, messageCount 
			String sourceHost = taskResolver.getHostnameByTask(line[0]);
			String destinationHost = taskResolver.getHostnameByTask(line[1]);
			
			if (sourceHost == null) {
				sourceHost = "undefined-host-name";
			}
			
			if (destinationHost == null) {
				destinationHost = "undefined-host-name";
			}
			
			Counter receiverCounter = receivedMessagesPerHost.get(destinationHost);
			if (receiverCounter == null) {
				receiverCounter = new Counter();
				receivedMessagesPerHost.put(destinationHost, receiverCounter);
			}
			
			
			Counter senderCounter = sendMessagesPerHost.get(sourceHost);
			if (senderCounter == null) {
				senderCounter = new Counter();
				sendMessagesPerHost.put(sourceHost, senderCounter);
			}
			
			long messageCount = Long.valueOf(line[2]);
			receiverCounter.increase(messageCount);
			senderCounter.increase(messageCount);
		}
		
		
		
		
		
	}
	
	
	private class Counter {
		long counter = 0;
		
		public void increase(long number) {
			counter = counter + number;
		}
		
		public long getCounter() {
			return counter;
		}
	}
	
}
