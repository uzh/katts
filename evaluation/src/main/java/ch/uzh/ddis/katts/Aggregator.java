package ch.uzh.ddis.katts;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import com.google.gdata.util.ServiceException;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

public class Aggregator {
	
	private String pathToEvaluationFolder;
	private String jobName;
	
	private TaskResolver taskResolver;
	
	private GoogleSpreadsheet googleSpreadsheet;
	
	public Aggregator(String pathToEvaluationFolder, String jobName){
		
		this.jobName = jobName;
		this.pathToEvaluationFolder = pathToEvaluationFolder;
		
		try {
			taskResolver = new TaskResolver(pathToEvaluationFolder + "/aggregates/tasks.csv");
		} catch (IOException e) {
			throw new RuntimeException("The task mapping file could not be read.", e);
		}
		
	}
	
	public void aggregateMessagePerHost() {
		
		CSVReader csvReader;
		try {
			csvReader = new CSVReader(new FileReader(pathToEvaluationFolder + "/aggregates/final_message_count.csv"));
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
		HashMap<String, Counter> interNodeMessages = new HashMap<String, Counter>();
		
		long totalRemoteMessages = 0;
		long totalLocalMessages = 0;
		long totalMessages = 0;
		
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
			
			if (sourceHost.equals(destinationHost)) {
				Counter interProcessCounter = interNodeMessages.get(sourceHost);
				if (interProcessCounter == null) {
					interProcessCounter = new Counter();
					interNodeMessages.put(sourceHost, interProcessCounter);
				}
				
				interProcessCounter.increase(messageCount);
				
				totalLocalMessages = totalLocalMessages + messageCount;
			}
			else {
				totalRemoteMessages = totalRemoteMessages + messageCount;
			}
			totalMessages = totalMessages + messageCount;
		}
		
		try {
			writeMapToCsv("messages_sent_per_host", sendMessagesPerHost);
			writeMapToCsv("messages_received_per_host", receivedMessagesPerHost);
			writeMapToCsv("messages_intermachine_per_host", sendMessagesPerHost);
		}
		catch(IOException e1) {
			throw new RuntimeException(e1);
		}
		
		if (googleSpreadsheet != null) {
			JobData job = new JobData();
			
			job.setJobName(this.jobName);
			job.setExpectedNumberOfTasks(Integer.valueOf(readValueFromEvaluationFolder("expected_number_of_tasks")));
			job.setJobEnd(Long.valueOf(readValueFromEvaluationFolder("terminated_on")));
			job.setJobStart(Long.valueOf(readValueFromEvaluationFolder("start")));
			job.setNumberOfNodes(Integer.valueOf(readValueFromEvaluationFolder("number_of_nodes")));
			job.setNumberOfProcessors(Integer.valueOf(readValueFromEvaluationFolder("number_of_processors")));
			job.setNumberOfProcessorsPerNode(Integer.valueOf(readValueFromEvaluationFolder("number_of_processors_per_node")));
			job.setNumberOfTuplesOutputed(Long.valueOf(readValueFromEvaluationFolder("number_of_tuples_outputed")));
			job.setTotalLocalMessages(totalLocalMessages);
			job.setTotalRemoteMessages(totalRemoteMessages);
			job.setTotalMessages(totalMessages);
			job.setFactorOfThreadsPerProcessor(Float.valueOf(readValueFromEvaluationFolder("factor_of_threads_per_processor")));
		
			try {
				googleSpreadsheet.addRow(job);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		
		
	}
	
	private String readValueFromEvaluationFolder(String name) {
		try {
			File file = new File(this.pathToEvaluationFolder + "/" + name);
			return FileUtils.readFileToString(file).trim();
		} catch (IOException e) {
			throw new RuntimeException("Cant read a value from the evaluation folder.");
		}
	}
	
	private void writeMapToCsv(String file, HashMap<String, Counter> map) throws IOException {
		CSVWriter csvWriter = new CSVWriter(new FileWriter(pathToEvaluationFolder + "/aggregates/" + file + ".csv"));
		for (Entry<String, Counter> counter : map.entrySet()) {
			
			String[] outputLines = new String[2];
			outputLines[0] = counter.getKey();
			outputLines[1] = Long.toString(counter.getValue().getCounter());
			csvWriter.writeNext(outputLines);
		}
		csvWriter.flush();
		csvWriter.close();
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


	public GoogleSpreadsheet getGoogleSpreadsheet() {
		return googleSpreadsheet;
	}

	public void setGoogleSpreadsheet(GoogleSpreadsheet googleSpreadsheet) {
		this.googleSpreadsheet = googleSpreadsheet;
	}
	
}
