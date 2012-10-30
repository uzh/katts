package ch.uzh.ddis.katts.monitoring;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapIterator;
import org.apache.commons.collections.keyvalue.MultiKey;
import org.apache.commons.collections.map.MultiKeyMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVWriter;


// TODO: Write out the counters, when the class is destroyed


public final class Recorder {

	private static Recorder instance;

	public static final String MONITORING_FOLDER_PATH = "katts.monitoring.folder.path";

	private String monitoringPath;
	private String topologyName;

	private Map stormConfiguration;

	/**
	 * This configuration sets after how many messages an record is written.
	 */
	private static final long NUMBER_OF_MESSAGE_PER_RECORD = 100;
	
	private MultiKeyMap messageCounter = new MultiKeyMap();
	

	private Recorder(Map stormConf, String topologyName) {
		this.stormConfiguration = stormConf;
		this.topologyName = topologyName;
		
//		try {
////			createMonitoringDirectoryIfNotExists();
////			messageCountWriter 		= new CSVWriter(new FileWriter(this.getFilePath("message_count")));
////			taskCsvWriter 			= new CSVWriter(new FileWriter(this.getFilePath("tasks")));
////			finalMessageCountWriter = new CSVWriter(new FileWriter(this.getFilePath("final_message_count")));
////			vmStatsWriter 			= new CSVWriter(new FileWriter(this.getFilePath("vm_stats")));
//			
//		} catch (IOException e) {
//			throw new RuntimeException("Could not intialize the monitoring file.", e);
//		}
		
//		Runtime.getRuntime().addShutdownHook(new Thread() {
//			@Override
//			public void run() {
//				writeFinalCounts();
//			}
//		});
		
	}

	public static synchronized Recorder getInstance(Map stormConf, String topologyName) {

		if (instance == null) {
			instance = new Recorder(stormConf, topologyName);

			// Start Virtual Machine Monitoring
			Thread vmMonitor = new Thread(new VmMonitor(instance, (Long) stormConf.get(VmMonitor.RECORD_INVERVAL)));
			vmMonitor.start();
		}

		return instance;
	}

	public synchronized void recordMessageSending(int sourceTask, int targetTask) {
		
		Long counter = (Long)messageCounter.get(sourceTask, targetTask);
		if (counter == null) {
			counter = 1l;
		}
		else {
			counter++;
		}
		
		messageCounter.put(sourceTask, targetTask, counter);
			
	}

	public synchronized void recordMemoryStats(long maxMemory, long allocatedMemory, long freeMemory) {
		// TODO: Find a way to log this data in some other way.
		
//		String[] line = new String[5];
//		line[0] = Long.toString(System.currentTimeMillis());
//		line[1] = this.getHostName();
//		line[2] = Long.toString(maxMemory);
//		line[3] = Long.toString(allocatedMemory);
//		line[4] = Long.toString(freeMemory);
//		
//		vmStatsWriter.writeNext(line);
//		try {
//			vmStatsWriter.flush();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
		
	}

	public String getMonitoringPath() {
		return monitoringPath;
	}

	public void setMonitoringPath(String monitoringPath) {
		this.monitoringPath = monitoringPath;
	}

	
//	
//	private void writeFinalCounts() {
//		MapIterator it = messageCounter.mapIterator();
//		
//		// The final message count file structure is: sourceTaskId, destinationTaskId, messageCount
//		
//		while(it.hasNext()) {
//			MultiKey next = (MultiKey) it.next();
//			
//			String[] line = new String[3];
//			int i = 0;
//			for (Object key : next.getKeys()) {
//				line[i] = ((Integer)key).toString();
//				i++;
//			}
//			line[i] = Long.toString((Long)messageCounter.get(next));
//			
//			finalMessageCountWriter.writeNext(line);
//		}
//		
//		try {
//			finalMessageCountWriter.flush();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//	}
//	

}
