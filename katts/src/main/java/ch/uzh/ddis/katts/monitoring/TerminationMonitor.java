package ch.uzh.ddis.katts.monitoring;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import ch.uzh.ddis.katts.RunXmlQuery;

public class TerminationMonitor {

	private Thread runner;

	private static TerminationMonitor instance;

	private boolean isDataSendToOutput = false;

	private int terminationCheckInterval = 20000;
	
	private long lastOutputSendOn = 0;
	
	private long messageCount = 0;

	private String pathToWriteFileWhenTerminated = null;
	
	private String evaluationFolder = null;
	
	public static final String CONF_TERMINATION_CHECK_INTERVAL = "katts.terminationCheckInterval";
	public static final String CONF_TERMINATION_FILE_PATH = "katts.terminationFilePath";
	

	private TerminationMonitor(Map stormConf) {

		if (stormConf.containsKey(CONF_TERMINATION_CHECK_INTERVAL)) {
			terminationCheckInterval = Integer.valueOf((String) stormConf.get(CONF_TERMINATION_CHECK_INTERVAL));
		}

		if (stormConf.containsKey(CONF_TERMINATION_FILE_PATH)) {
			pathToWriteFileWhenTerminated = (String) stormConf.get(CONF_TERMINATION_FILE_PATH);
		}

		if (stormConf.containsKey(RunXmlQuery.CONF_EVALUATION_FOLDER_NAME)) {
			evaluationFolder = (String) stormConf.get(RunXmlQuery.CONF_EVALUATION_FOLDER_NAME);
		}

		if (terminationCheckInterval > 0 ) {

			WaitMonitor waiter = new WaitMonitor(this);

			runner = new Thread(waiter);
			runner.start();
		}

	}

	public static TerminationMonitor getInstance(Map stormConf) {
		if (instance == null) {
			instance = new TerminationMonitor(stormConf);
		}

		return instance;
	}

	public synchronized void dataIsSendToOutput() {
		isDataSendToOutput = true;
		lastOutputSendOn = System.currentTimeMillis();
		messageCount++;
	}
	
	
	private class WaitMonitor implements Runnable {

		TerminationMonitor monitor;
		boolean isStopped = false;

		WaitMonitor(TerminationMonitor monitor) {
			this.monitor = monitor;
		}

		@Override
		public void run() {

			while (!isStopped) {
				monitor.isDataSendToOutput = false;
				try {
					Thread.sleep(monitor.terminationCheckInterval);
				} catch (InterruptedException e) {
					throw new RuntimeException(
							"The termination monitor could not send the monitor thread to background.", e);
				}
				
				// When the flag is not changed, then we know that in the measured interval
				// no data is send to output.
				if (!monitor.isDataSendToOutput) {
					isStopped = true;
					
					if (monitor.pathToWriteFileWhenTerminated != null) {
						try {
							File file = new File(monitor.pathToWriteFileWhenTerminated);
							file.getParentFile().mkdirs();
							FileUtils.writeStringToFile(file, Long.toString(monitor.lastOutputSendOn));
						} catch (IOException e) {
							throw new RuntimeException("The termination monitor could not write the termination file.", e);
						}
					}
					
					if (monitor.evaluationFolder != null) {
						try {
							File file = new File(monitor.evaluationFolder + "/number_of_tuples_outputed");
							file.getParentFile().mkdirs();
							FileUtils.writeStringToFile(file, Long.toString(monitor.messageCount));
						} catch (IOException e) {
							throw new RuntimeException("The termination monitor could create the file for the number of tuples outputed.", e);
						}
					}
					
					
				}
			}
		}
	}

}
