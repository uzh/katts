package ch.uzh.ddis.katts.monitoring;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class TerminationMonitor {

	private Thread runner;

	private static TerminationMonitor instance;

	private boolean isDataSendToOutput = false;

	private int terminationCheckInterval = 30000;
	
	private long lastOutputSendOn = 0;

	private String pathToWriteFileWhenTerminated = null;
	
	public static final String CONF_TERMINATION_CHECK_INTERVAL = "katts.terminationCheckInterval";
	public static final String CONF_TERMINATION_FILE_PATH = "katts.terminationFilePath";
	

	private TerminationMonitor(Map stormConf) {

		if (stormConf.containsKey("katts.terminationCheckInterval")) {
			terminationCheckInterval = Integer.valueOf((String) stormConf.get(CONF_TERMINATION_CHECK_INTERVAL));
		}

		if (stormConf.containsKey("katts.terminationCheckInterval")) {
			pathToWriteFileWhenTerminated = (String) stormConf.get(CONF_TERMINATION_FILE_PATH);
		}

		if (terminationCheckInterval > 0 ) {

			WaitMonitor waiter = new WaitMonitor(this);

			runner = new Thread(waiter);
			runner.run();
		}

	}

	public static TerminationMonitor getInstance(Map stormConf) {
		if (instance == null) {
			instance = new TerminationMonitor(stormConf);
		}

		return instance;
	}

	public void dataIsSendToOutput() {
		isDataSendToOutput = true;
		lastOutputSendOn = System.currentTimeMillis();
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
							FileWriter fstream;
							fstream = new FileWriter(file);
							BufferedWriter out = new BufferedWriter(fstream);
							Long millis = monitor.lastOutputSendOn;
							out.write(millis.toString());
							out.close();
						} catch (IOException e) {
							throw new RuntimeException("The termination monitor could not write the termination file.", e);
						}
					}
					
				}
			}
		}
	}

}
