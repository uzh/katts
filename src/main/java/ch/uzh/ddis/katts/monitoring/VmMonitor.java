package ch.uzh.ddis.katts.monitoring;

///**
// * This is protypical implementation for monitoring the memory consumption of the Java VM.
// * 
// * @author Thomas Hunziker
// *
// */
//public class VmMonitor implements Runnable {
//
//	private Recorder recorder;
//	
//	public static final String RECORD_INVERVAL = "katts.monitoring.vm.record.interval";
//
//	private boolean run = true;
//
//	/**
//	 * The time between measuring points in milliseconds.
//	 */
//	private long interval = 1000;
//
//	/**
//	 * 
//	 * @param interval
//	 *            The time between measuring points in seconds.
//	 */
//	public VmMonitor(Recorder recorder, long interval) {
//		this.recorder = recorder;
//		this.interval = interval * 1000;
//	}
//
//	@Override
//	public void run() {
//		
//		Runtime runtime = Runtime.getRuntime();
//		
//		while (this.run) {
//			long maxMemory = runtime.maxMemory();
//			long allocatedMemory = runtime.totalMemory();
//			long freeMemory = runtime.freeMemory();
//			recorder.recordMemoryStats(maxMemory, allocatedMemory, freeMemory);
//			try {
//				Thread.sleep(interval);
//			} catch (InterruptedException e) {
//				// Ignore, when the process is interrupted, only stop the 
//				// thread.
//				this.stopRunning();
//			}
//		}
//	}
//
//	public void stopRunning() {
//		run = false;
//	}
//
//}
