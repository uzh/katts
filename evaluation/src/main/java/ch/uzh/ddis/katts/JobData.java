package ch.uzh.ddis.katts;

import java.util.Map;

/**
 * This class holds all information logged into the google spreadsheet.
 * 
 * @author Thomas Hunziker
 *
 */
public class JobData {

	private String jobName;
	
	private long jobStart;
	private long jobEnd;
	
	private int numberOfNodes;
	private int expectedNumberOfTasks;
	private int numberOfProcessors;
	private int numberOfProcessorsPerNode;
	
	
	private long totalMessages;
	private long totalLocalMessages;
	private long totalRemoteMessages;
	private long numberOfTuplesOutputed;
	
	private float factorOfThreadsPerProcessor;
	
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public long getJobStart() {
		return jobStart;
	}
	public void setJobStart(long jobStart) {
		this.jobStart = jobStart;
	}
	public long getJobEnd() {
		return jobEnd;
	}
	public void setJobEnd(long jobEnd) {
		this.jobEnd = jobEnd;
	}
	public int getNumberOfNodes() {
		return numberOfNodes;
	}
	public void setNumberOfNodes(int numberOfNodes) {
		this.numberOfNodes = numberOfNodes;
	}
	public int getExpectedNumberOfTasks() {
		return expectedNumberOfTasks;
	}
	public void setExpectedNumberOfTasks(int expectedNumberOfTasks) {
		this.expectedNumberOfTasks = expectedNumberOfTasks;
	}
	public int getNumberOfProcessors() {
		return numberOfProcessors;
	}
	public void setNumberOfProcessors(int numberOfProcessors) {
		this.numberOfProcessors = numberOfProcessors;
	}
	public int getNumberOfProcessorsPerNode() {
		return numberOfProcessorsPerNode;
	}
	public void setNumberOfProcessorsPerNode(int numberOfProcessorsPerNode) {
		this.numberOfProcessorsPerNode = numberOfProcessorsPerNode;
	}
	public long getTotalMessages() {
		return totalMessages;
	}
	public void setTotalMessages(long totalMessages) {
		this.totalMessages = totalMessages;
	}
	public long getTotalLocalMessages() {
		return totalLocalMessages;
	}
	public void setTotalLocalMessages(long totalLocalMessages) {
		this.totalLocalMessages = totalLocalMessages;
	}
	public long getTotalRemoteMessages() {
		return totalRemoteMessages;
	}
	public void setTotalRemoteMessages(long totalRemoteMessages) {
		this.totalRemoteMessages = totalRemoteMessages;
	}
	public long getNumberOfTuplesOutputed() {
		return numberOfTuplesOutputed;
	}
	public void setNumberOfTuplesOutputed(long numberOfTuplesOutputed) {
		this.numberOfTuplesOutputed = numberOfTuplesOutputed;
	}
	
	public long getJobDuration() {
		return this.getJobEnd() - this.getJobStart();
	}
	public float getFactorOfThreadsPerProcessor() {
		return factorOfThreadsPerProcessor;
	}
	public void setFactorOfThreadsPerProcessor(float factorOfThreadsPerProcessor) {
		this.factorOfThreadsPerProcessor = factorOfThreadsPerProcessor;
	}
	
}
