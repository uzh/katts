package ch.uzh.ddis.katts.evaluation;

import java.util.Map;

/**
 * This class holds all information logged into the google spreadsheet.
 * 
 * @author Thomas Hunziker
 *
 */
class JobData {

	private String jobName;
	
	private long jobStart;
	private long jobEnd;
	
	private int numberOfNodes;
	private long expectedNumberOfTasks;
	private long numberOfProcessors;
	private long numberOfProcessorsPerNode;
	
	
	private long totalMessages;
	private long totalLocalMessages;
	private long totalRemoteMessages;
	private long numberOfTuplesOutputed;
	private long numberOfTriplesProcessed;
	
	private String sankeyDataHosts;
	private String sankeyDataTasks;
	private String sankeyDataComponents;
	
	private double factorOfThreadsPerProcessor;
	
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
	public long getExpectedNumberOfTasks() {
		return expectedNumberOfTasks;
	}
	public void setExpectedNumberOfTasks(long expectedNumberOfTasks) {
		this.expectedNumberOfTasks = expectedNumberOfTasks;
	}
	public long getNumberOfProcessors() {
		return numberOfProcessors;
	}
	public void setNumberOfProcessors(long numberOfProcessors) {
		this.numberOfProcessors = numberOfProcessors;
	}
	public long getNumberOfProcessorsPerNode() {
		return numberOfProcessorsPerNode;
	}
	public void setNumberOfProcessorsPerNode(long numberOfProcessorsPerNode) {
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
	public double getFactorOfThreadsPerProcessor() {
		return factorOfThreadsPerProcessor;
	}
	public void setFactorOfThreadsPerProcessor(double factorOfThreadsPerProcessor) {
		this.factorOfThreadsPerProcessor = factorOfThreadsPerProcessor;
	}
	public long getNumberOfTriplesProcessed() {
		return numberOfTriplesProcessed;
	}
	public void setNumberOfTriplesProcessed(long numberOfTriplesProcessed) {
		this.numberOfTriplesProcessed = numberOfTriplesProcessed;
	}
	public String getSankeyDataHosts() {
		return sankeyDataHosts;
	}
	public void setSankeyDataHosts(String sankeyDataHosts) {
		this.sankeyDataHosts = sankeyDataHosts;
	}
	public String getSankeyDataTasks() {
		return sankeyDataTasks;
	}
	public void setSankeyDataTasks(String sankeyDataTasks) {
		this.sankeyDataTasks = sankeyDataTasks;
	}
	public String getSankeyDataComponents() {
		return sankeyDataComponents;
	}
	public void setSankeyDataComponents(String sankeyDataComponents) {
		this.sankeyDataComponents = sankeyDataComponents;
	}
	
}
