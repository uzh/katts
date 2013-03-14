package ch.uzh.ddis.katts.evaluation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.TopologyInfo;

/**
 * This class resolves task related information depending on the storm topology definition.
 * 
 * @author Thomas Hunziker
 * 
 */
class TaskResolver {

	private Map<String, String> tasks = new HashMap<String, String>();
	private Map<String, String> components = new HashMap<String, String>();

	public TaskResolver(TopologyInfo topology) {
		List<ExecutorSummary> executors = topology.get_executors();
		for (ExecutorSummary executor : executors) {
			int start = executor.get_executor_info().get_task_start();
			int end = executor.get_executor_info().get_task_end();
			for (int i = start; i <= end; i++) {
				tasks.put(Integer.toString(i), executor.get_host());
				components.put(Integer.toString(i), executor.get_component_id());
			}
		}
	}

	public String getHostnameByTask(String taskId) {
		return tasks.get(taskId);
	}

	public String getComponentIdByTask(String taskId) {
		return components.get(taskId);
	}

}
