package ch.uzh.ddis.katts.evaluation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.gson.Gson;

/**
 * This class collects data of a network for the sankey charts.
 * 
 * @author Thomas Hunziker
 *
 */
class SankeyNetworkDataCollector {

	/** We store the id of all node that are either in a target or a source relationship with another node.*/ 
	private Set<String> nodeIds = new HashSet<String>();
	
	private SankeyLinkMap<Counter> links = new SankeyLinkMap<Counter>();
		
	public synchronized void updateEdgeWeights(String source, String destination, long messageCount) {
		this.nodeIds.add(source);
		this.nodeIds.add(destination);
		
		Counter counter = this.links.get(source, destination);
		if (counter == null) {
			counter = new Counter(source, destination);
			this.links.put(source, destination, counter);
		}
		counter.increase(messageCount);
	}
	
	
	public String toJson() {
		Map<String, Object> data = new HashMap<String, Object>();
		
		List<Map<String, String>> nodes = new ArrayList<Map<String, String>>();
		
		for (String nodeId : nodeIds) {
			Map<String, String> name = new HashMap<String, String>();
			name.put("name", nodeId);
			nodes.add(name);
		}
		data.put("nodes", nodes);
		
		List<Map<String, Object>> links = new ArrayList<Map<String, Object>>();
		
		for (Entry<String, Counter> entry : this.links.entrySet()) {
			Counter counter = entry.getValue();
			Map<String, Object> values = new HashMap<String, Object>();
			
			if (!counter.getSource().equals(counter.getDestination())) {
				values.put("source", counter.getSource());
				values.put("target", counter.getDestination());
				values.put("value", counter.getCounter());
				links.add(values);
			}
		}
		data.put("links", links);
		
		Gson gson = new Gson();
		return gson.toJson(data);
	}
	
}
