package ch.uzh.ddis.katts.evaluation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;

/**
 * This class collects data of a network for the sankey charts.
 * 
 * @author Thomas Hunziker
 *
 */
class SankeyNetworkDataCollector {

	private SankeyLinkMap<Counter> links = new SankeyLinkMap<Counter>();
	private int nodeCounter = 0;
	
	private Map<String, Integer> nodes = new HashMap<String, Integer>();
	
	
	public synchronized void updateEdgeWeights(String source, String destination, long messageCount) {
		this.addNode(source);
		this.addNode(destination);
		
		Counter counter = this.links.get(source, destination);
		if (counter == null) {
			counter = new Counter(source, destination);
			this.links.put(source, destination, counter);
		}
		counter.increase(messageCount);
	}
	
	public synchronized void addNode(String nodeName) {
		if (!this.nodes.containsKey(nodeName)) {
			this.nodes.put(nodeName, this.nodeCounter);
			this.nodeCounter++;
		}
	}
	
	
	public String toJson() {
		Map<String, Object> data = new HashMap<String, Object>();
		
		List<Map<String, String>> nodes = new ArrayList<Map<String, String>>();
		
		Map<Integer, String> nodesInverted = new HashMap<Integer, String>();
		
		for (Entry<String, Integer> entry : this.nodes.entrySet()) {
			nodesInverted.put(entry.getValue(), entry.getKey());
		}
		
		for (int i = 0; i < nodeCounter; i++) {
			Map<String, String> name = new HashMap<String, String>();
			name.put("name", nodesInverted.get((Integer)i));
			nodes.add(name);
		}
		data.put("nodes", nodes);
		
		List<Map<String, Object>> links = new ArrayList<Map<String, Object>>();
		
		for (Entry<String, Counter> entry : this.links.entrySet()) {
			Counter counter = entry.getValue();
			Map<String, Object> values = new HashMap<String, Object>();
			
			if (!counter.getSource().equals(counter.getDestination())) {
				values.put("source", this.nodes.get(counter.getSource()));
				values.put("target", this.nodes.get(counter.getDestination()));
				values.put("value", counter.getCounter());
				links.add(values);
			}
		}
		data.put("links", links);
		
		Gson gson = new Gson();
		return gson.toJson(data);
	}
	
}
