package ch.uzh.ddis.katts.bolts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.uzh.ddis.katts.query.stream.Stream;

public class Emitter {
	
	private Bolt sourceBolt = null;
	private Map<Stream, Long> counters = new HashMap<Stream, Long>();
	
	public Emitter(Bolt sourceBolt) {
		this.sourceBolt = sourceBolt;
	}
	
	public VariableBindings createVariableBindings(Stream stream, Event anchorEvent) {
		if (counters.get(stream) == null) {
			counters.put(stream, 0L);
		}
		return new VariableBindings(stream, this, anchorEvent);
	}

	public Bolt getSourceBolt() {
		return sourceBolt;
	}
	
	public synchronized void emit(VariableBindings variableBinding) {
		
		long sequenceNumber = counters.get(variableBinding.getStream());
		List<Object> tuple = variableBinding.getDataListSorted(sequenceNumber);
		String streamId = variableBinding.getStream().getId();
		this.getSourceBolt().getCollector().emit(streamId, variableBinding.getAnchorEvent().getTuple(), tuple);
		
		sequenceNumber++;
		counters.put(variableBinding.getStream(), sequenceNumber);
	}
	
}
