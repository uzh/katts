package ch.uzh.ddis.katts.bolts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ch.uzh.ddis.katts.query.stream.Stream;

/**
 * The emitter is used to emit an event to the corresponding streams. It builds the adapter to the storm emiting
 * infrastructure.
 * 
 * @author Thomas Hunziker
 * 
 */
public class Emitter {

	private Bolt sourceBolt = null;
	private Map<Stream, Long> counters = new HashMap<Stream, Long>();

	/**
	 * Constructor of the emitter.
	 * 
	 * @param sourceBolt
	 *            The bolt from which the event should be sent from.
	 */
	public Emitter(Bolt sourceBolt) {
		this.sourceBolt = sourceBolt;
	}

	/**
	 * This method returns a new {@link VariableBindings} based on the given stream and the anchor event.
	 * 
	 * @param stream
	 *            The stream on which this VariableBindings should be emitted on.
	 * @param anchorEvent
	 *            The event, which produces this VariableBindings. This is required by Storm for reliability.
	 * @return A new variable binding as configured.
	 */
	public VariableBindings createVariableBindings(Stream stream, Event anchorEvent) {
		if (counters.get(stream) == null) {
			counters.put(stream, 0L);
		}
		return new VariableBindings(stream, this, anchorEvent);
	}

	/**
	 * Returns the source bolt for which this emitter was created for.
	 * 
	 * @return
	 */
	public Bolt getSourceBolt() {
		return sourceBolt;
	}

	/**
	 * This method emits the given variable binding on the defined stream
	 * 
	 * @param variableBinding
	 *            The variable binding to emit.
	 */
	public synchronized void emit(VariableBindings variableBinding) {

		long sequenceNumber = counters.get(variableBinding.getStream());
		List<Object> tuple = variableBinding.getDataListSorted(sequenceNumber);
		String streamId = variableBinding.getStream().getId();
		// TODO lorenz: figure out this achor stuff and remove this null-hack
		// this.getSourceBolt().emit(streamId, variableBinding.getAnchorEvent().getTuple(), tuple);
		this.getSourceBolt().emit(streamId, null, tuple);

		sequenceNumber++;
		counters.put(variableBinding.getStream(), sequenceNumber);
	}

}
