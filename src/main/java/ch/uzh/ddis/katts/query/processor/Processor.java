package ch.uzh.ddis.katts.query.processor;

import ch.uzh.ddis.katts.query.ConsumerNode;
import ch.uzh.ddis.katts.query.ProducerNode;

/**
 * This interface represents nodes that process some data. A processor consumes and emits always data.
 * 
 * 
 * @author Thomas Hunziker
 * 
 */
public interface Processor extends ConsumerNode, ProducerNode {

	/**
	 * The paralelismweight indicates how many instances should be created for a certain bolt comparing to the other
	 * components in the cluster. This can be used to balance the required processing power between bolts.
	 * 
	 * @return
	 */
	public float getParallelismWeight();
}
