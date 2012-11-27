package ch.uzh.ddis.katts.query.processor;

import ch.uzh.ddis.katts.query.ConsumerNode;
import ch.uzh.ddis.katts.query.ProducerNode;

/**
 * This interface represents nodes that process some data. A processor
 * consumes and emits always data.
 * 
 * 
 * @author Thomas Hunziker
 *
 */
public interface Processor extends ConsumerNode, ProducerNode {

	float getParallelismWeight();
}
