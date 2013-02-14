/**
 * 
 */
package ch.uzh.ddis.katts.bolts.aggregate;

import java.util.ArrayList;
import java.util.List;

import javax.xml.datatype.DatatypeFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.testing.MockedSources;
import backtype.storm.tuple.Tuple;
import ch.uzh.ddis.katts.bolts.Event;
import ch.uzh.ddis.katts.query.processor.aggregate.AggregateConfiguration;
import ch.uzh.ddis.katts.query.processor.aggregate.AggregatorConfiguration;
import ch.uzh.ddis.katts.query.processor.aggregate.SumAggregatorConfiguration;

/**
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 *
 */
public class AggregateBoltTest {

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testConfig() throws Exception {
		DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
		AggregateConfiguration config;
		SumAggregatorConfiguration sumConfig;
		List<AggregatorConfiguration<?>> aggregatorConfigList;
		
		sumConfig = new SumAggregatorConfiguration();
		sumConfig.setOf("price");
		sumConfig.setAs("total_price");
		aggregatorConfigList = new ArrayList<AggregatorConfiguration<?>>();
		aggregatorConfigList.add(sumConfig);
		
		config = new AggregateConfiguration();
		config.setGroupBy("department");
		config.setWindowSize(datatypeFactory.newDuration("P2D"));
		config.setOutputInterval(datatypeFactory.newDuration("P2D"));
		config.setAggregators(aggregatorConfigList);
		
		new AggregateBolt(config);
	}
		
//	@Test
//	public void testGrouping() throws Exception {
//		AggregateBolt bolt
//		DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
//		AggregateConfiguration config;
//		SumAggregatorConfiguration sumConfig;
//		List<AggregatorConfiguration<?>> aggregatorConfigList;
//		Event event;
//		Tuple tuple;
//		
//		sumConfig = new SumAggregatorConfiguration();
//		sumConfig.setOf("price");
//		sumConfig.setAs("total_price");
//		aggregatorConfigList = new ArrayList<AggregatorConfiguration<?>>();
//		aggregatorConfigList.add(sumConfig);
//		
//		config = new AggregateConfiguration();
//		config.setGroupBy("department");
//		config.setWindowSize(datatypeFactory.newDuration("P2D"));
//		config.setOutputInterval(datatypeFactory.newDuration("P2D"));
//		config.setAggregators(aggregatorConfigList);
//		
//		bolt = new AggregateBolt(config);
//		bolt.prepare(null, null, null);
//		
//		tuple = MockedSources
//		event = new Event(new Tuple(), bolt, null);
//		
//		bolt.execute(event);
//	}

}
