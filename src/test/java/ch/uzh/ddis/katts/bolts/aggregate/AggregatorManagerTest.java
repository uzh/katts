package ch.uzh.ddis.katts.bolts.aggregate;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import javax.xml.datatype.DatatypeFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.uzh.ddis.katts.query.processor.aggregate.AggregateConfiguration;
import ch.uzh.ddis.katts.query.processor.aggregate.AggregatorConfiguration;
import ch.uzh.ddis.katts.query.processor.aggregate.SumAggregatorConfiguration;

public class AggregatorManagerTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testOnlyUpdateOnChange() throws Exception {
AggregatorManager manager;
DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
SumAggregatorConfiguration sumConfig;
List<AggregatorConfiguration<?>> aggregatorConfigList;

sumConfig = new SumAggregatorConfiguration();
sumConfig.setOf("price");
sumConfig.setAs("total_price");
aggregatorConfigList = new ArrayList<AggregatorConfiguration<?>>();
aggregatorConfigList.add(sumConfig);

//new AggregateBolt(config);

//manager = new AggregatorManager(
//		datatypeFactory.newDuration("P2D"), // window size
//		datatypeFactory.newDuration("P2D"), // update interval
//		true, // onlyIfChanged
//		sumConfig // aggregator list
//		);
	}

}
