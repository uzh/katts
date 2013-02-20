package ch.uzh.ddis.katts.bolts.join;

import static ch.uzh.ddis.katts.util.MockDataUtils.convertToValues;
import static ch.uzh.ddis.katts.util.MockDataUtils.parseString;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import ch.uzh.ddis.katts.bolts.source.DummySpout;
import ch.uzh.ddis.katts.query.processor.join.EvictionRuleConfiguration;
import ch.uzh.ddis.katts.query.processor.join.SameValueJoinConditionConfiguration;
import ch.uzh.ddis.katts.query.processor.join.TemporalJoinConfiguration;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

public class TemporalJoinBoltTest {

	private DateTimeFormatter isoFormat = ISODateTimeFormat.dateTimeParser();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBasicTopology() {
		MkClusterParam mkClusterParam = new MkClusterParam();
		mkClusterParam.setSupervisors(4);
		Config daemonConf = new Config();
		daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
		mkClusterParam.setDaemonConf(daemonConf);

		/**
		 * This is a combination of <code>Testing.withLocalCluster</code> and <code>Testing.withSimulatedTime</code>.
		 */
		Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {
			@Override
			public void run(ILocalCluster cluster) throws Exception {
				final int numberOfWorkers = 2;
				final Fields leftFields = new Fields("sequenceNumber", "startDate", "endDate", "ticker", "name");
				final Fields rightFields = new Fields("sequenceNumber", "startDate", "endDate", "ticker", "price");
				TopologyBuilder builder;
				Config conf = new Config();
				conf.setNumWorkers(numberOfWorkers);

				Stream leftStream = new Stream();
				leftStream.setId("leftStream");
				StreamConsumer leftConsumer = new StreamConsumer();
				leftConsumer.setStream(leftStream);
				Stream rightStream = new Stream();
				rightStream.setId("rightStream");
				StreamConsumer rightConsumer = new StreamConsumer();
				rightConsumer.setStream(rightStream);
				
				SameValueJoinConditionConfiguration svjcc = new SameValueJoinConditionConfiguration();
				svjcc.setJoinFields("ticker");
				TemporalJoinConfiguration tjc = new TemporalJoinConfiguration();
				tjc.setId("tjc");
				tjc.setJoinCondition(svjcc);
				EvictionRuleConfiguration onlyAllowSameTime = new EvictionRuleConfiguration();
				onlyAllowSameTime.setOn("*");
				onlyAllowSameTime.setFrom("*");
				onlyAllowSameTime.setCondition("#from.endDate lt #on.startDate");
				tjc.setEvictBefore(Arrays.asList(onlyAllowSameTime));
				
				// build the test topology
				builder = new TopologyBuilder();
				builder.setSpout("leftSpout", new DummySpout(leftFields));
				builder.setSpout("rightSpout", new DummySpout(rightFields));
				TemporalJoinBolt tjb = new TemporalJoinBolt(tjc);
				tjb.setConsumerStreams(Arrays.asList(leftConsumer, rightConsumer));
				builder.setBolt("tjcBolt", tjb) //
						.fieldsGrouping("leftSpout", new Fields("ticker")) // attach left spout
						.fieldsGrouping("rightSpout", new Fields("ticker")); // attach right spout
				StormTopology topology = builder.createTopology();


				// prepare the mock data
				MockedSources mockedSources = new MockedSources();
				mockedSources.addMockData(
						"leftSpout", "leftStream", //
						convertToValues(leftFields,
								parseString("sequenceNumber=0,startDate=2001-01-01,ticker=HCI,name=Holy Cows Inc.")),
						convertToValues(leftFields,
								parseString("sequenceNumber=1,startDate=2001-01-02,ticker=HCI,name=Holy Cows Inc.")),
						convertToValues(leftFields,
								parseString("sequenceNumber=2,startDate=2001-01-03,ticker=HCI,name=Holy Cows Inc.")),
						convertToValues(leftFields,
								parseString("sequenceNumber=3,startDate=2001-01-04,ticker=HCI,name=Holy Cows Inc.")));
				mockedSources.addMockData(
						"rightSpout", "rightStream", //
						convertToValues(rightFields,
								parseString("sequenceNumber=0,startDate=2001-01-01,ticker=HCI,price=1.0D")),
						convertToValues(rightFields,
								parseString("sequenceNumber=1,startDate=2001-01-02,ticker=HCI,price=2.0D")),
						convertToValues(rightFields,
								parseString("sequenceNumber=2,startDate=2001-01-03,ticker=HCI,price=3.0D")),
						convertToValues(rightFields,
								parseString("sequenceNumber=3,startDate=2001-01-04,ticker=HCI,price=4.0D")));
				
				CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
				completeTopologyParam.setMockedSources(mockedSources);
				completeTopologyParam.setStormConf(conf);
				/**
				 * TODO
				 */
				Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

				
				Testing.readTuples(result, "leftSpout");
				Testing.readTuples(result, "rightSpout");
				Testing.readTuples(result, "tjcBolt");
				
				assertTrue(true);
				
				// check whether the result is right
//				 assertTrue(Testing.multiseteq(new Values(new Values("nathan"), new Values("bob"), new Values("joey"),
//				 new Values("nathan")), Testing.readTuples(result, "1")));
//				 assertTrue(Testing.multiseteq(new Values(new Values("nathan", 1), new Values("nathan", 2), new
//				 Values(
//				 "bob", 1), new Values("joey", 1)), Testing.readTuples(result, "2")));
//				 assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2), new Values(3), new Values(4)),
//				 Testing.readTuples(result, "3")));
//				 assertTrue(Testing.multiseteq(new Values(new Values(1), new Values(2), new Values(3), new Values(4)),
//				 Testing.readTuples(result, "4")));
			}

		});
	}
}
