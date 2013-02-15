package ch.uzh.ddis.katts.bolts.aggregate;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import javax.xml.datatype.DatatypeFactory;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ch.uzh.ddis.katts.bolts.aggregate.AggregatorManager.Callback;
import ch.uzh.ddis.katts.bolts.join.SimpleVariableBindings;
import ch.uzh.ddis.katts.query.processor.aggregate.AggregatorConfiguration;
import ch.uzh.ddis.katts.query.processor.aggregate.SumAggregatorConfiguration;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

public class AggregatorManagerTest {

	private DateTimeFormatter isoFormat = ISODateTimeFormat.dateTimeParser();

	/**
	 * Parses a comma separated list of key-value pairs and writes them into a new instance of SimpleVariableBindings.
	 * 
	 * The two fields "startDate" and "endDate" need to be formatted in the ISO date format as specified by
	 * {@link DateTimeFormatter}. If only startDate is provided, the same value will be used for the "endDate" field as
	 * well.
	 * 
	 * @param stringToParse
	 *            the string in the form "key1=value1,key2=value2". This implementation does not support escaping.
	 * @throws ParseException
	 *             if stringToParse could not be parsed.
	 */
	private SimpleVariableBindings parseString(String stringToParse) throws ParseException {

		Pattern doublePattern = Pattern.compile("[0-9]*.[0-9]*D");
		SimpleVariableBindings result = new SimpleVariableBindings();

		for (String keyValuePair : stringToParse.split(",")) {
			String[] kvArr = keyValuePair.split("=");
			try {
				String key = kvArr[0];
				String value = kvArr[1];
				if (SimpleVariableBindings.FIELD_STARTDATE.equals(key)
						|| SimpleVariableBindings.FIELD_ENDDATE.equals(key)) {
					result.put(key, isoFormat.parseDateTime(value).toDate());
				} else {
					if (doublePattern.matcher(value).matches()) {
						result.put(key, Double.valueOf(value));
					} else {
						result.put(key, value);
					}
				}

			} catch (ArrayIndexOutOfBoundsException e) {
				throw new ParseException("Could not parse string " + keyValuePair, 0);
			}
		}

		if (result.containsKey(SimpleVariableBindings.FIELD_STARTDATE)
				&& !result.containsKey(SimpleVariableBindings.FIELD_ENDDATE)) {
			result.put(SimpleVariableBindings.FIELD_ENDDATE, result.get(SimpleVariableBindings.FIELD_STARTDATE));
		}

		return result;
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testSimpleSum() throws Exception {
		AggregatorManager manager;
		AggregatorManager.Callback callback;
		DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
		SumAggregatorConfiguration sumConfig;
		List<AggregatorConfiguration<?>> aggregatorConfigList;
		ImmutableList<String> groupByKey;
		final Table<ImmutableList<Object>, String, Object> resultTable = HashBasedTable.create();

		sumConfig = new SumAggregatorConfiguration();
		sumConfig.setOf("price");
		sumConfig.setAs("total_price");
		aggregatorConfigList = new ArrayList<AggregatorConfiguration<?>>();
		aggregatorConfigList.add(sumConfig);

		callback = new Callback() {

			@Override
			public void callback(Table<ImmutableList<Object>, String, Object> aggregateValues, Date startDate,
					Date endDate) {
				synchronized (resultTable) {
					resultTable.putAll(aggregateValues);
				}
			}
		};

		manager = new AggregatorManager(datatypeFactory.newDuration("PT1S"), // window size
				datatypeFactory.newDuration("PT1S"), // update interval
				callback, // this method will be called when there are
				true, // onlyIfChanged
				sumConfig // aggregator list
		);

		groupByKey = ImmutableList.of("HHH", "sales");
		manager.incorporateValue(groupByKey,
				parseString("startDate=2001-01-01T00:00:01,ticker=HHH,department=sales,price=3.0D"));
		manager.incorporateValue(groupByKey,
				parseString("startDate=2001-01-01T00:00:02,ticker=HHH,department=sales,price=4.0D"));
		// We should have gotten the sum for HHH-sales during the first second back (written in the result table)
		synchronized (resultTable) {
			Assert.assertEquals(3.0D, ((Double) resultTable.get(groupByKey, "total_price")).doubleValue(), 0.01D);
		}
	}

	@Test
	public void testOnlyUpdateOnChangeTrue() throws Exception {
		AggregatorManager manager;
		AggregatorManager.Callback callback;
		DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
		SumAggregatorConfiguration sumConfig;
		List<AggregatorConfiguration<?>> aggregatorConfigList;
		final ImmutableList<String> groupByKey;
		final Table<ImmutableList<Object>, String, Object> resultTable = HashBasedTable.create();
		final Date[] lastUpdateDates = new Date[2]; // start- and end date
		Date lastUpdate;

		sumConfig = new SumAggregatorConfiguration();
		sumConfig.setOf("price");
		sumConfig.setAs("total_price");
		aggregatorConfigList = new ArrayList<AggregatorConfiguration<?>>();
		aggregatorConfigList.add(sumConfig);

		groupByKey = ImmutableList.of("HHH", "sales");

		callback = new Callback() {

			@Override
			public void callback(Table<ImmutableList<Object>, String, Object> aggregateValues, Date startDate,
					Date endDate) {
				synchronized (resultTable) {
					if (aggregateValues.contains(groupByKey, "total_price")) {
						resultTable.putAll(aggregateValues);
						lastUpdateDates[0] = startDate;
						lastUpdateDates[1] = endDate;
					}
				}
			}
		};

		manager = new AggregatorManager(datatypeFactory.newDuration("PT1S"), // window size
				datatypeFactory.newDuration("PT1S"), // update interval
				callback, // this method will be called when there are
				true, // onlyIfChanged
				sumConfig // aggregator list
		);

		manager.incorporateValue(groupByKey,
				parseString("startDate=2001-01-01T00:00:01,ticker=HHH,department=sales,price=3.0D"));
		manager.advanceInTime(isoFormat.parseMillis("2001-01-01T00:00:02"));
		// We should have gotten the sum for HHH-sales during the first second back (written in the result table)
		synchronized (resultTable) {
			Assert.assertEquals(3.0D, ((Double) resultTable.get(groupByKey, "total_price")).doubleValue(), 0.01D);
			lastUpdate = lastUpdateDates[1];
		}
		// Add 3.0 to the sum. since we're only looking at a 1 second window, this did not change the value
		// of the sum (it is still 3), so this should not trigger an update -> the lastUpdate should not have changed.
		manager.incorporateValue(groupByKey,
				parseString("startDate=2001-01-01T00:00:02,ticker=HHH,department=sales,price=3.0D"));
		manager.advanceInTime(isoFormat.parseMillis("2001-01-01T00:00:03"));
		synchronized (resultTable) {
			Assert.assertEquals(3.0D, ((Double) resultTable.get(groupByKey, "total_price")).doubleValue(), 0.01D);
			Assert.assertEquals(lastUpdate, lastUpdateDates[1]);
		}
	}

	@Test
	public void testOnlyUpdateOnChangeFalse() throws Exception {
		AggregatorManager manager;
		AggregatorManager.Callback callback;
		DatatypeFactory datatypeFactory = DatatypeFactory.newInstance();
		SumAggregatorConfiguration sumConfig;
		List<AggregatorConfiguration<?>> aggregatorConfigList;
		final ImmutableList<String> groupByKey;
		final Table<ImmutableList<Object>, String, Object> resultTable = HashBasedTable.create();
		final Date[] lastUpdateDates = new Date[2]; // start- and end date
		Date lastUpdate;

		sumConfig = new SumAggregatorConfiguration();
		sumConfig.setOf("price");
		sumConfig.setAs("total_price");
		aggregatorConfigList = new ArrayList<AggregatorConfiguration<?>>();
		aggregatorConfigList.add(sumConfig);

		groupByKey = ImmutableList.of("HHH", "sales");

		callback = new Callback() {

			@Override
			public void callback(Table<ImmutableList<Object>, String, Object> aggregateValues, Date startDate,
					Date endDate) {
				synchronized (resultTable) {
					if (aggregateValues.contains(groupByKey, "total_price")) {
						resultTable.putAll(aggregateValues);
						lastUpdateDates[0] = startDate;
						lastUpdateDates[1] = endDate;
					}
				}
			}
		};

		manager = new AggregatorManager(datatypeFactory.newDuration("PT1S"), // window size
				datatypeFactory.newDuration("PT1S"), // update interval
				callback, // this method will be called when there are
				false, // onlyIfChanged
				sumConfig // aggregator list
		);

		manager.incorporateValue(groupByKey,
				parseString("startDate=2001-01-01T00:00:01,ticker=HHH,department=sales,price=3.0D"));
		manager.advanceInTime(isoFormat.parseMillis("2001-01-01T00:00:02"));
		// We should have gotten the sum for HHH-sales during the first second back (written in the result table)
		synchronized (resultTable) {
			Assert.assertEquals(3.0D, ((Double) resultTable.get(groupByKey, "total_price")).doubleValue(), 0.01D);
			lastUpdate = lastUpdateDates[1];
		}
		// Add 3.0 to the sum. since we're only looking at a 1 second window, this did not change the value
		// of the sum (it is still 3), so this should not trigger an update -> the lastUpdate should not have changed.
		manager.incorporateValue(groupByKey,
				parseString("startDate=2001-01-01T00:00:02,ticker=HHH,department=sales,price=3.0D"));
		manager.advanceInTime(isoFormat.parseMillis("2001-01-01T00:00:03"));
		synchronized (resultTable) {
			Assert.assertEquals(3.0D, ((Double) resultTable.get(groupByKey, "total_price")).doubleValue(), 0.01D);
			Assert.assertFalse(lastUpdate.equals(lastUpdateDates[1]));
		}
	}
}
