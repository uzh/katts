/**
 * 
 */
package ch.uzh.ddis.katts.query.processor.aggregate;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.datatype.Duration;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.aggregate.AggregateBolt;
import ch.uzh.ddis.katts.query.processor.AbstractProcessor;

/**
 * This node builds one or multiple aggregates over the stream of incoming n-tuples. It can be configured in windowed or
 * non-windowed mode, it supports grouping over one or multiple fields, and supports a configurable output interval.
 * 
 * <p/>
 * A configuration for the aggregate sums over two fields, over a 5-minute window, grouped by ticker_symvol and
 * ticker_department that fires every minute, but only if the value has changed since it has last been fired, looks as
 * follows:
 * 
 * <pre>
 * &lt;aggregate groupBy="ticker_symbol,ticker_department" windowSize="PT5M" every="PT1M" onlyIfChanged="true"&gt;
 *  &lt;consumes&gt;
 *    &lt;stream streamId="some_incoming_stream"&gt;
 *  &lt;/consumes&gt;
 *  
 *  &lt;aggregators&gt;
 *    &lt;sum of="ticker_bid_price" as="teh_bid_sum"&gt;
 *    &lt;sum of="ticker_ask_price" as="teh_ask_sum"&gt;
 *    &lt;count as="teh_count"&gt;
 *    &lt;expression exp="#teh_bid_sum / #teh_count" as="teh_bid_average"&gt;
 *  &lt;/aggregators&gt;
 * 
 *  &lt;produces&gt;
 *    &lt;stream id="summedTickerStream" inheritFrom="tickerStream" &gt;
 *      &lt;variable type="xs:double" name="bid_sum" referencesTo="teh_bid_sum" /&gt;
 *      &lt;variable type="xs:double" name="ask_sum" referencesTo="teh_ask_sum" /&gt;
 *      &lt;variable type="xs:double" name="count" referencesTo="teh_count" /&gt;
 *      &lt;variable type="xs:double" name="average" referencesTo="teh_bid_average" /&gt;
 *    &lt;/stream&gt;
 *  &lt;/produces&gt;
 * &lt;/aggregate&gt;
 * </pre>
 * 
 * <p/>
 * The aggregate can be configured using one or more aggregators. All aggregators share the same grouping configuration,
 * window size, and firing policy.
 * 
 * <p/>
 * <b>Group By:</b><br/>
 * The "groupBy" value is a comma-separated list of variable names (analogous to the "group by" keyword in SQL). If the
 * "group by" attribte is ommited, one big group is assumed.
 * 
 * <p/>
 * <b>windowSize, every, and onlyIfChanged:</b><br/>
 * The "windowSize" and "every" attributes are duration values specified as defined in the W3C XML Schema 1.0
 * specification for durations. If the "windowSize" attribute is omitted, a window of infinite size
 * ("since the beginning of time") is assumed. Updated values of all the aggregates are propagated using a timer that
 * fires in intervals as specified by the "every" attribute. If the "every" attribute is omitted, updates to the
 * aggregates will be propagated as soon as new values contribute to the aggregate value (i.e. when new tuples arrive).
 * Please note that this does not mean that the aggregate does not need to have changed, in order for it to be emitted.
 * For example, adding 0 to a sum value will leave the sum value unchanged. However, the respective sum value will still
 * be emitted, unless the "onlyIfChanged" attribute is supplied. If the "onlyIfChanged" attribute is omitted its default
 * value ("false") will be used.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
@XmlRootElement(name = "aggregate")
@XmlAccessorType(XmlAccessType.FIELD)
public class AggregateConfiguration extends AbstractProcessor {

	/**
	 * This is analoguous to the "group by" statement of an SQL query. A comma separated list of variable names, over
	 * which the sum should be computed. If this field is missing, the sum will be computed over all incoming messages.
	 */
	@XmlAttribute(required = false)
	private String groupBy;

	/**
	 * The size of the window, over which the sum should be computed. If this attribute is omitted, the window is
	 * considered to be infinite and the sum will be computed "from the beginning of time".
	 * 
	 * The value of this window has to be specified as defined in the W3C XML Schema 1.0 specification for time spans.
	 */
	@XmlAttribute(required = false)
	private Duration windowSize;

	/**
	 * The interval in which the current state of the node should be communicated to the following nodes in the
	 * topology. If this attribute is missing, the interval is considered to be "instant", which means that the node
	 * emits the sums whenever the value changes. This could result in long periods without updates for certain sums.
	 * 
	 * The value of this interval has to be specified as defined in the W3C XML Schema 1.0 specification for time spans.
	 */
	@XmlAttribute(name = "every", required = false)
	private Duration outputInterval;

	/**
	 * If this value is set to true the current values of the aggregates will only be propagated if they have changed.
	 * The default value for this attribute is <b>false</b>.
	 */
	@XmlAttribute(required = false)
	private boolean onlyIfChanged;

	@XmlElementRefs({ @XmlElementRef(type = SumAggregatorConfiguration.class) })
	@XmlElementWrapper(name = "components")
	private List<AggregatorConfiguration<?>> aggregators = new ArrayList<AggregatorConfiguration<?>>();

	@Override
	public Bolt createBoltInstance() {
		return new AggregateBolt(this);
	}

	/**
	 * {@link AggregateConfiguration#groupBy}
	 * 
	 * @return the groupBy
	 */
	public String getGroupBy() {
		return groupBy;
	}

	/**
	 * {@link AggregateConfiguration#groupBy}
	 * 
	 * @param groupBy
	 *            the groupBy to set
	 */
	public void setGroupBy(String groupBy) {
		this.groupBy = groupBy;
	}

	/**
	 * {@link AggregateConfiguration#windowSize}
	 * 
	 * @return the windowSize
	 */
	public Duration getWindowSize() {
		return windowSize;
	}

	/**
	 * {@link AggregateConfiguration#windowSize}
	 * 
	 * @param windowSize
	 *            the windowSize to set
	 */
	public void setWindowSize(Duration windowSize) {
		this.windowSize = windowSize;
	}

	/**
	 * {@link AggregateConfiguration#outputInterval}
	 * 
	 * @return the outputInterval
	 */
	public Duration getOutputInterval() {
		return outputInterval;
	}

	/**
	 * {@link AggregateConfiguration#outputInterval}
	 * 
	 * @param outputInterval
	 *            the outputInterval to set
	 */
	public void setOutputInterval(Duration outputInterval) {
		this.outputInterval = outputInterval;
	}

	/**
	 * {@link AggregateConfiguration#onlyIfChanged}
	 * 
	 * @return the onlyIfChanged
	 */
	public boolean isOnlyIfChanged() {
		return onlyIfChanged;
	}

	/**
	 * {@link AggregateConfiguration#onlyIfChanged}
	 * 
	 * @param onlyIfChanged
	 *            the onlyIfChanged to set
	 */
	public void setOnlyIfChanged(boolean onlyIfChanged) {
		this.onlyIfChanged = onlyIfChanged;
	}

	/**
	 * {@link AggregateConfiguration#aggregators}
	 * 
	 * @return the aggregators
	 */
	public List<AggregatorConfiguration<?>> getAggregators() {
		return aggregators;
	}

	/**
	 * {@link AggregateConfiguration#aggregators}
	 * 
	 * @param aggregators
	 *            the aggregators to set
	 */
	public void setAggregators(List<AggregatorConfiguration<?>> aggregators) {
		this.aggregators = aggregators;
	}

}
