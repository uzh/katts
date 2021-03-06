package ch.uzh.ddis.katts.query;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import ch.uzh.ddis.katts.query.output.FileOutput;
import ch.uzh.ddis.katts.query.output.SystemOutput;
import ch.uzh.ddis.katts.query.processor.Processor;
import ch.uzh.ddis.katts.query.processor.UnionConfiguration;
import ch.uzh.ddis.katts.query.processor.aggregate.AggregateConfiguration;
import ch.uzh.ddis.katts.query.processor.aggregate.Partitioner;
import ch.uzh.ddis.katts.query.processor.aggregate.SumConfiguration;
import ch.uzh.ddis.katts.query.processor.filter.ExpressionFilter;
import ch.uzh.ddis.katts.query.processor.filter.NTupleFilter;
import ch.uzh.ddis.katts.query.processor.filter.TripleFilter;
import ch.uzh.ddis.katts.query.processor.function.ExpressionFunction;
import ch.uzh.ddis.katts.query.processor.join.OneFieldJoin;
import ch.uzh.ddis.katts.query.processor.join.TemporalJoinConfiguration;
import ch.uzh.ddis.katts.query.source.FileGraphPatternReaderConfiguration;
import ch.uzh.ddis.katts.query.source.FileSource;
import ch.uzh.ddis.katts.query.source.NTupleFileSource;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;

/**
 * The query class is the root element of a query structure. It contains a list of nodes. Each node is linked by
 * streams. All the nodes and the query builds the topology configuration. The effective bolts and spouts are
 * implemented in a separated package.
 * 
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement()
public class Query implements Serializable {

	private static final long serialVersionUID = 1L;

	@XmlElementRefs({ @XmlElementRef(type = AggregateConfiguration.class),//
			// @XmlElementRef(type = CachedFileSourceConfiguration.class), //
			@XmlElementRef(type = ExpressionFilter.class),//
			@XmlElementRef(type = ExpressionFunction.class), //
			@XmlElementRef(type = FileGraphPatternReaderConfiguration.class), //
			@XmlElementRef(type = FileSource.class), //
			@XmlElementRef(type = FileOutput.class), //
			@XmlElementRef(type = NTupleFileSource.class), //
			@XmlElementRef(type = NTupleFilter.class), //
			@XmlElementRef(type = SumConfiguration.class), //
			@XmlElementRef(type = SystemOutput.class),//
			@XmlElementRef(type = TemporalJoinConfiguration.class), //
			@XmlElementRef(type = TripleFilter.class), //
			@XmlElementRef(type = OneFieldJoin.class), //
			@XmlElementRef(type = Partitioner.class), //
			@XmlElementRef(type = UnionConfiguration.class), //
	})
	private List<Node> nodes = new ArrayList<Node>();

	@XmlTransient
	private long defaultBufferTimeout = -1;

	private List<Stream> outputStreamsList;

	private List<StreamConsumer> inputStreamsList;

	/**
	 * Determines the output streams.
	 * 
	 * @return
	 */
	public Query validate() {

		final List<Node> nodesList = getNodes();
		final List<Stream> outputStreamsList = new ArrayList<Stream>();
		final List<StreamConsumer> inputStreamsList = new ArrayList<StreamConsumer>();
		// determine input and output nodes
		for (Node n : nodesList) {
			// Input
			if (n instanceof Processor) {
				Processor proc = (Processor) n;
				for (StreamConsumer s : proc.getConsumers()) {
					inputStreamsList.add(s);
				}
			}
			// Output
			if (n instanceof ProducerNode) {
				ProducerNode pn = (ProducerNode) n;
				for (Stream s : pn.getProducers()) {
					if (s.isOutputFlag()) {
						outputStreamsList.add(s);
					}
				}
			}
		}
		setInputStreamsList(inputStreamsList);
		setOutputStreamsList(outputStreamsList);

		// TODO: Implement this method.

		// Check if all the consumers / producers get the right streams
		// Check if there are missing variables on streams
		// Remove not needed variables after each producer (add a filter)

		return this;
	}

	/**
	 * This method tries to optimize the query.
	 * 
	 * @return
	 */
	public Query optimize() {

		// TODO: Add a way to add optimization classes
		// Important optimizations:
		// - Arrangement of the bolts (the one with a higher selectivity first etc.)
		// Potentially this method sets another scheduler for the query depending on the available Bolts / Spouts.

		return this;
	}

	/**
	 * This method returns all nodes in the query.
	 * 
	 * @return
	 */
	@XmlTransient
	public List<Node> getNodes() {
		return this.nodes;
	}

	/**
	 * This method sets list of nodes in the query.
	 * 
	 * @param nodes
	 * @return
	 */
	public Query setNodes(List<Node> nodes) {
		this.nodes = nodes;
		return this;
	}

	/**
	 * This method adds a node to the query.
	 * 
	 * @param node
	 * @return
	 */
	public Query appendNode(Node node) {
		this.getNodes().add(node);
		return this;
	}

	/**
	 * This method builds a string representation of the query. The representation is an xml serialization.
	 * 
	 * @return Xml serialization of the query
	 */
	public String toString() {
		try {
			return getQueryAsString();
		} catch (JAXBException e) {
			throw new RuntimeException("Could not build query.", e);
		}
	}

	// Timeouts are not required anymore, however they are in situation useful, when it is not clear how long buffers
	// should store items...
	// /**
	// * This method returns the timeout for
	// * @return
	// */
	// @XmlAttribute()
	// public long getDefaultBufferTimeout() {
	// return defaultBufferTimeout;
	// }
	//
	// public void setDefaultBufferTimeout(long defaultBufferTimeout) {
	// this.defaultBufferTimeout = defaultBufferTimeout;
	// }

	/**
	 * Alias of toString()
	 * 
	 * @see Query#toString()
	 * 
	 * @return Xml serialization of the query
	 * @throws JAXBException
	 */
	public String getQueryAsString() throws JAXBException {
		JAXBContext jaxbContext = getJAXBContext();

		Marshaller marshaller = jaxbContext.createMarshaller();

		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

		ByteArrayOutputStream os = new ByteArrayOutputStream();
		marshaller.marshal(this, os);

		return os.toString();
	}

	/**
	 * This method loads the query from a XML file. The XML file must be in a valid, in sense of the XSD, and serialized
	 * form.
	 * 
	 * @param path
	 *            Path to the file
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 * @throws JAXBException
	 */
	public static Query createFromFile(String path) throws UnsupportedEncodingException, FileNotFoundException,
			JAXBException {
		return create(new FileInputStream(path));
	}

	/**
	 * This method loads the query from a InputStream.
	 * 
	 * @param xmlStream
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 * @throws JAXBException
	 */
	public static Query create(InputStream xmlStream) throws UnsupportedEncodingException, FileNotFoundException,
			JAXBException {
		JAXBContext jaxbContext = Query.getJAXBContext();
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

		return (Query) unmarshaller.unmarshal(xmlStream);
	}

	/**
	 * This method loads the query from a String.
	 * 
	 * @param xmlAsString
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws FileNotFoundException
	 * @throws JAXBException
	 */
	public static Query create(String xmlAsString) throws UnsupportedEncodingException, FileNotFoundException,
			JAXBException {
		return create(new ByteArrayInputStream(xmlAsString.getBytes("UTF-8")));
	}

	/**
	 * This method returns a JAXB Context for the serialization and deserialization.
	 * 
	 * @return
	 * @throws JAXBException
	 */
	public static JAXBContext getJAXBContext() throws JAXBException {
		return JAXBContext.newInstance("ch.uzh.ddis.katts.query");
	}

	/**
	 * @return the outputStreamsList
	 */
	public List<Stream> getOutputStreamsList() {
		return outputStreamsList;
	}

	/**
	 * @param outputStreamsList
	 *            the outputStreamsList to set
	 */
	public void setOutputStreamsList(List<Stream> outputStreamsList) {
		this.outputStreamsList = outputStreamsList;
	}

	/**
	 * @return the defaultBufferTimeout
	 */
	public long getDefaultBufferTimeout() {
		return defaultBufferTimeout;
	}

	/**
	 * @param defaultBufferTimeout
	 *            the defaultBufferTimeout to set
	 */
	public void setDefaultBufferTimeout(long defaultBufferTimeout) {
		this.defaultBufferTimeout = defaultBufferTimeout;
	}

	public List<StreamConsumer> getInputStreamsList() {
		return this.inputStreamsList;
	}

	public void setInputStreamsList(List<StreamConsumer> inputStreamsList) {
		this.inputStreamsList = inputStreamsList;
	}

}
