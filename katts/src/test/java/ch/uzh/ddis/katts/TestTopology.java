package ch.uzh.ddis.katts;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Calendar;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import javax.xml.datatype.DatatypeConstants.Field;

import ch.uzh.ddis.katts.query.Query;
import ch.uzh.ddis.katts.query.output.FileOutput;
import ch.uzh.ddis.katts.query.output.SystemOutput;
import ch.uzh.ddis.katts.query.processor.aggregate.Partitioner;
import ch.uzh.ddis.katts.query.processor.aggregate.component.MaxPartitioner;
import ch.uzh.ddis.katts.query.processor.aggregate.component.MinPartitioner;
import ch.uzh.ddis.katts.query.processor.filter.ExpressionFilter;
import ch.uzh.ddis.katts.query.processor.filter.TripleCondition;
import ch.uzh.ddis.katts.query.processor.filter.TripleFilter;
import ch.uzh.ddis.katts.query.processor.function.ExpressionFunction;
import ch.uzh.ddis.katts.query.processor.join.OneFieldJoin;
import ch.uzh.ddis.katts.query.source.File;
import ch.uzh.ddis.katts.query.source.FileSource;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.StreamConsumer;
import ch.uzh.ddis.katts.query.stream.Variable;
import ch.uzh.ddis.katts.query.stream.grouping.Grouping;
import ch.uzh.ddis.katts.query.stream.grouping.ShuffleGrouping;
import ch.uzh.ddis.katts.query.stream.grouping.VariableGrouping;

public class TestTopology {

	
	
	public static void main(String[] args) throws JAXBException, UnsupportedEncodingException, DatatypeConfigurationException {
		
		// Source
		File file = new File();
		file.setPath("data/triples-sample.csv.zip");
		file.setZipped(true);
		file.setCsvFieldDelimiter(",");
		file.setZipFileEntry("triples-sample.csv");
		
		FileSource source = new FileSource();
		source.appendFile(file);
		
		
		Variable tickerId = new Variable();
		tickerId.setName("ticker_id");
		tickerId.setTypeName("xs:string");
		tickerId.setReferencesTo("subject");
		
		// Triple Price filter
		TripleFilter priceTripleFilter = new TripleFilter();
		priceTripleFilter.setApplyOnSource(source.getId());
		priceTripleFilter.setGroupOn("subject");
		priceTripleFilter.appendCondition(new TripleCondition("predicate", "wc:prc"));
		
		
		Variable tickerPrice = new Variable();
		tickerPrice.setName("ticker_price");
		tickerPrice.setTypeName("xs:double");
		tickerPrice.setReferencesTo("object");
	
		Stream tickerPriceStream = new Stream();
		tickerPriceStream.appendVariable(tickerPrice);
		tickerPriceStream.appendVariable(tickerId);
		tickerPriceStream.setId("tickerPrice");
	
		priceTripleFilter.appendProducer(tickerPriceStream);
		
		// Ticker symbol filter
		TripleFilter symbolTripleFilter = new TripleFilter();
		symbolTripleFilter.setApplyOnSource(source.getId());
		symbolTripleFilter.setGroupOn("subject");
		symbolTripleFilter.appendCondition(new TripleCondition("predicate", "wc:ticker"));
		
		Variable tickerSymbole = new Variable();
		tickerSymbole.setName("ticker_symbol");
		tickerSymbole.setTypeName("xs:string");
		tickerSymbole.setReferencesTo("object");
	
		Stream tickerSymbolStream = new Stream();
		tickerSymbolStream.appendVariable(tickerSymbole);
		tickerSymbolStream.appendVariable(tickerId);
		tickerSymbolStream.setId("tickerSymbol");
	
		symbolTripleFilter.appendProducer(tickerSymbolStream);
		
		// Ticker company filter
		TripleFilter compnameTripleFilter = new TripleFilter();
		compnameTripleFilter.setApplyOnSource(source.getId());
		compnameTripleFilter.setGroupOn("subject");
		compnameTripleFilter.appendCondition(new TripleCondition("predicate", "wc:comnam"));
		
		Variable tickerCompname = new Variable();
		tickerCompname.setName("ticker_compname");
		tickerCompname.setTypeName("xs:string");
		tickerCompname.setReferencesTo("object");
	
		Stream tickerCompnameStream = new Stream();
		tickerCompnameStream.appendVariable(tickerCompname);
		tickerCompnameStream.appendVariable(tickerId);
		tickerCompnameStream.setId("tickerCompanyName");
	
		compnameTripleFilter.appendProducer(tickerCompnameStream);
		
		// Join Filters:
		VariableGrouping tickerIdGrouping = new VariableGrouping();
		tickerIdGrouping.appendGroupOnVariable(tickerId);
		
		StreamConsumer tickerSymbolConsumer = new StreamConsumer();
		tickerSymbolConsumer.setGrouping(tickerIdGrouping);
		tickerSymbolConsumer.setStream(tickerSymbolStream);
		
		StreamConsumer tickerPriceConsumer = new StreamConsumer();
		tickerPriceConsumer.setGrouping(tickerIdGrouping);
		tickerPriceConsumer.setStream(tickerPriceStream);
		
		StreamConsumer tickerCompnameConsumer = new StreamConsumer();
		tickerCompnameConsumer.setGrouping(tickerIdGrouping);
		tickerCompnameConsumer.setStream(tickerCompnameStream);
		
		OneFieldJoin tickerJoin = new OneFieldJoin();
		tickerJoin.appendConsumer(tickerSymbolConsumer);
		tickerJoin.appendConsumer(tickerPriceConsumer);
		tickerJoin.appendConsumer(tickerCompnameConsumer);
		
		tickerJoin.setJoinOn(tickerId);
		
		
		
		Variable tickerCompnameTickerStream = new Variable();
		tickerCompnameTickerStream.setName("ticker_compname");
		tickerCompnameTickerStream.setTypeName("xs:string");
		tickerCompnameTickerStream.setReferencesTo("ticker_compname");

		Variable tickerSymboleTickerStream = new Variable();
		tickerSymboleTickerStream.setName("ticker_symbol");
		tickerSymboleTickerStream.setTypeName("xs:string");
		tickerSymboleTickerStream.setReferencesTo("ticker_symbol");
		
		Variable tickerPriceTickerStream = new Variable();
		tickerPriceTickerStream.setName("ticker_price");
		tickerPriceTickerStream.setTypeName("xs:double");
		tickerPriceTickerStream.setReferencesTo("ticker_price");

		Stream tickerStream = new Stream();
		tickerStream.setId("tickerStream");
		tickerStream.appendVariable(tickerSymboleTickerStream);
		tickerStream.appendVariable(tickerPriceTickerStream);
		tickerStream.appendVariable(tickerCompnameTickerStream);
		
		tickerJoin.appendProducer(tickerStream);
		
		
		// Partitioner
		VariableGrouping tickerSymboleGrouping = new VariableGrouping();
		tickerSymboleGrouping.appendGroupOnVariable(tickerSymboleTickerStream);
		StreamConsumer tickerConsumer = new StreamConsumer();
		tickerConsumer.setGrouping(tickerSymboleGrouping);
		tickerConsumer.setStream(tickerStream);
		
		Partitioner partitioner = new Partitioner();
		partitioner.appendConsumer(tickerConsumer);
		
		partitioner.setSlideSize(DatatypeFactory.newInstance().newDuration("P1D"));
		partitioner.setWindowSize(DatatypeFactory.newInstance().newDuration("P20D"));
		partitioner.setAggregateOn(tickerPriceTickerStream);
		partitioner.setPartitionOn(tickerSymboleTickerStream);
		
		partitioner.appendComponent(new MinPartitioner());
		partitioner.appendComponent(new MaxPartitioner());
		
		Variable minTickerPrice = new Variable();
		minTickerPrice.setName("ticker_min");
		minTickerPrice.setTypeName("xs:double");
		minTickerPrice.setReferencesTo("min");
		
		Variable maxTickerPrice = new Variable();
		maxTickerPrice.setName("ticker_max");
		maxTickerPrice.setTypeName("xs:double");
		maxTickerPrice.setReferencesTo("max");
		
		Stream tickerStreamMinMax = new Stream();
		tickerStreamMinMax.setId("tickerStreamMinMax");
		tickerStreamMinMax.appendVariable(minTickerPrice);
		tickerStreamMinMax.appendVariable(maxTickerPrice);
		tickerStreamMinMax.setInheritFrom(tickerStream);
		
		partitioner.appendProducer(tickerStreamMinMax);
		
		
		// Build factor
		ExpressionFunction fctFunction = new ExpressionFunction();
		fctFunction.setExpression("#ticker_max / #ticker_min");
		StreamConsumer tickerMinMaxConsumer = new StreamConsumer();
		tickerMinMaxConsumer.setGrouping(tickerSymboleGrouping);
		tickerMinMaxConsumer.setStream(tickerStreamMinMax);
		fctFunction.appendConsumer(tickerMinMaxConsumer);
		
		Variable tickerfct = new Variable();
		tickerfct.setName("ticker_fct");
		tickerfct.setTypeName("xs:double");
		tickerfct.setReferencesTo("result");
		
		Stream tickerStreamfct = new Stream();
		tickerStreamfct.setId("tickerStreamfct");
		tickerStreamfct.appendVariable(tickerfct);
		tickerStreamfct.setInheritFrom(tickerStreamMinMax);
		
		fctFunction.appendProducer(tickerStreamfct);
		
		
		
		// Factor filter
		ExpressionFilter filter = new ExpressionFilter();
//		filter.setExpression("#ticker_fct > 3");
		filter.setExpression("true");
		StreamConsumer tickerFctConsumer = new StreamConsumer();
		tickerFctConsumer.setGrouping(tickerSymboleGrouping);
		tickerFctConsumer.setStream(tickerStreamfct);
		filter.appendConsumer(tickerFctConsumer);
		
		
		Stream filteredTickerStream = new Stream();
		filteredTickerStream.setId("filteredTickerStream");
		filteredTickerStream.setInheritFrom(tickerStreamfct);
		filter.appendProducer(filteredTickerStream);
		
		
		
		
//		
//		StreamConsumer consumer = new StreamConsumer();
//		
//		Variable tickerSymbol = new Variable();
//		tickerSymbol.setName("ticker_symbole");
//		tickerSymbol.setTypeName("xs:string");
//		tickerSymbol.setReferencesTo("TICKER");
//		
//		VariableGrouping grouping = new VariableGrouping();
//		grouping.appendGroupOnVariable(tickerSymbol);
//		consumer.setGrouping(grouping);
//		
//		Partitioner min = new Partitioner();
//		min.appendConsumer(consumer);
//		min.setWindowSize(DatatypeFactory.newInstance().newDuration("P30D"));
//		min.setSlideSize(DatatypeFactory.newInstance().newDuration("P3D"));
//		min.setAggregateOn(tickerPrice);
		
//		
//		SystemOutputBolt output = new SystemOutputBolt();
//		
//		StreamConsumer consumer = new StreamConsumer();
//		consumer.setStream(stream);
//		output.appendConsumer(consumer);
		
		StreamConsumer outputConsumer = new StreamConsumer();
		outputConsumer.setStream(filteredTickerStream);
		
		FileOutput output = new FileOutput();
		output.setFilePath("data/output.csv");
		output.appendConsumer(outputConsumer);
		
		
		Query query = new Query();
		query.appendNode(source);
		query.appendNode(priceTripleFilter);
		query.appendNode(symbolTripleFilter);
		query.appendNode(compnameTripleFilter);
		query.appendNode(tickerJoin);
		query.appendNode(partitioner);
		query.appendNode(fctFunction);
		query.appendNode(filter);
		query.appendNode(output);
		
		
		
		System.out.println(query.getQueryAsString());
		
	}
	
}
