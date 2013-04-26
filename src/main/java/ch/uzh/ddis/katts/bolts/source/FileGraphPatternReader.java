package ch.uzh.ddis.katts.bolts.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.katts.bolts.source.file.Source;
import ch.uzh.ddis.katts.query.source.FileGraphPatternReaderConfiguration;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;
import ch.uzh.ddis.katts.utils.Util;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;

/**
 * This Spout reads in triples from files. By the abstraction of the {@link Source} different type of files can be read
 * with this spout.
 * 
 * All triples that share the same timestamp will be matched against the configured graph pattern. If there is a match
 * all variables from the graph pattern will be emitted on the configured stream.
 * 
 * Multiple files can be configured. There will be one spout instance for each file.
 * 
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 * 
 */
public class FileGraphPatternReader extends AbstractLineReader {

	/**
	 * This variable can be "referenced" to in the variable mapping of the outgoing stream configuration.
	 * <p/>
	 * Example:<br/>
	 * &lt;variable name="source_id" referencesTo="sourceFilePath" /&gt;
	 * 
	 */
	public final static String SOURCE_ID_VARIABLE_NAME = "sourceFilePath";

	private FileGraphPatternReaderConfiguration configuration;

	/** We keep track of how many lines we have read using this variable. */
	private int lastLineRead = 0;

	/** We keep track of what date we processed last in order to know when we have to clear out the cache. */
	private Date lastDateProcessed = new Date(0); // new Date(0), so that we don't have to deal with null values

	/** This list contains all variables that could be found in the defined patterns of the configuration object. */
	private List<String> variableNameList;

	/** This set contains the same names as variableNameList, but with O(1) complexity for the contains() method. */
	private Set<String> variableNameSet;

	/**
	 * As we cannot "peek" at the next triple of the source, we have to load a triple from the source in order to have a
	 * look at it. We store the last read triple in this variable, so we can use this triple instead of reading a fresh
	 * one from the source as the first triple of each batch.
	 */
	private List<String> quadrupleForNextRound;

	/** We need to send this along with the tuples. */
	private long sequenceNumber = 0;

	/**
	 * Creates a new reader instance using the provided configuration.
	 * 
	 * @param configuration
	 *            the configuration object.
	 */
	public FileGraphPatternReader(FileGraphPatternReaderConfiguration configuration) {
		super(configuration);

		Pattern variableNamePattern = Pattern.compile("(^|\\s)(\\?\\w+)($|\\s)");

		this.variableNameSet = new HashSet<String>();
		this.configuration = configuration;
		this.variableNameList = new ArrayList<String>();

		for (String triplePattern : this.configuration.getPatterns()) {
			Matcher m = variableNamePattern.matcher(triplePattern);
			int currentPosition = 0;
			while (m.find(currentPosition)) {
				String variableName = m.group(2);
				if (!this.variableNameSet.contains(variableName)) {
					this.variableNameList.add(variableName);
					this.variableNameSet.add(variableName);
					/*
					 * why so complicated? so that the order of the variable names is the same as the order in which
					 * they are listed in the triple patterns.
					 */
				}
				currentPosition = m.end();
			}
		}
	}

	@Override
	public boolean nextTuple(Source source) {
		boolean result = false;
		Date dateProcessedInThisBatch = null;

		/*
		 * We keep all triples that share the same time value in this cache. Then we search this cache for matches. The
		 * arrays in this
		 */
		List<List<String>> quadrupleCache;

		// clear cache (wasteful but fast)
		quadrupleCache = new ArrayList<List<String>>();

		// read all triples that share the same time
		batch: while (true) {
			List<String> quadruple = null;

			if (quadrupleForNextRound != null) {
				quadruple = this.quadrupleForNextRound;
				this.quadrupleForNextRound = null;
			} else {
				try {
					quadruple = source.getNextTuple();
				} catch (Exception e) {
					throw new RuntimeException(
							String.format("Unable to read next triple because: %1s", e.getMessage()), e);
				}
			}

			if (quadruple != null) {
				Date semanticDate;

				// parse the date field, this supports raw millisecond values and ISO formatted datetime strings
				semanticDate = extractDate(quadruple.get(0));

				if (!this.lastDateProcessed.equals(semanticDate)) {
					// store the current triple for the next batch and stop processing
					this.quadrupleForNextRound = quadruple;
					this.lastDateProcessed = semanticDate;
					result = true; // make sure we come back one more time
					break batch;
				} else {
					dateProcessedInThisBatch = semanticDate;
				}

				// add the quadruple to the current batch
				quadrupleCache.add(quadruple);

				if (lastLineRead % 30000 == 0) {
					logger.info(String.format("Read time of component %1s is %2s. Line: %3s",
							this.configuration.getId(), semanticDate.toString(), Long.valueOf(lastLineRead)));
				}

				this.lastLineRead++;

			} else {
				/*
				 * This line COULD show up twice in the log: The first time, when the current batch ends. Then, because
				 * we still emit all elements from the current batch, the nextTuple method will be called once more, and
				 * we will end up in this place once again. The second time, there won't be any tuples to emit, though.
				 * So we will return with "false" from this method.
				 */
				logger.info(String.format("End of file is reached in component %1s on line: %2s",
						this.configuration.getId(), lastLineRead));
				break batch;
			}

		}

		// emit all tuples of this batch
		for (Bindings bindings : findSubGraphTuples(quadrupleCache)) {
			for (Stream stream : this.configuration.getProducers()) {

				List<Object> tuple = new ArrayList<Object>();
				tuple.add(sequenceNumber++);

				if (dateProcessedInThisBatch == null) {
					throw new IllegalStateException();
				}

				tuple.add(dateProcessedInThisBatch); // startDate
				tuple.add(dateProcessedInThisBatch); // endDate - same as startDate as we process batches of same-time

				// support sourceFilePath as a propery
				bindings.put(SOURCE_ID_VARIABLE_NAME, getSource().getSourceId());

				for (Variable variable : stream.getVariables()) {
					tuple.add(bindings.get(variable.getReferencesTo()));
				}
				// System.out.println("emitting tuple " + bindings);
				emit(stream.getId(), tuple); // We emit on the default stream
				result = true;
			}
		}

		return result;
	}

	/**
	 * This method searches for all subgraphs as defined by the patterns in the configuration objects and returns all
	 * tuples that could be built out of this.
	 * 
	 * @param quadrupleCache
	 *            the list of all quadruples that belong to the current semantic time timestamp.
	 * @return a list containing all subgraph patterns matched as lists of objects.
	 */
	private Collection<Bindings> findSubGraphTuples(List<List<String>> quadrupleCache) {
		Map<String, SetMultimap<Object, Bindings>> nameValueBindingsIndex;
		Set<Bindings> partiallyBoundBindings;
		List<Bindings> fullyBoundBindings;
		String firstPattern;
		List<List<String>> workingCache;

		fullyBoundBindings = new ArrayList<Bindings>();

		/*
		 * A map that maps variable names to their values as well as to all bindings objects that contain this binding.
		 * 
		 * Map<variableName, MultiMap<variableValue, Binding>>
		 * 
		 * We also initialize this map as a "default" map (i.e. a map that generates empty SetMultimap objects if the
		 * multimap is requested for a variableName that does not exist yet.
		 */
		nameValueBindingsIndex = new HashMap<String, SetMultimap<Object, Bindings>>() {
			public SetMultimap<Object, Bindings> get(Object key) {
				SetMultimap<Object, Bindings> result = super.get(key);
				if (result == null) {
					result = HashMultimap.create();
					put(key.toString(), result);
				}
				return result;
			}
		};

		/*
		 * Next to the nameValueBidningsMap-Index, we also need a simple way to iterate over all bindings that are
		 * currenly not yet fully bound. This is what this set is for.
		 */
		partiallyBoundBindings = new HashSet<Bindings>();

		// we generate the initial set of bindings
		firstPattern = this.configuration.getPatterns().get(0);
		workingCache = new ArrayList<List<String>>();
		for (List<String> quadruple : quadrupleCache) {
			Bindings bindings = tryToBindVariables(firstPattern, quadruple);

			if (bindings != null) { // add the bindings object to our cache

				if (bindings.isFullyBound()) {
					// if this is the case (most likely not yet), store it so we can generate the result from it
					// later
					fullyBoundBindings.add(bindings);
				} else {
					for (String variableName : bindings.keySet()) {
						// store it in our database for all matched variables
						nameValueBindingsIndex.get(variableName).put(bindings.get(variableName), bindings);
						partiallyBoundBindings.add(bindings);
					}
				}

			} else { // the pattern did not match, put it into the working cache
				workingCache.add(quadruple); // this will save some work later on
			}
		}

		/*
		 * Now we go over all all remaining triple patterns from top to bottom and bind variables as we find matches
		 */
		for (String pattern : this.configuration.getPatterns().subList(1, this.configuration.getPatterns().size())) {
			List<List<String>> newWorkingCache;

			/*
			 * In each step, we create a new working cache so we don't iterate over quadruples that we have already
			 * successfully processed
			 */
			newWorkingCache = new ArrayList<List<String>>();

			for (List<String> quadruple : workingCache) {
				Bindings bindings = tryToBindVariables(pattern, quadruple);

				if (bindings != null) { // add the bindings object to our cache
					// needed to prevent concurrent modification exception
					Set<Bindings> additionalPartiallyBoundBindings = new HashSet<Bindings>();

					for (Bindings partiallyBound : partiallyBoundBindings) { // check with bindings already in the cache
						Bindings mergedBindings = partiallyBound.mergeWith(bindings);

						if (mergedBindings != null) { // no collisions
							if (mergedBindings.isFullyBound()) {
								// if this is the case (most likely not yet), store it so we can generate the result
								// from it
								// later
								fullyBoundBindings.add(mergedBindings);
							} else {
								for (String variableName : bindings.keySet()) {
									// store it in our database for all matched variables
									nameValueBindingsIndex.get(variableName).put(mergedBindings.get(variableName),
											mergedBindings);
									additionalPartiallyBoundBindings.add(mergedBindings);
								}
							}
						}
					}

					partiallyBoundBindings.addAll(additionalPartiallyBoundBindings);

				} else { // the pattern did not match, put it into the working cache
					newWorkingCache.add(quadruple); // this will save some work later on
				}
			}

			// cache replace the working
			workingCache = newWorkingCache;
		}

		return fullyBoundBindings;
	}

	/**
	 * Tries to match a triple pattern against the contents of the supplied quadruple and returns the variable bindings
	 * that result from this process. If the pattern could not be matched, this method returns null.
	 * 
	 * @param pattern
	 *            the triple pattern that we should try and match.
	 * @param quadruple
	 *            the quadruple holding the information to match against.
	 * @return a map containing the bound variables or null, if no variables could be bound.
	 */
	private Bindings tryToBindVariables(String pattern, List<String> quadruple) {
		Bindings result;
		String[] patternParts;

		result = new Bindings(this.variableNameSet);
		patternParts = pattern.split(" ");

		if (patternParts.length != 3) {
			throw new IllegalStateException("More than three parts in the pattern '" + pattern + "'");
		}

		tryMach: for (int i = 0; i < 3; i++) {
			String currentPart = patternParts[i];
			String quadruplePart = quadruple.get(i + 1); // first element is the time
			if (isVariableName(currentPart)) {
				if (i == 2) {
					// objects (index = 2) could be doubles or dates or whatever...
					result.put(currentPart, Util.convertStringToObject(quadruplePart));
				} else {
					result.put(currentPart, quadruplePart);
				}
			} else {
				if (!currentPart.equals(quadruplePart)) { // did not match!
					result = null;
					break tryMach;
				}
			}
		}

		return result;
	}

	/** pattern used for the {@link #isVariableName(String)} method. */
	private final Pattern namePattern = Pattern.compile("(^|\\s)(\\?\\w+)($|\\s)");

	/**
	 * @return true, if the string has the form of a SPARQL variable (e.g. "?blah"), false otherwise;
	 */
	private boolean isVariableName(String testValue) {
		return namePattern.matcher(testValue).matches();
	}

	/**
	 * Parses the supplied value into a date value. This method supports either ISO date/time values as parseable by
	 * {@link Util#parseDateTime(String)} or long values (as strings).
	 * 
	 * @param dateString
	 *            the string to parse.
	 * @return the converted date value.
	 */
	private Date extractDate(String dateString) {
		Date semanticDate;
		if (dateString.contains("-") || dateString.contains("T") || dateString.contains(":")) {
			semanticDate = Util.parseDateTime(dateString);
		} else {
			semanticDate = new Date(Long.parseLong(dateString));
		}
		return semanticDate;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		for (Stream stream : this.configuration.getProducers()) {
			List<String> fields = new ArrayList<String>();
			fields.add("sequenceNumber");
			fields.add("startDate");
			fields.add("endDate");
			fields.addAll(Variable.getFieldList(stream.getAllVariables()));
			declarer.declareStream(stream.getId(), new Fields(fields));
		}
	}

	/** This class is used to hide some of the complexities involved in merging Maps. */
	private class Bindings extends ForwardingMap<String, Object> {

		/** This is the backing map where all the bindings are actually stored. */
		private HashMap<String, Object> backingMap = new HashMap<String, Object>();

		/** We keep track of how manye unbound variables this bindings object has using this set. */
		private int unboundVariablesCount;

		/**
		 * A set that contains all variables that need to be bound before we can say that all variables have been bound.
		 */
		private Set<String> variablesToBind;

		/**
		 * Creates a new bindings object. The list of variable names will be used to determine if all variables have
		 * been bound in this bindings object.
		 * 
		 * @param variables
		 *            the list of all variable names that have to be bound before this bindings object to have fully
		 *            bound.
		 */
		public Bindings(Set<String> variables) {
			this.variablesToBind = variables;
			this.unboundVariablesCount = variables.size();
		}
		
		/**
		 * A copy constructor that creates a bindings object using the current state of the provided other bindings
		 * object.
		 * @param copyFrom the bindings object to copy all the data from.
		 */
		private Bindings(Bindings copyFrom) {
			this.variablesToBind = copyFrom.variablesToBind;
			this.unboundVariablesCount = copyFrom.unboundVariablesCount;
			// this will call the map's put method and not the one in this class
			this.backingMap.putAll(copyFrom.backingMap);
		}

		/**
		 * Creates a new Bindings object by merging the other map into this one. If there are conflicts (contradicting
		 * bindings for some variables) the return value will be <code>null</code>. values for the same variable
		 * 
		 * @param other
		 *            the other bindings object to merge with.
		 * @return a new Bindings instance with the merged variable bindings or <code>null</code> in case of a
		 *         collision.
		 */
		public Bindings mergeWith(Bindings other) {
			Bindings result = new Bindings(this);

			testForCollisions: for (String key : other.keySet()) {
				Object thisValue = get(key);
				Object otherValue = other.get(key);
				if (thisValue == null) {
					result.put(key, otherValue); // does not exist, yet. Cool!
				} else if (!thisValue.equals(otherValue)) {
					// collision!
					result = null; // signal to the calling code that we had a collision
					break testForCollisions;
				}
			}

			/*
			 * We postponed adding all values of "this" bindings object until now, in order to prevent unnecessary work
			 * in case of a collision.
			 */

			if (result != null) {
				result.putAll(this);
			}

			return result;
		}

		@Override
		public Object put(String key, Object value) {
			/*
			 * If this is one of the variables that we have to bind and it is not already bound we decrease the addition
			 * of this key represents one step towards being fully bound.
			 */
			if (this.variablesToBind.contains(key) && !containsKey(key)) {
				this.unboundVariablesCount -= 1;
			}
			return super.put(key, value);
		}

		/**
		 * @return true if this bindings object is fully bound (i.e. has no more unbound variables), false otherwise.
		 */
		public boolean isFullyBound() {
			return this.unboundVariablesCount == 0;
		}

		@Override
		protected Map<String, Object> delegate() {
			return this.backingMap;
		}

	}

}
