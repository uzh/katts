package ch.uzh.ddis.katts.bolts.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.katts.bolts.source.file.Source;
import ch.uzh.ddis.katts.monitoring.Recorder;
import ch.uzh.ddis.katts.monitoring.TerminationMonitor;
import ch.uzh.ddis.katts.query.source.FileGraphPatternReaderConfiguration;
import ch.uzh.ddis.katts.query.stream.Stream;
import ch.uzh.ddis.katts.query.stream.Variable;
import ch.uzh.ddis.katts.utils.Util;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.HashMultimap;
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
	// private List<String> variableNameList;

	/** This set contains the same names as variableNameList, but with O(1) complexity for the contains() method. */
	// private Set<String> variableNameSet;

	/**
	 * In this variable we count, how many relevant triples we processed using this reader. Relevant triples are triples
	 * that match any of the configured triple patterns.
	 */
	private long relevantTriplesProcessed = 0;

	/**
	 * In this variable we count how many triples we read in using this reader. Here we count all triples that have been
	 * read in, the ones that match a triple pattern and the ones that don't match. However, we don't count the triples
	 * that are being skipped because they were excluded in the configuration using the fromDate or the toDate
	 * configuratino parameter.
	 */
	private long triplesProcessed = 0;

	/*
	 * A map that maps variable names to their values as well as to all bindings objects that contain this binding.
	 * 
	 * Map<variableName, MultiMap<variableValue, Binding>>
	 * 
	 * We also initialize this map as a "default" map (i.e. a map that generates empty SetMultimap objects if the
	 * multimap is requested for a variableName that does not exist yet.
	 */
	Map<String, SetMultimap<Object, Bindings>> nameValueBindingsIndex = new HashMap<String, SetMultimap<Object, Bindings>>() {
		public SetMultimap<Object, Bindings> get(Object key) {
			SetMultimap<Object, Bindings> result = super.get(key);
			if (result == null) {
				result = HashMultimap.create();
				put(key.toString(), result);
			}
			return result;
		}
	};

	/** We need to send this along with the tuples. */
	private long sequenceNumber = 0;

	/**
	 * This array contains all patterns this reader has been configured with. We use an array data structure as we want
	 * to use indices to access it and because we need a very fast data structure.
	 */
	private String[] patterns;

	/**
	 * Creates a new reader instance using the provided configuration.
	 * 
	 * @param configuration
	 *            the configuration object.
	 */
	public FileGraphPatternReader(FileGraphPatternReaderConfiguration configuration) {
		super(configuration);
		List<String> patternList = configuration.getPatterns();
		// Pattern variableNamePattern = Pattern.compile("(^|\\s)(\\?\\w+)($|\\s)");

		// this.variableNameSet = new HashSet<String>();
		this.configuration = configuration;
		// this.variableNameList = new ArrayList<String>();
		this.patterns = new String[patternList.size()];

		for (int i = 0; i < patternList.size(); i++) {
			// String triplePattern = patternList.get(i);
			this.patterns[i] = patternList.get(i);

			// Matcher m = variableNamePattern.matcher(triplePattern);
			// int currentPosition = 0;
			// while (m.find(currentPosition)) {
			// String variableName = m.group(2);
			// if (!this.variableNameSet.contains(variableName)) {
			// this.variableNameList.add(variableName);
			// this.variableNameSet.add(variableName);
			// /*
			// * why so complicated? so that the order of the variable names is the same as the order in which
			// * they are listed in the triple patterns.
			// */
			// }
			// currentPosition = m.end();
			// }

		}

	}

	@Override
	public void open(@SuppressWarnings("rawtypes") final Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		super.open(conf, context, collector);

		// register a callback so we can write the triple counts to zookeeper
		TerminationMonitor.getInstance(conf).registerTerminationCallback(new TerminationMonitor.TerminationCallback() {
			@Override
			public void workerTerminated() {
				String sourceName;

				sourceName = FileGraphPatternReader.this.getSource().getSourceId();
				// parse filename from path leaving the last slash present
				sourceName = sourceName.substring(sourceName.lastIndexOf("/"), sourceName.length());

				logger.info("Writing triple counts for file " + sourceName + " to Zookeeper.");

				try {
					Recorder recorder = Recorder.getInstance(conf);
					recorder.writeToZookeeper(Recorder.TRIPLES_PROCESSED_PATH + sourceName,
							Long.valueOf(FileGraphPatternReader.this.triplesProcessed));
					recorder.writeToZookeeper(Recorder.RELEVANT_TRIPLES_PROCESSED_PATH + sourceName,
							Long.valueOf(FileGraphPatternReader.this.relevantTriplesProcessed));
				} catch (Exception e) {
					logger.error("Could not write triple counts to Zookeeper.", e.getMessage());
				}
			}
		});
	}

	@Override
	public boolean nextTuple(Source source) {
		boolean emittedAtLeastOneTuple = false;
		long skippedLines = 0;

		untilEmit: while (!emittedAtLeastOneTuple) { // read until we have emitted at least one tuple
			List<String> quadruple = null; // the current entry we're working with

			try {
				quadruple = source.getNextTuple();
			} catch (Exception e) {
				throw new RuntimeException(String.format("Unable to read next triple because: %1s", e.getMessage()), e);
			}

			if (quadruple != null) {
				Date semanticDate;
				boolean tripleMatchesAtLeastOnePattern = false;

				// parse the date field, this supports raw millisecond values and ISO formatted datetime strings
				semanticDate = extractDate(quadruple.get(0));

				// make sure the date is within the configured interval
				if (this.configuration.getFromDate() != null && semanticDate.before(this.configuration.getFromDate())) {
					skippedLines++;
					if (skippedLines % (50 * 1000) == 0) {
						logger.debug(String.format("Skipped 1000 lines, now at %1s on line %2d.",
								Util.formatDate(semanticDate), lastLineRead));
					}
					this.lastLineRead++;
					continue untilEmit; // skip this line
				}
				if (this.configuration.getToDate() != null && !semanticDate.before(this.configuration.getToDate())) {
					logger.info(String.format("Current date (%1s) is not before configured toDate (%2s). Stopping"
							+ "reading in component %3s on line: %4s", Util.formatDate(semanticDate),
							Util.formatDate(this.configuration.getToDate()), this.configuration.getId(), lastLineRead));
					break untilEmit; // we're done with this file, break out.
				}

				// make sure we empty the cache if hit a quadruple of another "batch" (i.e. a different date value)
				if (semanticDate.equals(this.lastDateProcessed)) {
					this.nameValueBindingsIndex.clear();
					this.lastDateProcessed = semanticDate;
				}

				// try to bind to all patterns
				for (int patternIndex = 0; patternIndex < this.patterns.length; patternIndex++) {
					Bindings bound = tryToBindVariables(patternIndex, quadruple);

					if (bound != null) { // the quadruple could be bound
						tripleMatchesAtLeastOnePattern = true; // the quadruple matches at least one pattern

						if (bound.isFullyMatched()) {
							// if this is the case (most likely not yet), emit it straight away
							if (emitBindings(semanticDate, bound)) {
								emittedAtLeastOneTuple = true;
							}
						} else {

							Set<Bindings> bindingsToEmit;
							Queue<Bindings> bindingsToMatchWithCache;
							Set<Bindings> guard; // prevent adding the same item to the queue multiple times

							/**
							 * One single new bound bindings object could result in many new variable bindings
							 * combinations. For this reason we cannot just process the one new variable bindings object
							 * we would receive from calling tryToBindVariables(), but we create a queue containing all
							 * the bindings objects that result from the potential chain reaction of one new bindings
							 * object.
							 * 
							 * Example (properties being represented by dashes):
							 * 
							 * 1. Let's say we have the following graphs in the cache: A-B and C-D 2. Now we get a new
							 * triple with B-C. 3. The immediate next graphs generated from this are A-B-C and B-C-D 4.
							 * However we would also expect A-B-C-D
							 * 
							 * So, the process is as follows:
							 * 
							 * 1. Add the current binding to the bindingsToMatchWithCache-queue (and the guard) 2. Work
							 * until queue empty: 2.1 bind with all bindings in cache 2.2 add each new resulting binding
							 * to queue 2.3 add the current bindings to the cache
							 */

							bindingsToEmit = new HashSet<Bindings>();
							bindingsToMatchWithCache = new LinkedList<Bindings>();
							guard = new HashSet<Bindings>();
							bindingsToMatchWithCache.add(bound);

							while (!bindingsToMatchWithCache.isEmpty()) {
								Bindings currentBindings = bindingsToMatchWithCache.poll();

								// add the bindings object to our cache and emit fully bound bindings along the way
								for (String variableName : currentBindings.keySet()) {
									Object variableValue = currentBindings.get(variableName);
									Set<Bindings> bindingsInCache;

									// find all bindings that have the same variable bound, already
									bindingsInCache = this.nameValueBindingsIndex.get(variableName).get(variableValue);

									for (Bindings inCacheBindings : bindingsInCache) {
										Bindings merged = currentBindings.mergeWith(inCacheBindings);
										if (merged != null) {
											if (merged.isFullyMatched()) {
												bindingsToEmit.add(merged);
											} else {
												// add to queue, so we can check this with all bindings that are
												// already in the cache in the next go.
												if (guard.add(merged)) {
													bindingsToMatchWithCache.add(merged);
												}
											}
										}
									}
								}

								// add the current binding to the index for all its variables
								for (String vn : currentBindings.keySet()) { // add it for each variable
									nameValueBindingsIndex.get(vn).put(currentBindings.get(vn), currentBindings);
								}
							}

							// emit all bindings that were fully bound
							for (Bindings toEmit : bindingsToEmit) {
								if (emitBindings(semanticDate, toEmit)) {
									emittedAtLeastOneTuple = true;
								}
							}

						}

					}
				} // loop over patterns

				if (lastLineRead % 30000 == 0) {
					logger.info(String.format("Read time of component %1s is %2s. Line: %3s",
							this.configuration.getId(), semanticDate.toString(), Long.valueOf(lastLineRead)));
				}

				// book keeping
				this.lastLineRead++;
				this.triplesProcessed++;
				if (tripleMatchesAtLeastOnePattern) {
					this.relevantTriplesProcessed++;
				}

			} else {
				logger.info(String.format("End of file is reached in component %1s on line: %2s",
						this.configuration.getId(), lastLineRead));
				break untilEmit;
			}

		} // untilEmit: while (true)

		return emittedAtLeastOneTuple;
	}

	/**
	 * Emits the contents of the supplied bindings object on all configured streams using the respective configured
	 * variable.
	 * 
	 * @param semanticDate
	 *            the start AND the end date of the tuples that are emitted.
	 * @param bindings
	 *            the bindings object.
	 * @return true, if any tuple was emitted (if an outgoing stream was configured), false otherwise.
	 */
	private boolean emitBindings(Date semanticDate, Bindings bindings) {
		Boolean result = false;

		for (Stream stream : this.configuration.getProducers()) {

			List<Object> tuple = new ArrayList<Object>();
			tuple.add(sequenceNumber++);

			if (semanticDate == null) {
				throw new IllegalStateException();
			}

			tuple.add(semanticDate); // startDate
			tuple.add(semanticDate); // endDate - same as startDate as we process batches of same-time

			// support sourceFilePath as a propery
			bindings.bind(SOURCE_ID_VARIABLE_NAME, getSource().getSourceId(), -1);

			for (Variable variable : stream.getVariables()) {
				tuple.add(bindings.get(variable.getReferencesTo()));
			}
			// System.out.println("emitting tuple " + bindings);
			emit(stream.getId(), tuple); // We emit on the default stream
			result = true;
		}

		return result;
	}

	/**
	 * Tries to match a triple pattern against the contents of the supplied quadruple and returns the variable bindings
	 * that result from this process. If the pattern could not be matched, this method returns null.
	 * 
	 * @param patternIndex
	 *            the triple pattern that we should try and match.
	 * @param quadruple
	 *            the quadruple holding the information to match against.
	 * @return a map containing the bound variables or null, if no variables could be bound or there was a collision
	 *         while binding.
	 */
	private Bindings tryToBindVariables(int patternIndex, List<String> quadruple) {
		Bindings result;
		String pattern;
		String[] patternParts;

		result = new Bindings(this.patterns.length);
		pattern = patterns[patternIndex];
		patternParts = pattern.split(" ");

		if (patternParts.length != 3) {
			throw new IllegalStateException("More than three parts in the pattern '" + pattern + "'");
		}

		tryMatch: for (int i = 0; i < 3; i++) {
			String currentPart = patternParts[i];
			String quadruplePart = quadruple.get(i + 1); // first element is the time
			if (isVariableName(currentPart)) {
				if (i == 2) {
					// objects (index = 2) could be doubles or dates or whatever...
					if (result.bind(currentPart, Util.convertStringToObject(quadruplePart), patternIndex) == false) {
						// there was a collision -> abort and return null;
						result = null;
						break tryMatch;
					}
				} else {
					if (result.bind(currentPart, quadruplePart, patternIndex) == false) {
						// there was a collision -> abort and return null;
						result = null;
						break tryMatch;
					}
				}
			} else {
				if (!currentPart.equals(quadruplePart)) { // did not match! -> abort and return null;
					result = null;
					break tryMatch;
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
		private int unmatchedPatternCount;

		/**
		 * This array contains a boolean for each pattern that needs to be matched. Whenever a variables bindings are
		 * added to this bindings object through either the constructor or the merge method that have not yet been
		 * matched according to this array, we decrease the unmatchedPatternCount by one. The {@link #isFullyBound()}
		 * method returns true, when that counter has reached zero. matched patterns using this array.
		 */
		private boolean[] matchedPatterns;

		/**
		 * Creates a new bindings object. The number of patterns parameter will be used to track against how many
		 * pattern this bindings object has already been matched before the {@link #isFullyMatched()} method returns
		 * true.
		 * 
		 * @param numberOfPatternsToMatch
		 *            the number of patterns that have to be matched before this bindigns object is considered to be
		 *            fully matched.
		 */
		public Bindings(int numberOfPatternsToMatch) {
			this.unmatchedPatternCount = numberOfPatternsToMatch;
			this.matchedPatterns = new boolean[this.unmatchedPatternCount];
		}

		/**
		 * Private default constructor, we handle bookkeeping ourselves.
		 */
		private Bindings() {
		}

		/**
		 * Creates a new Bindings object by merging the other map into this one. If there are conflicts (contradicting
		 * bindings for some variables) the return value will be <code>null</code>. values for the same variable
		 * 
		 * @param other
		 *            the other bindings object to merge with.
		 * @return a new Bindings instance with the merged variable bindings or <code>null</code> in case of a collision
		 *         or if nothing new would have emerged from the merge (all keys of one bindings object were already
		 *         present in the other bindings object.
		 */
		public Bindings mergeWith(Bindings other) {
			Bindings result = null;
			boolean anythingNew = false;
			/*
			 * The only requirement to find out if there is anything new to be gained from merging the two bindings is
			 * to compare their respective matchedPatterns arrays.
			 */
			checkForNews: for (int i = 0; i < FileGraphPatternReader.this.patterns.length; i++) {
				if (this.matchedPatterns[i] != other.matchedPatterns[i]) {
					/*
					 * There is at least something new to be gained in merging these two bindings. This does not mean
					 * that there are necessarily new bindings values to be found, but the patterns that were matched to
					 * arrive at the bindings object are different and we need to match ALL patterns before we can emit
					 * a bindings object.
					 */
					anythingNew = true;
					break checkForNews;
				}
			}

			if (anythingNew) {
				result = new Bindings();
				result.unmatchedPatternCount = this.unmatchedPatternCount;
				result.matchedPatterns = Arrays.copyOf(this.matchedPatterns, this.matchedPatterns.length);

				testForCollisions: for (String key : other.keySet()) {
					Object thisValue = get(key); // we check on "this" as we have not yet copied the content to the
													// result
					Object otherValue = other.get(key);
					if (thisValue == null) {
						result.backingMap.put(key, otherValue); // does not exist, yet. Cool!
					} else if (!thisValue.equals(otherValue)) {
						// collision!
						result = null; // signal to the calling code that we had a collision
						break testForCollisions;
					}
				}

				// if there were no collisions
				if (result != null) {
					// merge matchedPatterns arrays
					for (int i = 0; i < FileGraphPatternReader.this.patterns.length; i++) {
						if (!result.matchedPatterns[i] && other.matchedPatterns[i]) {
							result.matchedPatterns[i] = true;
							result.unmatchedPatternCount--;
						}
					}

					/*
					 * We postponed adding all values of "this" bindings object until now, in order to prevent
					 * unnecessary work in case of a collision.
					 */
					result.backingMap.putAll(this);
				}

			}

			return result;
		}

		/**
		 * Binds a variable name to a value. Basically the same as the {@link Map#put(Object, Object)} method with the
		 * difference that for this method the user needs to specify the index of the triple pattern that was matched
		 * when this key-value pair has been generated.
		 * 
		 * @param name
		 *            the name of the variable binding.
		 * @param value
		 *            the value of the variable binding.
		 * @param patternIndex
		 *            the index of the pattern that was matched when generating the variable binding. If this value is
		 *            negative, no pattern index will be checked against.
		 * @return true if no variable with the key has not yet existed in this bindings object or, if it already did
		 *         had the same value assigned as the one provided, false otherwise. A return value of false means that
		 *         there was a collision in the bindings object.
		 */
		public boolean bind(String name, Object value, int patternIndex) {
			Object previousValue = super.put(name, value);

			if (patternIndex >= 0 && this.matchedPatterns[patternIndex] == false) {
				this.matchedPatterns[patternIndex] = true;
				this.unmatchedPatternCount--;
			}

			return previousValue == null || previousValue.equals(value);
		}

		@Override
		public Object put(String key, Object value) {
			throw new RuntimeException("Operation not suported. Use put(String, String, int) method instead.");
		}

		/**
		 * @return true if this bindings object has been fully matched against all bound (i.e. has no more unbound
		 *         variables), false otherwise.
		 */
		public boolean isFullyMatched() {
			return this.unmatchedPatternCount == 0;
		}

		@Override
		protected Map<String, Object> delegate() {
			return this.backingMap;
		}

	}

}
