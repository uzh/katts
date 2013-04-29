package ch.uzh.ddis.katts.bolts.source;

import java.util.ArrayList;
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
				for (String pattern : this.configuration.getPatterns()) {
					Bindings currentBindings = tryToBindVariables(pattern, quadruple);

					if (currentBindings != null) { // the quadruple could be bound

						if (currentBindings.isFullyBound()) {
							// if this is the case (most likely not yet), emit it straight away
							if (emitBindings(semanticDate, currentBindings)) {
								emittedAtLeastOneTuple = true;
							}
						} else {
							// add the bindings object to our cache and emit fully bound bindings along the way
							for (String variableName : currentBindings.keySet()) {
								Object variableValue = currentBindings.get(variableName);
								Set<Bindings> bindingsInCache;
								List<Bindings> toAddToCache;

								// find all bindings that have the same variable bound, already
								bindingsInCache = nameValueBindingsIndex.get(variableName).get(variableValue);

								/*
								 * we store bindings that need to be added to the cache in this list to prevent
								 * concurrent modification exceptions.
								 */
								toAddToCache = new ArrayList<Bindings>();

								for (Bindings inCacheBindings : bindingsInCache) {
									Bindings merged = currentBindings.mergeWith(inCacheBindings);
									if (merged != null) {
										if (merged.isFullyBound()) {
											if (emitBindings(semanticDate, merged)) {
												emittedAtLeastOneTuple = true;
											}
										} else { // add to cache for each variable
											toAddToCache.add(merged);
										}
									}
								}

								// store all merged (and not yet fully bound) bindings objects in the index
								for (Bindings bindingsToAdd : toAddToCache) {
									for (String vn : bindingsToAdd.keySet()) { // add it for each variable
										nameValueBindingsIndex.get(vn).put(bindingsToAdd.get(vn), bindingsToAdd);
									}
								}

								// store the unmerged bindings object as it's own "root" bindings object in the index
								nameValueBindingsIndex.get(variableName).put(variableValue, currentBindings);
							}
						}

					}
				}

				if (lastLineRead % 30000 == 0) {
					logger.info(String.format("Read time of component %1s is %2s. Line: %3s",
							this.configuration.getId(), semanticDate.toString(), Long.valueOf(lastLineRead)));
				}
				this.lastLineRead++;

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
			bindings.put(SOURCE_ID_VARIABLE_NAME, getSource().getSourceId());

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
		 * 
		 * @param copyFrom
		 *            the bindings object to copy all the data from.
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
				Object thisValue = get(key); // we check on "this" as we have not yet copied the content to the result
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
