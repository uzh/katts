package ch.uzh.ddis.katts.bolts;

import java.util.Comparator;

/**
 * This comparator is used to compare different Events depending on their Event time.
 * 
 * @author Thomas Hunziker
 * 
 */
public class EventTimeComparator implements Comparator<Event> {

	@Override
	public int compare(Event event1, Event event2) {
		return event1.getStartDate().compareTo(event2.getStartDate());
	}

}
