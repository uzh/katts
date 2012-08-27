package ch.uzh.ddis.katts.bolts;

import java.util.Comparator;

public class EventTimeComparator implements Comparator<Event> {

	@Override
	public int compare(Event event1, Event event2) {
		return event1.getStartDate().compareTo(event2.getStartDate());
	}

}
