package ch.uzh.ddis.katts.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ElasticPriorityQueueTester {

	private static final int IMPRECISION = 20;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ElasticPriorityQueue<Event> elasticQueue;
		LinkedBlockingQueue<Event> eventQueue;
		long currentTimeOdered = 0; // used to test if the order is being kept
		long countOrdered = 0;
		long currentTimeOriginal = 0;
		long startTime = System.currentTimeMillis();
		Thread t1, t2, t3;

		eventQueue = new LinkedBlockingQueue<ElasticPriorityQueueTester.Event>();
		
		t1 = new Thread(new EventGenerator(eventQueue));
		t2 = new Thread(new EventGenerator(eventQueue));
		t3 = new Thread(new EventGenerator(eventQueue));

		elasticQueue = new ElasticPriorityQueue<ElasticPriorityQueueTester.Event>(IMPRECISION, new HashSet<String>(
				Arrays.asList(t1.getName(), t2.getName(), t3.getName())), new EventComparator());

		t1.start();
		t2.start();
		t3.start();

		System.out.println("GO!");

		// generate a couple of threads that write elements into the concurrent queue at random times

		while (true) {
			try {
				Event event = eventQueue.poll(5, TimeUnit.SECONDS);

				if (event == null) {
					break;
				}

				// debug: just print everything in the order they show up
				// System.out.println(event);
				// if (event.dateTime < currentTimeOriginal) System.out.println("|||||||||||||||||||||||||||||||||");
				// currentTimeOriginal = event.dateTime;

				// ok we have a set of irregularly ordered events and we want to order them without central control...
				for (Event orderedEvent : elasticQueue.offer(event, event.channel)) {
					countOrdered++;
					if (orderedEvent.dateTime < currentTimeOdered) {
						System.out.println("------------ FAIL!");
					}
					currentTimeOdered = orderedEvent.dateTime;
					System.out.println(orderedEvent);
				}

			} catch (InterruptedException e) {
				System.out.println("Interrupted while wainting for new event.");
				break;
			}
		}
		System.out.println("live count: " + countOrdered);

		// print remaining element in queue
		System.out.println("remaining elements:");
		countOrdered = 0;
		for (Event orderedEvent : elasticQueue.drainElements()) {
			if (orderedEvent.dateTime < currentTimeOdered)
				System.out.println("------------ FAIL!");
			currentTimeOdered = orderedEvent.dateTime;
			countOrdered++;
			// System.out.println(orderedEvent);
		}
		System.out.println("remaining count: " + countOrdered);

		System.out.println("done");
		System.out.println("took " + (System.currentTimeMillis() - startTime) + " milliseconds.");
	}

	private static final class EventGenerator implements Runnable {

		private Random generator = new Random();
		private LinkedBlockingQueue<Event> eventQueue;

		public EventGenerator(LinkedBlockingQueue<Event> eventQueue) {
			this.eventQueue = eventQueue;
		}

		@Override
		public void run() {
			long myTime = 0;
			for (int i = 0; i < 4000; i++) {
				this.eventQueue.add(new Event(myTime, Thread.currentThread().getName(), Integer.toString(i)));
				myTime += generator.nextInt(IMPRECISION - 1);
			}
		}
	}

	private static final class Event {
		public final long dateTime;
		public final String channel;
		public final String payload;

		public Event(long dateTime, String channel, String payload) {
			this.dateTime = dateTime;
			this.channel = channel;
			this.payload = payload;
		}

		@Override
		public String toString() {
			return String.format("%1d: %2s - %3s", dateTime, channel, payload);
		}
	}

	private static final class EventComparator implements Comparator<Event> {

		@Override
		public int compare(Event o1, Event o2) {
			if (o1 == null && o2 == null)
				return 0;
			if (o1 == null)
				return -1;
			if (o2 == null)
				return 1;
			if (o1.dateTime - o2.dateTime < 0)
				return -1;
			if (o1.dateTime - o2.dateTime > 0)
				return 1;
			return 0;
		}

	}

}