package ch.uzh.ddis.katts.utils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SortedTimeoutBufferTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void waitForInputFromAllChannelsTest() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1", "c2"), // list of channels
				0, // buffer timeout
				0, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { -1, -1, 1 };
		int[] channel1 = { -1, 1, 2 };
		int[] channel2 = { 1, 2, 3 };

		feedToBuffer(buffer, channel0, channel1, channel2);

		Assert.assertEquals(countData(channel0, channel1, channel2), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}

	// TODO lorenz: these time dependent tests are suboptimal. Hypothesis, if the waitTimeout is too low, the
	// flush task does not always get called.
	@Test
	public void minorHickupInOneChannel() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1"), // list of channels
				2, // buffer timeout
				50, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { -1, 1, 2, 3 };
		int[] channel1 = { 1, 3, 2, 4 };

		feedToBuffer(buffer, channel0, channel1);
		Thread.sleep(1000);

		Assert.assertEquals(countData(channel0, channel1), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}

	@Test
	public void mediumHickupInOneChannel() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1"), // list of channels
				10, // buffer timeout
				20, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { -1, 1, 2, 3 };
		int[] channel1 = { 1, 3, 4, 2 };

		feedToBuffer(buffer, channel0, channel1);
		Thread.sleep(40);

		Assert.assertEquals(countData(channel0, channel1), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}

	@Test
	public void majorHickupInOneChannel() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1"), // list of channels
				10, // buffer timeout
				20, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { -1, 1, 2, 3, 4 };
		int[] channel1 = { 1, 3, 4, 5, 2 };

		feedToBuffer(buffer, channel0, channel1);
		Thread.sleep(40);

		Assert.assertEquals(countData(channel0, channel1), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}

	@Test
	public void majorHickupInBothChannel() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1"), // list of channels
				20, // buffer timeout
				40, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { 1, 6, 4, 3, 10 };
		int[] channel1 = { 1, 3, 2, 6, 5 };

		feedToBuffer(buffer, channel0, channel1);
		Thread.sleep(100); // that's the greater of the two timeouts + 4

		Assert.assertEquals(countData(channel0, channel1), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}

	@Test
	public void oneChannelStops4Ever() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1"), // list of channels
				0, // buffer timeout
				10, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { 1, -1, -1, -1 };
		int[] channel1 = { 1, 2, 3, 4 };

		feedToBuffer(buffer, channel0, channel1);
		Thread.sleep(100); // that's the greater of the two timeouts + 1

		Assert.assertEquals(countData(channel0, channel1), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}

	@Test
	public void oneChannelInterrupted() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1"), // list of channels
				0, // buffer timeout
				2, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { 1, -1, -1, -1, 5, 6 };
		int[] channel1 = { 1, 2, 3, 4, 5, 6 };

		feedToBuffer(buffer, channel0, channel1);
		Thread.sleep(2); // that's the greater of the two timeouts + 1

		Assert.assertEquals(countData(channel0, channel1), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}
	
	@Test
	public void hickupsAndInterruptions() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1"), // list of channels
				10, // buffer timeout
				20, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { 1, -1, -1, -1, 5, 6 };
		int[] channel1 = { 1,  6,  5,  4, 3, 7 };

		feedToBuffer(buffer, channel0, channel1);
		Thread.sleep(100); // that's the greater of the two timeouts + 4

		Assert.assertEquals(countData(channel0, channel1), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}
	
	@Test
	public void oneSourceWayAhead() throws InterruptedException {
		ConcurrentLinkedQueue<DummyEvent> resultQueue;
		AbstractFlushQueue.ProcessCallback<DummyEvent> queueCallback;
		SortedTimeoutBuffer<DummyEvent> buffer;

		resultQueue = new ConcurrentLinkedQueue<DummyEvent>();
		queueCallback = createQueueCallback(resultQueue);
		buffer = new SortedTimeoutBuffer<DummyEvent>(Arrays.asList("c0", "c1"), // list of channels
				0, // buffer timeout
				100, // wait timeout
				new DummyEventComparator(), // comparator
				queueCallback);

		// the test data
		int[] channel0 = { 1,  2, 3, 4, 5, 6 };
		int[] channel1 = { 17, 18,  19,  20, 21, 22 };

		feedToBuffer(buffer, channel0, channel1);
		Thread.sleep(200); // that's the greater of the two timeouts + 4

		Assert.assertEquals(countData(channel0, channel1), resultQueue.size());
		assertAscendingOrder(resultQueue);
	}


	// ////////// HELPER METHODS //////////////

	// int[] expectedOrder = {1, 1, 2, 3, 4, 2, 3, 5, 6};
	// assertOrder(expectedOrder, resultQueue);
	// private void assertOrder(int[] expectedOrder, ConcurrentLinkedQueue<DummyEvent> resultQueue) {
	// int i = 0;
	// System.out.print("Testing order of elements: ");
	// for (DummyEvent e : resultQueue) {
	// System.out.print(", " + i);
	// Assert.assertEquals(expectedOrder[i], e.semanticTime);
	// i++;
	// }
	// System.out.println(" all correct!");
	// }

	/**
	 * Counts the elements in the supplied arrays ignoring the "minus one" values. The "minus one" values (-1) represent
	 * "null" and can therefore be ignored.
	 * 
	 * @param channels
	 *            the arrays to count the values of.
	 * @return the number of elements stored in the arrays.
	 */
	private static int countData(int[]... channels) {
		int result = 0;

		for (int[] channel : channels) {
			for (int element : channel) {
				if (element != -1) {
					result++;
				}
			}
		}

		return result;
	}

	private static DummyEvent make(int semanticTime, String id) {
		return new DummyEvent(semanticTime, id);
	}

	private AbstractFlushQueue.ProcessCallback<DummyEvent> createQueueCallback(
			final ConcurrentLinkedQueue<DummyEvent> queue) {
		return new AbstractFlushQueue.ProcessCallback<DummyEvent>() {
			@Override
			public void process(Collection<DummyEvent> elements) {
				for (DummyEvent e : elements) {
					//System.out.println("processing: " + e);
					queue.add(e);
				}
			}
		};
	}

	private static void feedToBuffer(SortedTimeoutBuffer<DummyEvent> buffer, int[]... channels)
			throws InterruptedException {
		int current = 0;
		out: while (true) { // break out if no more items
			boolean processedSomething = false;
			for (int i = 0; i < channels.length; i++) {
				int[] channel = channels[i];
				String channelId = "c" + i;
				if (current < channel.length) {
					int val = channel[current];
					if (val != -1) {
						DummyEvent e = make(channel[current], channelId);
						//System.out.println("adding to buffer: " + e);
						buffer.offer(e, channelId);
					}
					processedSomething = true;
				}
			}
			if (!processedSomething) {
				break out;
			}
			current++;
			Thread.sleep(1);
		}
	}

	/**
	 * Checks if all elements in the queue are in temporal order and calls Assert.fail() if not
	 * 
	 * @param queue
	 *            the queue the content of which is to check.
	 */
	private static void assertAscendingOrder(Queue<DummyEvent> queue) {
		DummyEvent lastProcessedEvent = null;
		long lastProcessedTime = 0;

		while (!queue.isEmpty()) {
			DummyEvent e = queue.poll();
			if (e.semanticTime < lastProcessedTime) {
				Assert.fail(String.format("element out of order: %1s came in after %2s", e, lastProcessedEvent));
			}
			lastProcessedTime = e.semanticTime;
			lastProcessedEvent = e;
		}
	}

	private static class DummyEvent {
		public final int semanticTime;
		public final String id;
		private int hash;

		public DummyEvent(int semanticTime, String id) {
			super();
			this.semanticTime = semanticTime;
			this.id = id;
		}

		@Override
		public String toString() {
			return String.format("semanticTime: %1d, id: %2s", semanticTime, id);
		}

		@Override
		public boolean equals(Object obj) {
			boolean result = false;
			if (obj instanceof DummyEvent) {
				DummyEvent other = (DummyEvent) obj;
				result = this.semanticTime == other.semanticTime && this.id.equals(other.id);
			}
			return result;
		}

		@Override
		public int hashCode() {
			if (hash == 0) { // if an element really has this hash.. s**t happens. this is a test class.
				HashCodeBuilder b = new HashCodeBuilder();
				b.append(this.semanticTime);
				b.append(this.id);
				hash = b.hashCode();
			}
			return hash;
		}

	}

	private static class DummyEventComparator implements Comparator<DummyEvent> {

		@Override
		public int compare(DummyEvent o1, DummyEvent o2) {
			return o1.semanticTime - o2.semanticTime;
		}

	}

}
