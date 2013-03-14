package ch.uzh.ddis.katts.evaluation;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * This class implements a watcher that waits (blocks the thread) for a certain ZooKeeper is written. 
 * 
 * @author Thomas Hunziker
 *
 */
public class Waiter implements Watcher, Runnable {

	private Object lock = new Object();
	private boolean isCompleted = false;
	private String barrierZkPath = null;
	private String address;

	public Waiter(String address, String barrierPath) throws IOException, KeeperException, InterruptedException {
		barrierZkPath = barrierPath;
		this.address = address;
	}
	
	private void connect() {
		ZooKeeper zooKeeper;
		try {
			zooKeeper = new ZooKeeper(address, 3000, this);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		synchronized (lock) {
			Stat result;
			try {
				result = zooKeeper.exists(barrierZkPath, true);
			} catch (KeeperException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			if (result != null) {
				isCompleted = true;
				lock.notify();
			}
		}
		
	}

	@Override
	public void process(WatchedEvent event) {
		String path = event.getPath();
		if (event.getType() == Event.EventType.None) {
			switch (event.getState()) {
			case Expired:
				connect();
				break;
			}
		} else {
			if (path != null && path.equals(barrierZkPath)) {
				synchronized (lock) {
					isCompleted = true;
					lock.notify();
				}
			}
		}
	}

	@Override
	public void run() {
		connect();
		synchronized (lock) {
			if (!isCompleted) {
				try {
					lock.wait();
				} catch (InterruptedException e) {
					throw new RuntimeException(
							"The thread for waiting for the notification of compleation, was interrupted.", e);
				}
			}
		}

	}
}
