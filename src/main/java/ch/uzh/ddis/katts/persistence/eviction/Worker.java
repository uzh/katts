package ch.uzh.ddis.katts.persistence.eviction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import ch.uzh.ddis.katts.persistence.Storage;
import ch.uzh.ddis.katts.persistence.StorageFactory;
import ch.uzh.ddis.katts.persistence.eviction.strategy.Strategy;

public class Worker implements Runnable{
	
	private List<Strategy> strategies = new ArrayList<Strategy>();

	@Override
	public void run() {
		while(true) {
			for (Strategy strategy : strategies) {
				for(Entry<String, Storage<?, ?>> storageEntry  : StorageFactory.getAllStorages().entrySet()) {
					storageEntry.getValue().runEviction(strategy);
				}
			}
		}
	}

}
