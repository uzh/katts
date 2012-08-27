package ch.uzh.ddis.katts.persistence;

import java.util.HashMap;
import java.util.Map;

import ch.uzh.ddis.katts.persistence.backend.LocalStorage;


public class StorageFactory {
	
	private static Map<String, Storage<?,?>> storageInstances = new HashMap<String, Storage<?,?>>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public synchronized static <K, V> Storage<K, V> createStorage(Class<? extends Storage> storageClass, String storageKey) throws InstantiationException, IllegalAccessException {	
		Storage storageInstance = storageInstances.get(storageKey);
		if (storageInstance == null) {
			storageInstance = storageClass.newInstance();
			storageInstance.initStorage(storageKey);
			storageInstances.put(storageKey, storageInstance);
		}
		
		return storageInstance;
	}
	
	public static <K, V> Storage<K, V> createDefaultStorage(String storageKey) throws InstantiationException, IllegalAccessException {
		return createStorage(LocalStorage.class, storageKey);
	}
	
	public static Map<String, Storage<?,?>> getAllStorages() {
		return storageInstances;
	}
	
}
