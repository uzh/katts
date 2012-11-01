package ch.uzh.ddis.katts;

import ch.uzh.ddis.katts.persistence.Storage;
import ch.uzh.ddis.katts.persistence.StorageFactory;
import ch.uzh.ddis.katts.persistence.backend.LocalStorage;

public class CacheTest {

	public static void main(String[] args) throws InstantiationException, IllegalAccessException {
		
		Storage<String, String> storage = StorageFactory.createStorage(LocalStorage.class, "Test");
		
		storage.put("Test", "Test");
		
		
		System.out.println(storage.get("Test"));
		
	}
	
}