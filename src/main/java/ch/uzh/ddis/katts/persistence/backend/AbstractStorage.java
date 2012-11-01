package ch.uzh.ddis.katts.persistence.backend;

import java.util.ArrayList;
import java.util.List;

import ch.uzh.ddis.katts.persistence.Storage;
import ch.uzh.ddis.katts.persistence.observer.EvictObserver;
import ch.uzh.ddis.katts.persistence.observer.GetObserver;
import ch.uzh.ddis.katts.persistence.observer.Observer;
import ch.uzh.ddis.katts.persistence.observer.PutObserver;
import ch.uzh.ddis.katts.persistence.observer.RemoveObserver;

public abstract class AbstractStorage<K, V> implements Storage<K, V>{
	
//	private List<PutObserver<K, V>> putObservers = new ArrayList<PutObserver<K, V>>();
//	private List<GetObserver<K, V>> getObservers = new ArrayList<GetObserver<K, V>>();
//	private List<RemoveObserver<K, V>> removeObservers = new ArrayList<RemoveObserver<K, V>>();
//	private List<EvictObserver<K, V>> evictObservers = new ArrayList<EvictObserver<K, V>>();
//	
//	public synchronized void addObserver(Observer<K, V> observer) {
//		if (observer instanceof PutObserver) {
//			putObservers.add((PutObserver<K, V>)observer);
//		}
//		else if (observer instanceof GetObserver) {
//			getObservers.add((GetObserver<K, V>)observer);
//		}
//		else if (observer instanceof RemoveObserver) {
//			removeObservers.add((RemoveObserver<K, V>)observer);
//		}
//		else if (observer instanceof EvictObserver) {
//			evictObservers.add((EvictObserver<K, V>)observer);
//		}
//	}
//	
//	public void notifyPut(K key, V value) {
//		for (PutObserver<K, V> observer : this.getPutObservers()) {
//			observer.updateValue(key, value);
//		}
//	}
//	
//	public void notifyGet(K key, V value) {
//		for (GetObserver<K, V> observer : this.getGetObservers()) {
//			observer.retrieveValue(key, value);
//		}
//	}
//	
//	public void notifyRemove(K key) {
//		for (RemoveObserver<K, V> observer : this.getRemoveObservers()) {
//			observer.removeValue(key);
//		}
//	}
//	
//	public void notifyEvict(K key) {
//		for (EvictObserver<K, V> observer : this.getEvictObservers()) {
//			observer.evictValue(key);
//		}
//	}
//
//	public List<PutObserver<K, V>> getPutObservers() {
//		return putObservers;
//	}
//
//	public void setPutObservers(List<PutObserver<K, V>> putObservers) {
//		this.putObservers = putObservers;
//	}
//
//	public List<GetObserver<K, V>> getGetObservers() {
//		return getObservers;
//	}
//
//	public void setGetObservers(List<GetObserver<K, V>> getObservers) {
//		this.getObservers = getObservers;
//	}
//
//	public List<RemoveObserver<K, V>> getRemoveObservers() {
//		return removeObservers;
//	}
//
//	public void setRemoveObservers(List<RemoveObserver<K, V>> removeObservers) {
//		this.removeObservers = removeObservers;
//	}
//
//	public List<EvictObserver<K, V>> getEvictObservers() {
//		return evictObservers;
//	}
//
//	public void setEvictObservers(List<EvictObserver<K, V>> evictObservers) {
//		this.evictObservers = evictObservers;
//	}
	
}
