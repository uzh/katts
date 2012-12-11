package ch.uzh.ddis.katts.query.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import ch.uzh.ddis.katts.query.Node;

/**
 * This class represents a list of producing streams ({@link Stream}. It is a wrapper around a ArrayList. This is
 * required because on loading from an XML the nodes must be linked back to the source. This is required to setup the
 * topology.
 * 
 * @author Thomas Hunziker
 * 
 */
public class Producers implements List<Stream>, Serializable {

	private static final long serialVersionUID = 1L;
	private ArrayList<Stream> list = new ArrayList<Stream>();
	private final Node node;

	public Producers(Node node) {
		this.node = node;
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return list.contains(o);
	}

	@Override
	public Iterator<Stream> iterator() {
		return list.iterator();
	}

	@Override
	public Object[] toArray() {
		return list.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return list.toArray(a);
	}

	@Override
	public boolean add(Stream e) {
		e.setNode(this.getNode());
		return list.add(e);
	}

	@Override
	public boolean remove(Object o) {
		return list.remove(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return list.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends Stream> c) {
		for (Stream stream : c) {
			stream.setNode(getNode());
		}
		return list.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends Stream> c) {
		for (Stream stream : c) {
			stream.setNode(getNode());
		}
		return list.addAll(index, c);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return list.removeAll(c);
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return list.retainAll(c);
	}

	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public Stream get(int index) {
		return list.get(index);
	}

	@Override
	public Stream set(int index, Stream element) {
		element.setNode(this.getNode());
		return list.set(index, element);
	}

	@Override
	public void add(int index, Stream element) {
		element.setNode(this.getNode());
		list.add(index, element);
	}

	@Override
	public Stream remove(int index) {
		return list.remove(index);
	}

	@Override
	public int indexOf(Object o) {
		return list.indexOf(o);
	}

	@Override
	public int lastIndexOf(Object o) {
		return list.lastIndexOf(o);
	}

	@Override
	public ListIterator<Stream> listIterator() {
		return list.listIterator();
	}

	@Override
	public ListIterator<Stream> listIterator(int index) {
		return list.listIterator(index);
	}

	@Override
	public List<Stream> subList(int fromIndex, int toIndex) {
		return list.subList(fromIndex, toIndex);
	}

	public Node getNode() {
		return node;
	}

}
