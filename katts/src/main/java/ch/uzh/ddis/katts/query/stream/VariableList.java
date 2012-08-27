package ch.uzh.ddis.katts.query.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * This class represents a list of variable. It implements a {@link List} of {@link Variable}.
 * It is implemented as a wrapper around an {@link ArrayList} and a {@link HashMap}.
 * The nodes need to have a fast access (Map like access) to the reference value of a variable
 *  and on the other hand the variables must be sorted, this combined implementation is required. 
 * 
 * @author Thomas Hunziker
 *
 */
public class VariableList implements List<Variable>, Serializable {
	
	private static final long serialVersionUID = 1L;
	private HashMap<String, Variable> map = new HashMap<String, Variable>();
	private ArrayList<Variable> list = new ArrayList<Variable>();
	
	public Variable getVariableReferencesTo(String referencesTo) {
		return map.get(referencesTo);
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
	public Iterator<Variable> iterator() {
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
	public boolean add(Variable e) {
		// If the element already exists in the list, then we need to remove
		// it, because in the case the element is already in, then we
		// break map.size() == list.size()
		if (map.containsKey(e.getReferencesTo())) {
			remove(e);
		}
		list.add(e);
		map.put(e.getReferencesTo(), e);
		return true;
	}

	@Override
	public boolean remove(Object o) {
		if (o instanceof Variable) {
			int removeIndex = -1;
			Variable varToRemove = (Variable)o;
			for (int i = 0; i < list.size(); i++) {
				Variable var = list.get(i);
				if (var.getReferencesTo().equals(varToRemove.getReferencesTo())) {
					removeIndex = i;
					break;
				}
			}
			if (removeIndex >= 0) {
				list.remove(removeIndex);
			}
			map.remove(((Variable)o).getReferencesTo());
			return true;
		}
		else {
			throw new IllegalArgumentException("The variable list can handle only objects of type " + Variable.class.getCanonicalName());
		}
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return list.containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends Variable> c) {
		for (Variable var : c) {
			this.add(var);
		}
		return true;
	}

	@Override
	public boolean addAll(int index, Collection<? extends Variable> c) {
		for (Variable var : c) {
			this.remove(var);
			this.map.put(var.getReferencesTo(), var);
		}
		list.addAll(index, c);
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		for (Object var: c) {
			this.remove(var);
		}
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		for (Variable var : this.list) {
			if (!c.contains(var)) {
				this.remove(var);
			}
		}
		return true;
	}

	@Override
	public void clear() {
		this.map.clear();
		this.list.clear();
	}

	@Override
	public Variable get(int index) {
		return list.get(index);
	}

	@Override
	public Variable set(int index, Variable element) {
		list.set(index, element);
		map.put(element.getReferencesTo(), element);
		return element;
	}

	@Override
	public void add(int index, Variable element) {
		this.remove(element);
		list.add(index, element);
		map.put(element.getReferencesTo(), element);
	}

	@Override
	public Variable remove(int index) {
		Variable var = list.get(index);
		list.remove(index);
		map.remove(var.getReferencesTo());
		return var;
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
	public ListIterator<Variable> listIterator() {
		return list.listIterator();
	}

	@Override
	public ListIterator<Variable> listIterator(int index) {
		return list.listIterator(index);
	}

	@Override
	public List<Variable> subList(int fromIndex, int toIndex) {
		VariableList list = new VariableList();
		
		for (Variable var : this.list.subList(fromIndex, toIndex)) {
			list.add(var);
		}
		
		return list;
	}

}
