package fr.profi.mzdb.utils.iterator;

import java.util.Iterator;

public class NullIterator implements Iterator<Object> {

	// @Override
	public boolean hasNext() {
		return false;
	}

	// @Override
	public Object next() {
		return null;
	}

	// @Override
	public void remove() {

	}

}