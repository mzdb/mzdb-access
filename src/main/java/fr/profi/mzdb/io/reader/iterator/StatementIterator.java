package fr.profi.mzdb.io.reader.iterator;

import java.util.Iterator;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.MzDbReader;

public abstract class StatementIterator<E> implements Iterator<E>, IStatementExtractor<E> {

	protected final MzDbReader mzDbReader;
	protected final SQLiteStatement statement;
	protected boolean isStatementClosed = false;
	protected E nextElem = null;

	public boolean isStatementClosed() {
		return isStatementClosed;
	}

	public StatementIterator(MzDbReader mzdb, SQLiteStatement stmt) throws SQLiteException {
		super();
		this.mzDbReader = mzdb;
		this.statement = stmt;

		nextElem = null;
	}

	public void closeStatement() {
		if (!isStatementClosed) {
			if (statement != null)
				statement.dispose();
			isStatementClosed = true;
		}
	}

	public boolean hasNext() {
		try {
			if (statement.step()) {
				return true;
			} else {
				this.closeStatement();
				return false;
			}

		} catch (SQLiteException e) {
			e.printStackTrace();
			return false;
		}
	}

	public E next() {

		try {
			this.nextElem = this.extractObject(statement);
			return nextElem;
		} catch (Exception e) {
			e.printStackTrace();
			// this.nextElem = null;
			// don't throw exception => we have a problem with the statement which is
			// closing automatically
			// TODO: find a safe way to check if the statement has been closed
			// rethrow(e);
			return null;// obj;
		}
	}

	public void remove() {
		throw new UnsupportedOperationException("Unsupported Operation");
	}

	protected void rethrow(SQLiteException e) {
		throw new RuntimeException(e.getMessage());
	}

}
