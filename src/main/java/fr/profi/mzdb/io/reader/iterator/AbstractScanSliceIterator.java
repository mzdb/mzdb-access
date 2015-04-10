package fr.profi.mzdb.io.reader.iterator;

import java.io.StreamCorruptedException;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.utils.sqlite.ISQLiteStatementConsumer;

public abstract class AbstractScanSliceIterator {

	protected final MzDbReader mzDbReader;
	protected final SQLiteStatement statement;
	protected final BoundingBoxIterator boundingBoxIterator;
	protected BoundingBox firstBB;
	protected final int msLevel;
	
	public AbstractScanSliceIterator(MzDbReader mzDbReader, String sqlQuery, int msLevel, ISQLiteStatementConsumer stmtBinder ) 
			throws SQLiteException, StreamCorruptedException {
		
		// Retrieve SQLite connection
		SQLiteConnection conn = mzDbReader.getConnection();
		
		// Create a new statement (will be automatically closed by the StatementIterator)
		SQLiteStatement stmt = conn.prepare(sqlQuery, true); // true = cached enabled
		
		// Call the statement binder
		stmtBinder.accept(stmt);

		// Set some fields
		this.boundingBoxIterator = new BoundingBoxIterator(mzDbReader, stmt, msLevel);		
		this.mzDbReader = mzDbReader;
		this.statement = stmt;
		this.msLevel = msLevel;

		initBB();
	}

	public SQLiteStatement getStatement() {
		return this.statement;
	}

	protected void initBB() {
		if (boundingBoxIterator.hasNext())
			this.firstBB = boundingBoxIterator.next();
		else {
			this.firstBB = null;
		}
	}

	public void closeStatement() {
		statement.dispose();
	}

	public boolean hasNext() {

		if (this.firstBB != null) { // this.statement.hasRow() ) {//
			return true;
		} else {
			this.closeStatement();
			return false;
		}
	}

	public void remove() {
		throw new UnsupportedOperationException("Unsuported Operation");
	}

}
