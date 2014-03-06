package fr.profi.mzdb.io.reader.iterator;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.BoundingBox;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public abstract class AbstractScanSliceIterator {

	protected final MzDbReader mzDbReader;
	protected final SQLiteStatement statement;
	protected final BoundingBoxIterator boundingBoxIterator;
	protected boolean isStatementClosed = false;
	protected BoundingBox firstBB;
	protected int msLevel;

	public AbstractScanSliceIterator(MzDbReader mzdbReader, String sqlQuery, int msLevel)
			throws SQLiteException {
		// super();
		this.mzDbReader = mzdbReader;

		SQLiteConnection conn = mzDbReader.getConnection();
		SQLiteStatement stmt = conn.prepare(sqlQuery, true); // false = disable
		// statement cache
		stmt.bind(1, msLevel);

		this.boundingBoxIterator = new BoundingBoxIterator(mzDbReader, stmt,
				mzdbReader.getDataEncodingByScanId(), msLevel);
		this.statement = stmt;
		this.msLevel = msLevel;

		initBB();
	}

	protected void initBB() {
		if (boundingBoxIterator.hasNext())
			this.firstBB = boundingBoxIterator.next();
		else {
			this.firstBB = null;
		}
	}

	public void closeStatement() {
		if (!isStatementClosed && boundingBoxIterator.isStatementClosed()) {
			if (statement != null)
				statement.dispose();
			isStatementClosed = true;
		}
	}

	public boolean hasNext() {

		if (this.firstBB != null)
			return true;
		else {
			this.closeStatement();
			return false;
		}
	}

	public void remove() {
		throw new UnsupportedOperationException("Unsuported Operation");
	}

}
