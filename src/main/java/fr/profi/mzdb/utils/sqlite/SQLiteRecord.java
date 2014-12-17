package fr.profi.mzdb.utils.sqlite;

import java.io.InputStream;
import java.util.HashMap;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

/**
 * @author David Bouyssie
 * 
 */
public class SQLiteRecord {

	protected SQLiteQuery sqliteQuery = null;
	protected SQLiteStatement stmt = null;
	protected HashMap<String, Integer> colIdxByColName = new HashMap<String, Integer>();

	public SQLiteRecord(SQLiteQuery sqliteQuery) {
		super();
		this.sqliteQuery = sqliteQuery;
		this.stmt = sqliteQuery.stmt;
		this.colIdxByColName = sqliteQuery.resultDesc.colIdxByColName;
	}
	
	public SQLiteStatement getStatement() {
		return this.stmt;
	}

	public int getColumnIndex(String colName) throws SQLiteException {
		if (this.colIdxByColName.containsKey(colName) == false) {
			throw new SQLiteException(-1, "undefined column '" + colName + "' in query: "+ this.sqliteQuery.queryString);
		}

		return this.colIdxByColName.get(colName);
	}

	public String[] getColumnNames() {
		return this.sqliteQuery.resultDesc.getColumnNames();
	}

	// TODO: replace calls to name().toLowerCase() by toString() ?
	public String columnString(Enum<?> enumeration) throws SQLiteException {
		return this.columnString(enumeration.name().toLowerCase());
	}

	public String columnString(String columnName) throws SQLiteException {
		return this.stmt.columnString(this.getColumnIndex(columnName));
	}

	public int columnInt(Enum<?> enumeration) throws SQLiteException {
		return this.columnInt(enumeration.name().toLowerCase());
	}

	public int columnInt(String columnName) throws SQLiteException {
		return this.stmt.columnInt(this.getColumnIndex(columnName));
	}

	public double columnDouble(Enum<?> enumeration) throws SQLiteException {
		return this.columnDouble(enumeration.name().toLowerCase());
	}

	public double columnDouble(String columnName) throws SQLiteException {
		return this.stmt.columnDouble(this.getColumnIndex(columnName));
	}

	public long columnLong(Enum<?> enumeration) throws SQLiteException {
		return this.columnLong(enumeration.name().toLowerCase());
	}

	public long columnLong(String columnName) throws SQLiteException {
		return this.stmt.columnLong(this.getColumnIndex(columnName));
	}

	public byte[] columnBlob(Enum<?> enumeration) throws SQLiteException {
		return this.columnBlob(enumeration.name().toLowerCase());
	}

	public byte[] columnBlob(String columnName) throws SQLiteException {
		return this.stmt.columnBlob(this.getColumnIndex(columnName));
	}

	public InputStream columnStream(Enum<?> enumeration) throws SQLiteException {
		return this.columnStream(enumeration.name().toLowerCase());
	}

	public InputStream columnStream(String columnName) throws SQLiteException {
		return this.stmt.columnStream(this.getColumnIndex(columnName));
	}

	public boolean columnNull(Enum<?> enumeration) throws SQLiteException {
		return this.columnNull(enumeration.name().toLowerCase());
	}

	public boolean columnNull(String columnName) throws SQLiteException {
		return this.stmt.columnNull(this.getColumnIndex(columnName));
	}

}
