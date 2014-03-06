package fr.profi.mzdb.utils.sqlite;

import com.almworks.sqlite4java.SQLiteException;

/**
 * @author David Bouyssie
 * 
 */
public interface ISQLiteRecordOperation {
	void execute(SQLiteRecord elem, int idx) throws SQLiteException;
}