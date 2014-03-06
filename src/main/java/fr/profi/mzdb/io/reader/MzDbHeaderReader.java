package fr.profi.mzdb.io.reader;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.db.model.MzDbHeader;
import fr.profi.mzdb.db.model.params.ParamTree;

import fr.profi.mzdb.db.table.MzdbTable;
import fr.profi.mzdb.utils.sqlite.ISQLiteRecordExtraction;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;

// TODO: Auto-generated Javadoc
/**
 * The Class MzDbHeaderReader.
 * 
 * @author David Bouyssie
 */
public class MzDbHeaderReader {

	/** The connection. */
	protected SQLiteConnection connection = null;

	/**
	 * Instantiates a new mz db header reader.
	 * 
	 * @param connection
	 *            the connection
	 */
	public MzDbHeaderReader(SQLiteConnection connection) {
		super();
		this.connection = connection;
	}

	/**
	 * Gets the mz db header.
	 * 
	 * @return the mz db header
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public MzDbHeader getMzDbHeader() throws SQLiteException {
		return new SQLiteQuery(connection, "select * from mzdb").extractRecord(new ISQLiteRecordExtraction<MzDbHeader>() {

					public MzDbHeader extract(SQLiteRecord r) throws SQLiteException {

						String version = r.columnString(MzdbTable.VERSION);
						int creationTimestamp = r.columnInt(MzdbTable.CREATION_TIMESTAMP);
						String paramTreeAsStr = r.columnString(MzdbTable.PARAM_TREE);
						ParamTree paramTree =  ParamTreeParser.parseParamTree(paramTreeAsStr);
						return new MzDbHeader(version, creationTimestamp, paramTree);
					}
				});
	}

}
