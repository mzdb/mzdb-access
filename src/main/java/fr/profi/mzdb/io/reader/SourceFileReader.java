package fr.profi.mzdb.io.reader;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.db.model.SourceFile;
import fr.profi.mzdb.db.table.InstrumentConfigurationTable;
import fr.profi.mzdb.db.table.SourceFileTable;
import fr.profi.mzdb.utils.sqlite.ISQLiteRecordExtraction;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;

// TODO: Auto-generated Javadoc

/**
 * The Class SourceFileReader.
 * 
 * @author David Bouyssie
 */
public class SourceFileReader {

	/** The connection. */
	protected SQLiteConnection connection = null;

	/**
	 * Instantiates a new source file reader.
	 * 
	 * @param connection
	 *            the connection
	 */
	public SourceFileReader(SQLiteConnection connection) {
		super();
		this.connection = connection;
	}

	/**
	 * Gets the source file.
	 * 
	 * @param id
	 *            the id
	 * @return the source file
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public SourceFile getSourceFile(int id) throws SQLiteException {

		return new SQLiteQuery(connection, "select * from source_file where id = ?").bind(1, id)
				.extractRecord(new ISQLiteRecordExtraction<SourceFile>() {

					public SourceFile extract(SQLiteRecord r) throws SQLiteException {

						int id = r.columnInt(InstrumentConfigurationTable.ID);
						String name = r.columnString(SourceFileTable.NAME);
						String location = r.columnString(SourceFileTable.LOCATION);
						String paramTreeAsStr = r.columnString(SourceFileTable.PARAM_TREE);

						return new SourceFile(id, name, location, ParamTreeParser
								.parseParamTree(paramTreeAsStr));
					}
				});
	}

}
