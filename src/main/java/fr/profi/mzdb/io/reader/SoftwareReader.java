package fr.profi.mzdb.io.reader;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.db.model.Software;
import fr.profi.mzdb.db.table.InstrumentConfigurationTable;
import fr.profi.mzdb.db.table.SoftwareTable;
import fr.profi.mzdb.utils.sqlite.ISQLiteRecordExtraction;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;

// TODO: Auto-generated Javadoc
/**
 * The Class SoftwareReader.
 * 
 * @author David Bouyssie
 */
public class SoftwareReader {

	/** The connection. */
	protected SQLiteConnection connection = null;

	/**
	 * Instantiates a new software reader.
	 * 
	 * @param connection
	 *            the connection
	 */
	public SoftwareReader(SQLiteConnection connection) {
		super();
		this.connection = connection;
	}

	/**
	 * Gets the software.
	 * 
	 * @param id
	 *            the id
	 * @return the software
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Software getSoftware(int id) throws SQLiteException {

		return new SQLiteQuery(connection, "select * from software where id = ?").bind(1, id).extractRecord(
				new ISQLiteRecordExtraction<Software>() {

					public Software extract(SQLiteRecord r) throws SQLiteException {

						int id = r.columnInt(InstrumentConfigurationTable.ID);
						String name = r.columnString(SoftwareTable.NAME);
						String version = r.columnString(SoftwareTable.VERSION);
						String paramTreeAsStr = r.columnString(SoftwareTable.PARAM_TREE);

						return new Software(id, name, version, ParamTreeParser.parseParamTree(paramTreeAsStr));
					}
				});

	}

}
