package fr.profi.mzdb.io.reader;

import com.almworks.sqlite4java.SQLiteConnection;

import fr.profi.mzdb.MzDbReader;

/**
 * @author David Bouyssie
 * 
 */
public class AbstractMzDbReaderHelper {

	/** The mz db reader. */
	protected MzDbReader mzDbReader = null;
	protected SQLiteConnection connection = null;
	protected MzDbEntityCache entityCache = null;

	/**
	 * Instantiates a new abstract mz db reader helper.
	 * 
	 * @param mzDbReader
	 *            the mz db reader
	 */
	public AbstractMzDbReaderHelper(MzDbReader mzDbReader) {
		super();
		this.mzDbReader = mzDbReader;
		this.connection = mzDbReader.getConnection();
		this.entityCache = mzDbReader.getEntityCache();
	}

}
