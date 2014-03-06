/**
 * 
 */
package fr.profi.mzdb.io.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.table.RunSliceTable;
import fr.profi.mzdb.model.RunSliceHeader;
import fr.profi.mzdb.utils.sqlite.ISQLiteRecordExtraction;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;
import fr.profi.mzdb.utils.sqlite.SQLiteRecordIterator;

// TODO: Auto-generated Javadoc
/**
 * The Class RunSliceHeaderReader.
 * 
 * @author David Bouyssie
 */
public class RunSliceHeaderReader extends AbstractMzDbReaderHelper {

	/**
	 * Instantiates a new run slice header reader.
	 * 
	 * @param mzDbReader
	 *            the mz db reader
	 */
	public RunSliceHeaderReader(MzDbReader mzDbReader) {
		super(mzDbReader);
	}

	/**
	 * The Class RunSliceHeaderExtractor.
	 * 
	 * @author David Bouyssie
	 */
	private class RunSliceHeaderExtractor implements ISQLiteRecordExtraction<RunSliceHeader> {

		/*
		 * public RunSliceHeader extract(SQLiteRecord record, int runSliceNumber ) throws SQLiteException {
		 * 
		 * return new RunSliceHeader( record.columnInt(RunSliceTable.ID),
		 * record.columnInt(RunSliceTable.MS_LEVEL), runSliceNumber,
		 * record.columnDouble(RunSliceTable.BEGIN_MZ), record.columnDouble(RunSliceTable.END_MZ),
		 * record.columnInt(RunSliceTable.RUN_ID) );
		 * 
		 * }
		 */

		/*
		 * (non-Javadoc)
		 * 
		 * @see fr.profi.mzdb.utils.sqlite.ISQLiteRecordExtraction#extract(fr.profi.mzdb
		 * .utils.sqlite.SQLiteRecord)
		 */
		public RunSliceHeader extract(SQLiteRecord record) throws SQLiteException {
			// return this.extract( record, record.columnInt(RunSliceTable.NUMBER) );

			return new RunSliceHeader(record.columnInt(RunSliceTable.ID),
					record.columnInt(RunSliceTable.MS_LEVEL), record.columnInt(RunSliceTable.NUMBER),
					record.columnDouble(RunSliceTable.BEGIN_MZ), record.columnDouble(RunSliceTable.END_MZ),
					record.columnInt(RunSliceTable.RUN_ID));
		}

	}

	/** The _run slice header extractor. */
	private RunSliceHeaderExtractor _runSliceHeaderExtractor = new RunSliceHeaderExtractor();

	/**
	 * Gets the run slices.
	 * 
	 * @return array of runSlice instance without data associated
	 * @throws SQLiteException
	 *             the SQLite exception
	 */
	public RunSliceHeader[] getRunSliceHeaders() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.runSliceHeaders != null) {
			return this.entityCache.runSliceHeaders;
		} else {

			ArrayList<RunSliceHeader> rshList = new ArrayList<RunSliceHeader>();

			// Retrieve the corresponding run slices
			String queryStr = "SELECT * FROM run_slice";
			SQLiteRecordIterator records = new SQLiteQuery(connection, queryStr).getRecords();

			while (records.hasNext()) {
				SQLiteRecord record = records.next();
				rshList.add(_runSliceHeaderExtractor.extract(record));
			}

			RunSliceHeader[] runSliceHeaders = rshList.toArray(new RunSliceHeader[rshList.size()]);

			if (this.entityCache != null)
				this.entityCache.runSliceHeaders = runSliceHeaders;

			return runSliceHeaders;
		}

	}

	/**
	 * Gets the run slices.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return array of runSlice instance without data associated
	 * @throws SQLiteException
	 *             the SQLite exception
	 */
	public RunSliceHeader[] getRunSliceHeaders(int msLevel) throws SQLiteException {

		ArrayList<RunSliceHeader> rshList = new ArrayList<RunSliceHeader>();

		if (this.entityCache != null && this.entityCache.runSliceHeaders != null) {
			RunSliceHeader[] runSliceHeaders = this.entityCache.runSliceHeaders;
			for (RunSliceHeader rsh : runSliceHeaders) {
				if (rsh.getMsLevel() == msLevel)
					rshList.add(rsh);
			}
		} else {

			// Retrieve the corresponding run slices
			String queryStr = "SELECT * FROM run_slice WHERE ms_level=? ORDER BY begin_mz "; // number
			SQLiteRecordIterator records = new SQLiteQuery(connection, queryStr).bind(1, msLevel)
					.getRecords();

			while (records.hasNext()) {
				SQLiteRecord record = records.next();
				rshList.add(_runSliceHeaderExtractor.extract(record));
			}

		}

		return rshList.toArray(new RunSliceHeader[rshList.size()]);

	}

	/**
	 * _get run slice header by id.
	 * 
	 * @param runSliceHeaders
	 *            the run slice headers
	 * @return the hash map
	 */
	private HashMap<Integer, RunSliceHeader> _getRunSliceHeaderById(RunSliceHeader[] runSliceHeaders) {

		HashMap<Integer, RunSliceHeader> runSliceHeaderById = new HashMap<Integer, RunSliceHeader>(
				runSliceHeaders.length);
		for (RunSliceHeader runSlice : runSliceHeaders) {
			runSliceHeaderById.put(runSlice.getId(), runSlice);
		}

		return runSliceHeaderById;
	}

	/**
	 * Gets the run slice by id.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return the run slice by id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public HashMap<Integer, RunSliceHeader> getRunSliceHeaderById(int msLevel) throws SQLiteException {

		RunSliceHeader[] runSliceHeaders = this.getRunSliceHeaders(msLevel);
		return this._getRunSliceHeaderById(runSliceHeaders);
	}

	/**
	 * Gets the run slice header by id.
	 * 
	 * @return the run slice header by id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Map<Integer, RunSliceHeader> getRunSliceHeaderById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.runSliceHeaderById != null) {
			return this.entityCache.runSliceHeaderById;
		} else {

			HashMap<Integer, RunSliceHeader> runSliceHeaderById = this._getRunSliceHeaderById(this
					.getRunSliceHeaders());

			if (this.entityCache != null)
				this.entityCache.runSliceHeaderById = runSliceHeaderById;

			return runSliceHeaderById;
		}
	}

	/**
	 * Gets the run slice header.
	 * 
	 * @param id
	 *            the id
	 * @return the run slice header
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public RunSliceHeader getRunSliceHeader(int id) throws SQLiteException {
		if (this.entityCache != null) {
			return this.getRunSliceHeaderById().get(id);
		} else {
			String queryStr = "SELECT * FROM run_slice WHERE id = ?";
			return new SQLiteQuery(connection, queryStr).bind(1, id).extractRecord(
					this._runSliceHeaderExtractor);
		}
	}

	/**
	 * Gets the run slice for mz.
	 * 
	 * @param mz
	 *            the mz
	 * @param msLevel
	 *            the ms level
	 * @return the run slice for mz
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public RunSliceHeader getRunSliceForMz(double mz, int msLevel) throws SQLiteException {

		// Retrieve the corresponding run slices
		String queryStr = "SELECT * FROM run_slice WHERE ms_level = ? AND begin_mz <= ? AND end_mz > ?";
		return new SQLiteQuery(connection, queryStr).bind(1, msLevel).bind(2, mz).bind(3, mz)
				.extractRecord(_runSliceHeaderExtractor);
	}

	/**
	 * Gets the run slice ids for mz range.
	 * 
	 * @param minMz
	 *            the min mz
	 * @param maxMz
	 *            the max mz
	 * @param msLevel
	 *            the ms level
	 * @return the run slice ids for mz range
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int[] getRunSliceIdsForMzRange(double minMz, double maxMz, int msLevel) throws SQLiteException {

		RunSliceHeader firstRunSlice = this.getRunSliceForMz(minMz, msLevel);
		RunSliceHeader lastRunSlice = this.getRunSliceForMz(maxMz, msLevel);
		double mzHeight = (msLevel == 1) ? mzDbReader.getBBSizes().BB_MZ_HEIGHT_MS1
				: mzDbReader.getBBSizes().BB_MZ_HEIGHT_MSn;

		int bufferLength = 1 + (int) ((maxMz - minMz) / mzHeight);

		String queryStr = "SELECT id FROM run_slice WHERE ms_level = ? AND begin_mz >= ? AND end_mz <= ?";

		return new SQLiteQuery(connection, queryStr).bind(1, msLevel).bind(2, firstRunSlice.getBeginMz())
				.bind(3, lastRunSlice.getEndMz()).extractInts(bufferLength);
	}

}
