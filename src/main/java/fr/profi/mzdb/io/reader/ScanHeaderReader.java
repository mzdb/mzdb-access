package fr.profi.mzdb.io.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.table.SpectrumTable;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.utils.sqlite.ISQLiteRecordExtraction;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;

/**
 * @author David Bouyssie
 * 
 */
public class ScanHeaderReader extends AbstractMzDbReaderHelper {

	/** The time index width. */
	static int TIME_INDEX_WIDTH = 15;

	private HashMap<Integer, ArrayList<Integer>> _scanIdsByTimeIndex = null;

	/**
	 * @param mzDbReader
	 */
	public ScanHeaderReader(MzDbReader mzDbReader) {
		super(mzDbReader);
	}

	// Define some variable for scan header extraction
	private static String _scanHeaderQueryStr = "SELECT * FROM spectrum";

	private ISQLiteRecordExtraction<ScanHeader> _scanHeaderExtractor = new ISQLiteRecordExtraction<ScanHeader>() {

		public ScanHeader extract(SQLiteRecord record) throws SQLiteException {

			int msLevel = record.columnInt(SpectrumTable.MS_LEVEL);

			double precursorMz = 0.0;
			int precursorCharge = 0;
			if (msLevel == 2) {
				precursorMz = record.columnDouble(SpectrumTable.MAIN_PRECURSOR_MZ);
				precursorCharge = record.columnInt(SpectrumTable.MAIN_PRECURSOR_CHARGE);
			}
			
			int bbFirstSpectrumId = record.columnInt(SpectrumTable.BB_FIRST_SPECTRUM_ID);

			// Parse param tree
			String paramTreeAsStr = record.columnString(SpectrumTable.PARAM_TREE);
			ParamTree paramTree =  ParamTreeParser.parseParamTree(paramTreeAsStr);

			String val = paramTree.getUserParam("in_high_res").getValue();
			// little workaround to retrieve the data resolution
			// FIXME: works only for Orbitrap data
			// FIXME: retrieve the resolution from the data encoding param tree
			boolean isHighRes = (val.equals("true")) ? true : false;

			ScanHeader sh = new ScanHeader(
				record.columnInt(SpectrumTable.ID),
				record.columnInt(SpectrumTable.INITIAL_ID),
				record.columnInt(SpectrumTable.CYCLE),
				(float) record.columnDouble(SpectrumTable.TIME),
				msLevel,
				record.columnInt(SpectrumTable.DATA_POINTS_COUNT),
				isHighRes,
				(float) record.columnDouble(SpectrumTable.TIC),
				record.columnDouble(SpectrumTable.BASE_PEAK_MZ),
				(float) record.columnDouble(SpectrumTable.BASE_PEAK_INTENSITY),
				precursorMz,
				precursorCharge,
				bbFirstSpectrumId
			);

			//sh.setParamTree(paramTree);
			
			return sh;

		}

	};

	/**
	 * Gets the scan headers.
	 * 
	 * @return the scan headers
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public ScanHeader[] getScanHeaders() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.scanHeaders != null) {
			return this.entityCache.scanHeaders;
		} else {

			ScanHeader[] scanHeaders = new ScanHeader[this.mzDbReader.getScansCount()];
			new SQLiteQuery(connection, _scanHeaderQueryStr).extractRecords(this._scanHeaderExtractor,
					scanHeaders);

			if (this.entityCache != null)
				this.entityCache.scanHeaders = scanHeaders;

			return scanHeaders;
		}

	}

	/**
	 * Gets the scan header by id.
	 * 
	 * @return the scan header by id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Map<Integer, ScanHeader> getScanHeaderById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.scanHeaderById != null) {
			return this.entityCache.scanHeaderById;
		} else {
			ScanHeader[] scanHeaders = this.getScanHeaders();
			Map<Integer, ScanHeader> scanHeaderById = new HashMap<Integer, ScanHeader>(scanHeaders.length);

			for (ScanHeader scanHeader : scanHeaders)
				scanHeaderById.put(scanHeader.getId(), scanHeader);

			if (this.entityCache != null)
				this.entityCache.scanHeaderById = scanHeaderById;

			return scanHeaderById;
		}
	}

	/**
	 * Gets the scan header.
	 * 
	 * @param id
	 *            the id
	 * @return scan header
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public ScanHeader getScanHeader(int id) throws SQLiteException {
		if (this.entityCache != null) {
			return this.getScanHeaderById().get(id);
		} else {
			String queryStr = _scanHeaderQueryStr + " WHERE id = ? ";
			return new SQLiteQuery(connection, queryStr).bind(1, id).extractRecord(this._scanHeaderExtractor);
		}
	}

	/**
	 * Gets the scan id by time.
	 * 
	 * @return the scan id by time
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	// TODO: remove this method and related cache when mzDB is updated
	public Map<Float, Integer> getScanIdByTime() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.scanIdByTime != null) {
			return this.entityCache.scanIdByTime;
		} else {
			ScanHeader[] scanHeaders = this.getScanHeaders();
			HashMap<Float, Integer> scanIdByTime = new HashMap<Float, Integer>(scanHeaders.length);

			for (ScanHeader h : scanHeaders)
				scanIdByTime.put(h.getTime(), h.getId());

			if (this.entityCache != null)
				this.entityCache.scanIdByTime = scanIdByTime;

			return scanIdByTime;
		}

	}

	/**
	 * Gets the scan header by initial id.
	 * 
	 * @return the scan header by initial id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	/*
	 * public HashMap<Integer, ScanHeader> getScanHeaderByInitialId() throws SQLiteException { //ScanHeader[]
	 * scanHeaders = getScanHeaders(); HashMap<Integer, ScanHeader> scanHeaderByInitialId = new
	 * HashMap<Integer, ScanHeader>();
	 * 
	 * for (ScanHeader scanHeader : this.getScanHeaders()) {
	 * scanHeaderByInitialId.put(scanHeader.getInitialId(), scanHeader); }
	 * 
	 * return scanHeaderByInitialId; }
	 */

	/**
	 * Gets the scan ids by time index.
	 * 
	 * @return hashmap of key time index value array of scanIds
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public HashMap<Integer, ArrayList<Integer>> getScanIdsByTimeIndex() throws SQLiteException {

		// TODO: use entity cache

		if (this._scanIdsByTimeIndex == null) {

			this._scanIdsByTimeIndex = new HashMap<Integer, ArrayList<Integer>>();
			ScanHeader[] scanHeaders = this.getScanHeaders();

			for (ScanHeader scanH : scanHeaders) {
				int timeIndex = (int) (scanH.getTime() / TIME_INDEX_WIDTH);

				if (_scanIdsByTimeIndex.get(timeIndex) == null)
					this._scanIdsByTimeIndex.put(timeIndex, new ArrayList<Integer>());

				this._scanIdsByTimeIndex.get(timeIndex).add(scanH.getId());
			}
		}

		return this._scanIdsByTimeIndex;
	}

	/**
	 * Gets the scan header for time.
	 * 
	 * @param time
	 *            the time
	 * @param msLevel
	 *            the ms level
	 * @return scanheader the closest to the time input parameter
	 * @throws Exception
	 */
	public ScanHeader getScanHeaderForTime(float time, int msLevel) throws Exception {

		// TODO: implements a SQL alternative method

		int timeIndex = (int) (time / TIME_INDEX_WIDTH);

		ScanHeader nearestScanHeader = null;
		HashMap<Integer, ArrayList<Integer>> scanIdsByTimeIndex = this.getScanIdsByTimeIndex();

		for (int index = timeIndex - 1; index <= timeIndex + 1; index++) {

			if (scanIdsByTimeIndex.containsKey(index) == false) {
				continue;
			}

			ArrayList<Integer> tmpScanIds = scanIdsByTimeIndex.get(index);
			for (Integer tmpScanId : tmpScanIds) {

				ScanHeader scanH = getScanHeaderById().get(tmpScanId);
				if (scanH == null) {
					throw new Exception("can' t retrieve scan with id =" + tmpScanId);
				}

				if (scanH.getMsLevel() != msLevel)
					continue;

				if (nearestScanHeader == null
						|| Math.abs(scanH.getTime() - time) < Math.abs(nearestScanHeader.getTime() - time)) {
					nearestScanHeader = scanH;
				}
			}
		}

		return nearestScanHeader;
	}

	/**
	 * Gets the scan ids for time range.
	 * 
	 * @param minRT
	 *            the min rt
	 * @param maxRT
	 *            the max rt
	 * @param msLevel
	 *            the ms level
	 * @return array of integers corresponding to the ids of matching scan
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Integer[] getScanIdsForTimeRange(float minRT, float maxRT, int msLevel) throws SQLiteException {

		// TODO: use loadInts ?
		// TODO: use entity cache ?

		// Retrieve the corresponding run slices
		SQLiteStatement stmt = connection.prepare(
				"SELECT id FROM scan WHERE ms_level = ? AND time >= ? AND time <= ?", true);
		stmt.bind(1, msLevel);
		stmt.bind(2, minRT);
		stmt.bind(3, maxRT);

		ArrayList<Integer> scanIds = new ArrayList<Integer>();

		while (stmt.step()) {
			scanIds.add(stmt.columnInt(0));
		}

		stmt.dispose();

		return scanIds.toArray(new Integer[scanIds.size()]);
	}

	/**
	 * same as getScanIdsForTimeRange but without requiring sqlite statement.
	 * 
	 * @param minRT
	 *            the min rt
	 * @param maxRT
	 *            the max rt
	 * @param msLevel
	 *            the ms level
	 * @return array of integers corresponding to the ids of matching scan
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	/*
	 * public Integer[] getLazyScanIdsForTimeRange(float minRT, float maxRT, int msLevel) throws
	 * SQLiteException { int idmin = this.getScanHeaderForTime(minRT, msLevel).getId(); int idmax =
	 * this.getScanHeaderForTime(maxRT, msLevel).getId(); List<Integer> l = new ArrayList<Integer>();
	 * 
	 * for (int i = idmin; i <= idmax; ++i) { if (this.getScanHeader(i).getMsLevel() == msLevel) l.add(i); }
	 * return l.toArray(new Integer[l.size()]); }
	 */

}
