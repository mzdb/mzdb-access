package fr.profi.mzdb.io.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.table.SpectrumTable;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.PeakEncoding;
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
	protected final static int TIME_INDEX_WIDTH = 15;

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
		  
		  SQLiteStatement stmt = record.getStatement();
			
			//long nano = System.nanoTime();
			int msLevel = stmt.columnInt(SpectrumTable.MS_LEVEL.ordinal());

			double precursorMz = 0.0;
			int precursorCharge = 0;
			if (msLevel == 2) {
				precursorMz = stmt.columnDouble(SpectrumTable.MAIN_PRECURSOR_MZ.ordinal());
				precursorCharge = stmt.columnInt(SpectrumTable.MAIN_PRECURSOR_CHARGE.ordinal());
			}

			int bbFirstSpectrumId = stmt.columnInt(SpectrumTable.BB_FIRST_SPECTRUM_ID.ordinal());
			
			DataEncoding dataEnc = mzDbReader.getDataEncoding( stmt.columnInt(SpectrumTable.DATA_ENCODING_ID.ordinal()) );
			
			boolean isHighRes = dataEnc.getPeakEncoding() == PeakEncoding.LOW_RES_PEAK ? false : true;
			
			ScanHeader sh = new ScanHeader(
			  stmt.columnInt(SpectrumTable.ID.ordinal()),
			  stmt.columnInt(SpectrumTable.INITIAL_ID.ordinal()),
			  stmt.columnInt(SpectrumTable.CYCLE.ordinal()),
				(float) stmt.columnDouble(SpectrumTable.TIME.ordinal()),
				msLevel,
				stmt.columnInt(SpectrumTable.DATA_POINTS_COUNT.ordinal()),
				isHighRes,
				(float) stmt.columnDouble(SpectrumTable.TIC.ordinal()),
				stmt.columnDouble(SpectrumTable.BASE_PEAK_MZ.ordinal()),
				(float) stmt.columnDouble(SpectrumTable.BASE_PEAK_INTENSITY.ordinal()),
				precursorMz,
				precursorCharge,
				bbFirstSpectrumId
			);
			
			//System.out.println(System.nanoTime() - nano);

			// sh.setParamTree(paramTree);

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

			long nano = System.nanoTime();
			ScanHeader[] scanHeaders = new ScanHeader[this.mzDbReader.getScansCount()];
			
			new SQLiteQuery(connection, _scanHeaderQueryStr)
				.extractRecords(this._scanHeaderExtractor, scanHeaders);

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
	 * Gets the scan time by id.
	 * 
	 * @return the scan time mapped by the scan id
	 * @throws SQLiteException the SQLite exception
	 */
	public Map<Integer, Float> getScanTimeById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.scanTimeById != null) {
			return this.entityCache.scanTimeById;
		} else {
			int scansCount = this.mzDbReader.getScansCount();
			float[] scanTimes = new SQLiteQuery(connection, "SELECT time FROM spectrum").extractFloats(scansCount);

			Map<Integer, Float> scanTimeById = new HashMap<Integer, Float>(scansCount);

			// TODO: check this approach is not too dangerous
			int scanId = 0;
			for (float scanTime : scanTimes) {
				scanId++;
				scanTimeById.put(scanId, scanTime);
			}

			if (this.entityCache != null)
				this.entityCache.scanTimeById = scanTimeById;

			return scanTimeById;
		}
	}
	
	/**
	 * Gets the scan header for time.
	 * 
	 * @param time
	 *            the time
	 * @param msLevel
	 *            the ms level
	 * @return ScanHeader the closest to the time input parameter
	 * @throws Exception
	 */
	public ScanHeader getScanHeaderForTime(float time, int msLevel) throws Exception {

		if (this.entityCache != null && this.entityCache.scanIdsByTimeIndex != null) {
			int timeIndex = (int) (time / TIME_INDEX_WIDTH);

			ScanHeader nearestScanHeader = null;
			Map<Integer, ArrayList<Integer>> scanIdsByTimeIndex = this.getScanIdsByTimeIndex();

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

					if ( nearestScanHeader == null || 
						 Math.abs(scanH.getTime() - time) < Math.abs(nearestScanHeader.getTime() - time) ) {
						nearestScanHeader = scanH;
					}
				}
			}

			return nearestScanHeader;
		} else {
			String queryStr = "SELECT id FROM spectrum WHERE ms_level = ? ORDER BY abs(spectrum.time - ?) ASC limit 1";
			int scanId = new SQLiteQuery(connection, queryStr)
				.bind(1,msLevel)
				.bind(2,time)
				.extractSingleInt();
			
			return this.getScanHeader(scanId);
		}

	}
  
	/**
	 * Gets the scan ids by time index.
	 * 
	 * @return hashmap of key time index value array of scanIds
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	protected Map<Integer, ArrayList<Integer>> getScanIdsByTimeIndex() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.scanIdsByTimeIndex != null) {
			return this.entityCache.scanIdsByTimeIndex;
		} else {
			HashMap<Integer, ArrayList<Integer>> scanIdsByTimeIndex = new HashMap<Integer, ArrayList<Integer>>();
			ScanHeader[] scanHeaders = this.getScanHeaders();

			for (ScanHeader scanH : scanHeaders) {
				int timeIndex = (int) (scanH.getTime() / TIME_INDEX_WIDTH);

				if (scanIdsByTimeIndex.get(timeIndex) == null)
					scanIdsByTimeIndex.put(timeIndex, new ArrayList<Integer>());

				scanIdsByTimeIndex.get(timeIndex).add(scanH.getId());
			}
			
			if (this.entityCache != null)
				this.entityCache.scanIdsByTimeIndex = scanIdsByTimeIndex;
			
			return scanIdsByTimeIndex;
		}
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
			"SELECT id FROM scan WHERE ms_level = ? AND time >= ? AND time <= ?",
			true
		);
		
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

}
