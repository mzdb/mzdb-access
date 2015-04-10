package fr.profi.mzdb.io.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.MzDbReader;
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
	private static String _scanHeaderQueryStr = 
		"SELECT id, initial_id, cycle, time, ms_level, tic, "+
		"base_peak_mz, base_peak_intensity, main_precursor_mz, main_precursor_charge, " +
		"data_points_count, data_encoding_id, bb_first_spectrum_id FROM spectrum";
	
	private static String _ms1ScanHeaderQueryStr = _scanHeaderQueryStr + " WHERE ms_level = 1";	
	private static String _ms2ScanHeaderQueryStr = _scanHeaderQueryStr + " WHERE ms_level = 2";
	
	private enum ScanHeaderCols {

		ID("id"),
		INITIAL_ID("initial_id"),
		CYCLE("cycle"),
		TIME("time"),
		MS_LEVEL("ms_level"),
		TIC("tic"),
		BASE_PEAK_MZ("base_peak_mz"),
		BASE_PEAK_INTENSITY("base_peak_intensity"),
		MAIN_PRECURSOR_MZ("main_precursor_mz"),
		MAIN_PRECURSOR_CHARGE("main_precursor_charge"),
		DATA_POINTS_COUNT("data_points_count"),
		DATA_ENCODING_ID("data_encoding_id"),
		BB_FIRST_SPECTRUM_ID("bb_first_spectrum_id");

		@SuppressWarnings("unused")
		protected final String columnName;

		private ScanHeaderCols(String colName) {
			this.columnName = colName;
		}

	}

	private ISQLiteRecordExtraction<ScanHeader> _scanHeaderExtractor = new ISQLiteRecordExtraction<ScanHeader>() {

		int idColIdx = ScanHeaderCols.ID.ordinal();
		int initialIdColIdx = ScanHeaderCols.INITIAL_ID.ordinal();
		int cycleColIdx = ScanHeaderCols.CYCLE.ordinal();
		int timeColIdx = ScanHeaderCols.TIME.ordinal();
		int msLevelColIdx = ScanHeaderCols.MS_LEVEL.ordinal();
		int ticColIdx = ScanHeaderCols.TIC.ordinal();
		int basePeakMzColIdx = ScanHeaderCols.BASE_PEAK_MZ.ordinal();
		int basePeakIntensityColIdx = ScanHeaderCols.BASE_PEAK_INTENSITY.ordinal();
		int mainPrecursorMzColIdx = ScanHeaderCols.MAIN_PRECURSOR_MZ.ordinal();
		int mainPrecursorChargeColIdx = ScanHeaderCols.MAIN_PRECURSOR_CHARGE.ordinal();
		int dataPointsCountColIdx = ScanHeaderCols.DATA_POINTS_COUNT.ordinal();
		int dataEncodingIdColIdx = ScanHeaderCols.DATA_ENCODING_ID.ordinal();
		int bbFirstSpectrumIdColIdx = ScanHeaderCols.BB_FIRST_SPECTRUM_ID.ordinal();
		
		public ScanHeader extract(SQLiteRecord record) throws SQLiteException {
		  
		  SQLiteStatement stmt = record.getStatement();
			
			//long nano = System.nanoTime();
			int msLevel = stmt.columnInt(msLevelColIdx);

			double precursorMz = 0.0;
			int precursorCharge = 0;
			if (msLevel == 2) {
				precursorMz = stmt.columnDouble(mainPrecursorMzColIdx);
				precursorCharge = stmt.columnInt(mainPrecursorChargeColIdx);
			}

			int bbFirstSpectrumId = stmt.columnInt(bbFirstSpectrumIdColIdx);
			
			DataEncoding dataEnc = mzDbReader.getDataEncoding( stmt.columnInt(dataEncodingIdColIdx) );
			
			boolean isHighRes = dataEnc.getPeakEncoding() == PeakEncoding.LOW_RES_PEAK ? false : true;
			
			ScanHeader sh = new ScanHeader(
			  stmt.columnInt(idColIdx),
			  stmt.columnInt(initialIdColIdx),
			  stmt.columnInt(cycleColIdx),
				(float) stmt.columnDouble(timeColIdx),
				msLevel,
				stmt.columnInt(dataPointsCountColIdx),
				isHighRes,
				(float) stmt.columnDouble(ticColIdx),
				stmt.columnDouble(basePeakMzColIdx),
				(float) stmt.columnDouble(basePeakIntensityColIdx),
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
	 * Gets the MS1 scan headers.
	 * 
	 * @return the scan headers
	 * @throws SQLiteException
	 */
	public ScanHeader[] getMs1ScanHeaders() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.ms1ScanHeaders != null) {
			return this.entityCache.ms1ScanHeaders;
		} else {

			ScanHeader[] ms1ScanHeaders = new ScanHeader[this.mzDbReader.getScansCount(1)];
			
			new SQLiteQuery(connection, _ms1ScanHeaderQueryStr)
				.extractRecords(this._scanHeaderExtractor, ms1ScanHeaders);

			if (this.entityCache != null)
				this.entityCache.ms1ScanHeaders = ms1ScanHeaders;

			return ms1ScanHeaders;
		}

	}

	/**
	 * Gets the MS1 scan header by id.
	 * 
	 * @return the scan header by id
	 * @throws SQLiteException
	 */
	public Map<Integer, ScanHeader> getMs1ScanHeaderById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.ms1ScanHeaderById != null) {
			return this.entityCache.ms1ScanHeaderById;
		} else {
			ScanHeader[] ms1ScanHeaders = this.getMs1ScanHeaders();

			Map<Integer, ScanHeader> ms1ScanHeaderById = new HashMap<Integer, ScanHeader>(ms1ScanHeaders.length);

			for (ScanHeader ms1ScanHeader : ms1ScanHeaders)
				ms1ScanHeaderById.put(ms1ScanHeader.getId(), ms1ScanHeader);

			if (this.entityCache != null)
				this.entityCache.ms1ScanHeaderById = ms1ScanHeaderById;

			return ms1ScanHeaderById;
		}
	}
	
	/**
	 * Gets the MS2 scan headers.
	 * 
	 * @return the scan headers
	 * @throws SQLiteException
	 */
	public ScanHeader[] getMs2ScanHeaders() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.ms2ScanHeaders != null) {
			return this.entityCache.ms2ScanHeaders;
		} else {

			ScanHeader[] ms2ScanHeaders = new ScanHeader[this.mzDbReader.getScansCount(2)];
			
			new SQLiteQuery(connection, _ms2ScanHeaderQueryStr)
				.extractRecords(this._scanHeaderExtractor, ms2ScanHeaders);

			if (this.entityCache != null)
				this.entityCache.ms2ScanHeaders = ms2ScanHeaders;

			return ms2ScanHeaders;
		}

	}
	
	/**
	 * Gets the MS2 scan header by id.
	 * 
	 * @return the scan header by id
	 * @throws SQLiteException
	 */
	public Map<Integer, ScanHeader> getMs2ScanHeaderById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.ms2ScanHeaderById != null) {
			return this.entityCache.ms2ScanHeaderById;
		} else {
			ScanHeader[] ms2ScanHeaders = this.getMs2ScanHeaders();

			Map<Integer, ScanHeader> ms2ScanHeaderById = new HashMap<Integer, ScanHeader>(ms2ScanHeaders.length);

			for (ScanHeader ms2ScanHeader : ms2ScanHeaders)
				ms2ScanHeaderById.put(ms2ScanHeader.getId(), ms2ScanHeader);

			if (this.entityCache != null)
				this.entityCache.ms2ScanHeaderById = ms2ScanHeaderById;

			return ms2ScanHeaderById;
		}
	}

	/**
	 * Gets the scan header.
	 * 
	 * @param id the id
	 * @return scan header
	 * @throws SQLiteException
	 */
	public ScanHeader getScanHeader(int id) throws SQLiteException {
		if (this.entityCache != null) {
			
			if( this.getMs1ScanHeaderById().containsKey(id) ) {
				return this.getMs1ScanHeaderById().get(id);
			} else if ( this.getMs2ScanHeaderById().containsKey(id) ) {
				return this.getMs2ScanHeaderById().get(id);
			} else {
				return null;
			}
			  
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

		if ( this.entityCache != null ) {
			Map<Integer, ArrayList<Integer>> scanIdsByTimeIndex = this.getScanIdsByTimeIndex(msLevel);
			
			int timeIndex = (int) (time / TIME_INDEX_WIDTH);
			ScanHeader nearestScanHeader = null;			

			for (int index = timeIndex - 1; index <= timeIndex + 1; index++) {

				if (scanIdsByTimeIndex.containsKey(index) == false) {
					continue;
				}

				ArrayList<Integer> tmpScanIds = scanIdsByTimeIndex.get(index);
				for (Integer tmpScanId : tmpScanIds) {

					ScanHeader scanH = this.getScanHeader(tmpScanId);
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
	 */
	protected Map<Integer, ArrayList<Integer>> getScanIdsByTimeIndex(int msLevel) throws SQLiteException {

		HashMap<Integer, ArrayList<Integer>> scanIdsByTimeIndex = null;
		if (this.entityCache != null) {
			
			if( msLevel == 1 && this.entityCache.ms1ScanIdsByTimeIndex != null) {
				scanIdsByTimeIndex = (HashMap<Integer, ArrayList<Integer>>) this.entityCache.ms1ScanIdsByTimeIndex;
			}
			else if ( msLevel == 2 & this.entityCache.ms1ScanIdsByTimeIndex != null) {
				scanIdsByTimeIndex = (HashMap<Integer, ArrayList<Integer>>) this.entityCache.ms2ScanIdsByTimeIndex;
			}
			
		}
		
		if(scanIdsByTimeIndex != null) return scanIdsByTimeIndex;
		else {
			scanIdsByTimeIndex = new HashMap<Integer, ArrayList<Integer>>();
			
			ScanHeader[] scanHeaders;
			if( msLevel == 1) scanHeaders = this.getMs1ScanHeaders();
			else if ( msLevel == 2 ) scanHeaders = this.getMs2ScanHeaders();
			else return null;

			for (ScanHeader scanH : scanHeaders) {
				int timeIndex = (int) (scanH.getTime() / TIME_INDEX_WIDTH);

				if (scanIdsByTimeIndex.get(timeIndex) == null)
					scanIdsByTimeIndex.put(timeIndex, new ArrayList<Integer>());

				scanIdsByTimeIndex.get(timeIndex).add(scanH.getId());
			}
			
			if (this.entityCache != null) {
				if( msLevel == 1) this.entityCache.ms1ScanIdsByTimeIndex = scanIdsByTimeIndex;
				else if ( msLevel == 2 ) this.entityCache.ms2ScanIdsByTimeIndex = scanIdsByTimeIndex;
			}
			
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
