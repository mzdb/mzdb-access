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
	
	public static boolean loadParamTree = false;
	public static boolean loadScanList = false;
	public static boolean loadPrecursorList = false;

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
		"data_points_count, param_tree, scan_list, precursor_list, data_encoding_id, bb_first_spectrum_id FROM spectrum";
	
	private static String _ms1ScanHeaderQueryStr = _scanHeaderQueryStr + " WHERE ms_level = 1";	
	private static String _ms2ScanHeaderQueryStr = _scanHeaderQueryStr + " WHERE ms_level = 2";
	
	private enum ScanHeaderCol {

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
		PARAM_TREE("param_tree"),
		SCAN_LIST("scan_list"),
		PRECURSOR_LIST("precursor_list"),
		DATA_ENCODING_ID("data_encoding_id"),
		BB_FIRST_SPECTRUM_ID("bb_first_spectrum_id");

		@SuppressWarnings("unused")
		protected final String columnName;

		private ScanHeaderCol(String colName) {
			this.columnName = colName;
		}

	}
	
	private static class ScanHeaderColIdx {	
	  static int id = ScanHeaderCol.ID.ordinal();
	  static int initialId= ScanHeaderCol.INITIAL_ID.ordinal();
	  static int cycleCol= ScanHeaderCol.CYCLE.ordinal();
	  static int time = ScanHeaderCol.TIME.ordinal();
	  static int msLevel = ScanHeaderCol.MS_LEVEL.ordinal();
	  static int tic = ScanHeaderCol.TIC.ordinal();
	  static int basePeakMz = ScanHeaderCol.BASE_PEAK_MZ.ordinal();
	  static int basePeakIntensity = ScanHeaderCol.BASE_PEAK_INTENSITY.ordinal();
	  static int mainPrecursorMz = ScanHeaderCol.MAIN_PRECURSOR_MZ.ordinal();
	  static int mainPrecursorCharge = ScanHeaderCol.MAIN_PRECURSOR_CHARGE.ordinal();
	  static int dataPointsCount = ScanHeaderCol.DATA_POINTS_COUNT.ordinal();
	  static int paramTree = ScanHeaderCol.PARAM_TREE.ordinal();
	  static int scanList = ScanHeaderCol.SCAN_LIST.ordinal();
	  static int precursorList = ScanHeaderCol.PRECURSOR_LIST.ordinal();
	  static int dataEncodingId = ScanHeaderCol.DATA_ENCODING_ID.ordinal();
	  static int bbFirstSpectrumId = ScanHeaderCol.BB_FIRST_SPECTRUM_ID.ordinal();
	}

	private ISQLiteRecordExtraction<ScanHeader> _scanHeaderExtractor = new ISQLiteRecordExtraction<ScanHeader>() {
		
		public ScanHeader extract(SQLiteRecord record) throws SQLiteException {
			
			SQLiteStatement stmt = record.getStatement();
			
			//long nano = System.nanoTime();
			int msLevel = stmt.columnInt(ScanHeaderColIdx.msLevel);

			double precursorMz = 0.0;
			int precursorCharge = 0;
			if (msLevel == 2) {
				precursorMz = stmt.columnDouble(ScanHeaderColIdx.mainPrecursorMz);
				precursorCharge = stmt.columnInt(ScanHeaderColIdx.mainPrecursorCharge);
			}

			int bbFirstSpectrumId = stmt.columnInt(ScanHeaderColIdx.bbFirstSpectrumId);
			
			DataEncoding dataEnc = mzDbReader.getDataEncoding( stmt.columnInt(ScanHeaderColIdx.dataEncodingId) );
			
			boolean isHighRes = dataEnc.getPeakEncoding() == PeakEncoding.LOW_RES_PEAK ? false : true;
			
			ScanHeader sh = new ScanHeader(
				stmt.columnLong(ScanHeaderColIdx.id),
				stmt.columnInt(ScanHeaderColIdx.initialId),
				stmt.columnInt(ScanHeaderColIdx.cycleCol),
				(float) stmt.columnDouble(ScanHeaderColIdx.time),
				msLevel,
				stmt.columnInt(ScanHeaderColIdx.dataPointsCount),
				isHighRes,
				(float) stmt.columnDouble(ScanHeaderColIdx.tic),
				stmt.columnDouble(ScanHeaderColIdx.basePeakMz),
				(float) stmt.columnDouble(ScanHeaderColIdx.basePeakIntensity),
				precursorMz,
				precursorCharge,
				bbFirstSpectrumId
			);
			
			if( loadParamTree ) {
				sh.setParamTree( ParamTreeParser.parseParamTree(stmt.columnString(ScanHeaderColIdx.paramTree)) );
			}
			if( loadScanList ) {
				sh.setScanList( ParamTreeParser.parseScanList(stmt.columnString(ScanHeaderColIdx.scanList)) );
			}
			if( loadPrecursorList && msLevel > 1 ) {
				sh.setPrecursor( ParamTreeParser.parsePrecursor(stmt.columnString(ScanHeaderColIdx.precursorList)) );
			}
			
			//System.out.println( (double) (System.nanoTime() - nano) / 1e3 );

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

			// First pass to load the index
			//final SQLiteStatement fakeStmt = connection.prepare(_ms1ScanHeaderQueryStr, true);
			//while (fakeStmt.step()) {}
			//fakeStmt.dispose();

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
	public Map<Long, ScanHeader> getMs1ScanHeaderById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.ms1ScanHeaderById != null) {
			return this.entityCache.ms1ScanHeaderById;
		} else {
			ScanHeader[] ms1ScanHeaders = this.getMs1ScanHeaders();

			Map<Long, ScanHeader> ms1ScanHeaderById = new HashMap<Long, ScanHeader>(ms1ScanHeaders.length);

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
	public Map<Long, ScanHeader> getMs2ScanHeaderById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.ms2ScanHeaderById != null) {
			return this.entityCache.ms2ScanHeaderById;
		} else {
			ScanHeader[] ms2ScanHeaders = this.getMs2ScanHeaders();

			Map<Long, ScanHeader> ms2ScanHeaderById = new HashMap<Long, ScanHeader>(ms2ScanHeaders.length);

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
	public ScanHeader getScanHeader(long id) throws SQLiteException {
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
	public Map<Long, Float> getScanTimeById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.scanTimeById != null) {
			return this.entityCache.scanTimeById;
		} else {
			int scansCount = this.mzDbReader.getScansCount();
			float[] scanTimes = new SQLiteQuery(connection, "SELECT time FROM spectrum").extractFloats(scansCount);

			Map<Long, Float> scanTimeById = new HashMap<Long, Float>(scansCount);

			// TODO: check this approach is not too dangerous
			// FIXME: load the both values in the SQL query
			long scanId = 0;
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
			Map<Integer, ArrayList<Long>> scanIdsByTimeIndex = this.getScanIdsByTimeIndex(msLevel);
			
			int timeIndex = (int) (time / TIME_INDEX_WIDTH);
			ScanHeader nearestScanHeader = null;			

			for (int index = timeIndex - 1; index <= timeIndex + 1; index++) {

				if (scanIdsByTimeIndex.containsKey(index) == false) {
					continue;
				}

				ArrayList<Long> tmpScanIds = scanIdsByTimeIndex.get(index);
				for (Long tmpScanId : tmpScanIds) {

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
	protected Map<Integer, ArrayList<Long>> getScanIdsByTimeIndex(int msLevel) throws SQLiteException {

		HashMap<Integer, ArrayList<Long>> scanIdsByTimeIndex = null;
		if (this.entityCache != null) {
			
			if( msLevel == 1 && this.entityCache.ms1ScanIdsByTimeIndex != null) {
				scanIdsByTimeIndex = (HashMap<Integer, ArrayList<Long>>) this.entityCache.ms1ScanIdsByTimeIndex;
			}
			else if ( msLevel == 2 & this.entityCache.ms1ScanIdsByTimeIndex != null) {
				scanIdsByTimeIndex = (HashMap<Integer, ArrayList<Long>>) this.entityCache.ms2ScanIdsByTimeIndex;
			}
			
		}
		
		if(scanIdsByTimeIndex != null) return scanIdsByTimeIndex;
		else {
			scanIdsByTimeIndex = new HashMap<Integer, ArrayList<Long>>();
			
			ScanHeader[] scanHeaders;
			if( msLevel == 1) scanHeaders = this.getMs1ScanHeaders();
			else if ( msLevel == 2 ) scanHeaders = this.getMs2ScanHeaders();
			else return null;

			for (ScanHeader scanH : scanHeaders) {
				int timeIndex = (int) (scanH.getTime() / TIME_INDEX_WIDTH);

				if (scanIdsByTimeIndex.get(timeIndex) == null)
					scanIdsByTimeIndex.put(timeIndex, new ArrayList<Long>());

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
	public long[] getScanIdsForTimeRange(float minRT, float maxRT, int msLevel) throws SQLiteException {

		// TODO: use entity cache ?		
		SQLiteQuery query = new SQLiteQuery(connection, "SELECT id FROM scan WHERE ms_level = ? AND time >= ? AND time <= ?");
		return query.bind(1, msLevel).bind(2, minRT).bind(3, maxRT).extractLongs(1);
	}

}
