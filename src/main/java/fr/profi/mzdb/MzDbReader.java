package fr.profi.mzdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.StreamCorruptedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.db.model.*;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.table.BoundingBoxTable;
import fr.profi.mzdb.io.reader.*;
import fr.profi.mzdb.io.reader.bb.BoundingBoxBuilder;
import fr.profi.mzdb.io.reader.bb.IBlobReader;
import fr.profi.mzdb.io.reader.iterator.BoundingBoxIterator;
import fr.profi.mzdb.io.reader.iterator.LcMsRunSliceIterator;
import fr.profi.mzdb.io.reader.iterator.LcMsnRunSliceIterator;
import fr.profi.mzdb.io.reader.iterator.MsScanIterator;
import fr.profi.mzdb.model.*;
import fr.profi.mzdb.utils.ms.MsUtils;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;
import fr.profi.mzdb.utils.sqlite.SQLiteRecordIterator;

/**
 * Allows to manipulates data contained in the mzDB file.
 * 
 * @author David
 */
public class MzDbReader {

	/**
	 * class holding bounding box dimensions that retrieved from the param_tree of the mzdb table. We
	 * distinguish two sizes, one for ms1, the other one for all msn
	 */
	public class BBSizes {
		public double BB_MZ_HEIGHT_MS1;
		public double BB_MZ_HEIGHT_MSn;
		public double BB_RT_WIDTH_MS1;
		public double BB_RT_WIDTH_MSn;
	};
	
	final protected BBSizes bbSizes = new BBSizes();
	
	final Logger logger = LoggerFactory.getLogger(MzDbReader.class);
	
	/** Some fields initialized in the constructor **/
	protected String dbLocation = null;
	protected SQLiteConnection connection = null;
	protected MzDbEntityCache entityCache = null;
	protected MzDbHeader mzDbHeader = null;
	protected IMzDBParamNameGetter _paramNameGetter = null;
	
	/** Some readers with internal entity cache **/
	private DataEncodingReader _dataEncodingReader = null;
	private ScanHeaderReader _scanHeaderReader = null;
	private RunSliceHeaderReader _runSliceHeaderReader = null;
	
	/** Some readers without internal entity cache **/
	private MzDbHeaderReader _mzDbHeaderReader = null;
	private InstrumentConfigReader _instrumentConfigReader = null;
	private RunReader _runReader = null;
	private SampleReader _sampleReader = null;
	private SoftwareReader _softwareListReader = null;
	private SourceFileReader _sourceFileReader = null;

	/**
	 * The is no loss mode. If no loss mode is enabled, all data points will be encoded as highres, i.e. 64
	 * bits mz and 64 bits int. No peak picking and not fitting will be performed on profile data.
	 */
	protected Boolean isNoLossMode;

	/**
	 * If swath acquisition, the list will be computed on first use (lazy loading) Will be always null on non
	 * swath acquisition
	 */
	protected IsolationWindow[] diaIsolationWindows = null;
	
	/** Define some lazy fields **/	
	// TODO: find a CV param representing the information better
	protected AcquisitionMode acquisitionMode = null;
	protected List<InstrumentConfiguration> instrumentConfigs = null;
	protected List<Run> runs = null;
	protected List<Sample> samples = null;
	protected List<Software> softwareList = null;
	protected List<SourceFile> sourceFiles = null;

	/**
	 * Instantiates a new mzDB reader (primary constructor). Builds a SQLite connection.
	 * 
	 * @param dbLocation
	 *            the db location
	 * @param cacheEntities
	 *            the cache entities
	 * @param logConnections
	 *            the log connections
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 * @throws FileNotFoundException
	 *             the file not found exception
	 * @throws SQLiteException
	 *             the SQLite exception
	 */
	public MzDbReader(File dbLocation, MzDbEntityCache entityCache, boolean logConnections)
			throws ClassNotFoundException, FileNotFoundException, SQLiteException {

		this.entityCache = entityCache;

		if (logConnections == false) {
			java.util.logging.Logger.getLogger("com.almworks.sqlite4java").setLevel(
					java.util.logging.Level.OFF);
		}

		// Check if database exists
		if (!dbLocation.exists()) {
			throw (new FileNotFoundException("can't find the mzDB file at the given path"));
		}

		this.dbLocation = dbLocation.getAbsolutePath();

		connection = new SQLiteConnection(dbLocation);
		connection.openReadonly();

		// SQLite optimization
		connection.exec("PRAGMA synchronous=OFF;");
		connection.exec("PRAGMA journal_mode=OFF;");
		connection.exec("PRAGMA temp_store=2;");
		connection.exec("PRAGMA cache_size=-100000;"); // around 100 Mo
		//connection.exec("PRAGMA mmap_size=3000000000"); // note: it may help for batch processing
		
		// Create a temporary table containing a copy of the sepctrum table		
		//System.out.println("before CREATE TEMP TABLE");
		//connection.exec("CREATE TEMP TABLE tmp_spectrum AS SELECT * FROM spectrum");
		//System.out.println("after CREATE TEMP TABLE");

		// Instantiates some readers without internal cache
		this._mzDbHeaderReader = new MzDbHeaderReader(connection);
		this._instrumentConfigReader = new InstrumentConfigReader(connection);
		this._runReader = new RunReader(connection);
		this._sampleReader = new SampleReader(connection);
		this._softwareListReader = new SoftwareReader(connection);
		this._sourceFileReader = new SourceFileReader(connection);
		
		// Instantiates some readers with internal cache (entity cache object)
		this._dataEncodingReader = new DataEncodingReader(this);
		this._scanHeaderReader = new ScanHeaderReader(this);
		this._runSliceHeaderReader = new RunSliceHeaderReader(this);
		
		// Set the mzDvHeader
		mzDbHeader = _mzDbHeaderReader.getMzDbHeader();
		
		// Set the paramNameGetter
		String pwizMzDbVersion = this.getPwizMzDbVersion();		
		this._paramNameGetter = (pwizMzDbVersion.compareTo("0.9.1") > 0) ? new MzDBParamName_0_9() : new MzDBParamName_0_8();
		
		// Set BB sizes
		this._setBBSizes(_paramNameGetter);
	}

	/**
	 * Instantiates a new mzDB reader (secondary constructor).
	 * 
	 * @param dbLocation
	 *            the db location
	 * @param cacheEntities
	 *            the cache entities
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 * @throws FileNotFoundException
	 *             the file not found exception
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public MzDbReader(File dbLocation, boolean cacheEntities) throws ClassNotFoundException,
			FileNotFoundException, SQLiteException {
	  this(dbLocation, cacheEntities ? new MzDbEntityCache() : null, false);		
	}

	/**
	 * Instantiates a new mzDB reader (secondary constructor).
	 * 
	 * @param dbPath
	 *            the db path
	 * @param cacheEntities
	 *            the cache entities
	 * @throws ClassNotFoundException
	 *             the class not found exception
	 * @throws FileNotFoundException
	 *             the file not found exception
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public MzDbReader(String dbPath, boolean cacheEntities) throws ClassNotFoundException,
			FileNotFoundException, SQLiteException {
		this(new File(dbPath), cacheEntities ? new MzDbEntityCache() : null, false);
	}
	
	/**
	 * Gets the connection.
	 * 
	 * @return the connection
	 */
	public SQLiteConnection getConnection() {
		return connection;
	}
	
	/**
	 * close the connection to avoid memory leaks.
	 */
	public void close() {
		connection.dispose();
	}

	/**
	 * Gets the entity cache.
	 * 
	 * @return the entity cache
	 */
	public MzDbEntityCache getEntityCache() {
		return entityCache;
	}

	public String getDbLocation() {
		return this.dbLocation;
	}
	
	public MzDbHeader getMzDbHeader() throws SQLiteException {
		return mzDbHeader;
	}
	
	/**
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public String getModelVersion() throws SQLiteException {
		String sqlString = "SELECT version FROM mzdb LIMIT 1";
		return new SQLiteQuery(connection, sqlString).extractSingleString();
	}

	/**
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	@Deprecated
	public String getSoftwareVersion() throws SQLiteException {
		return getPwizMzDbVersion();
	}
	
	public String getPwizMzDbVersion() throws SQLiteException {
		String sqlString = "SELECT version FROM software WHERE name LIKE '%mzDB'";
		return new SQLiteQuery(connection, sqlString).extractSingleString();
	}

	/**
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public boolean isNoLossMode() throws SQLiteException {

		if (this.isNoLossMode == null) {
			MzDbHeader p = this.getMzDbHeader();
			
			if (p.getUserParam(this._paramNameGetter.getLossStateParamName()).getValue().equals("false"))
				this.isNoLossMode = false;
			else
				this.isNoLossMode = true;
		}

		return this.isNoLossMode;
	}

	/**
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public BBSizes getBBSizes() throws SQLiteException {		
		return bbSizes;
	}
	
	/**
	 * @param bbSizes
	 * @param paramNameGetter
	 * @param header
	 */
	private void _setBBSizes(IMzDBParamNameGetter paramNameGetter) {
		bbSizes.BB_MZ_HEIGHT_MS1 = Double.parseDouble(
			mzDbHeader.getUserParam(paramNameGetter.getMs1BBMzWidthParamName()).getValue()
		);
		bbSizes.BB_MZ_HEIGHT_MSn = Double.parseDouble(
			mzDbHeader.getUserParam(paramNameGetter.getMsnBBMzWidthParamName()).getValue()
		);
		bbSizes.BB_RT_WIDTH_MS1 = Double.parseDouble(
			mzDbHeader.getUserParam(paramNameGetter.getMs1BBTimeWidthParamName()).getValue()
		);
		bbSizes.BB_RT_WIDTH_MSn = Double.parseDouble(
			mzDbHeader.getUserParam(paramNameGetter.getMs1BBTimeWidthParamName()).getValue()
		);
	}

	/**
	 * Gets the last time.
	 * 
	 * @return float the rt of the last scan
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public float getLastTime() throws SQLiteException {
		// Retrieve the number of scans
		String sqlString = "SELECT time FROM spectrum ORDER BY id DESC LIMIT 1";
		return (float) new SQLiteQuery(connection, sqlString).extractSingleDouble();
	}

	/**
	 * Gets the max ms level.
	 * 
	 * @return the max ms level
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getMaxMsLevel() throws SQLiteException {
		return new SQLiteQuery(connection, "SELECT max(ms_level) FROM run_slice").extractSingleInt();
	}

	/**
	 * Gets the mz range.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return runSlice min mz and runSlice max mz
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int[] getMzRange(int msLevel) throws SQLiteException {

		final SQLiteStatement stmt = connection
				.prepare("SELECT min(begin_mz), max(end_mz) FROM run_slice WHERE ms_level=?");
		stmt.bind(1, msLevel);
		stmt.step();

		final int minMz = stmt.columnInt(0);
		final int maxMz = stmt.columnInt(1);
		stmt.dispose();

		final int[] mzRange = { minMz, maxMz };
		return mzRange;
	}
	
	/**
	 * Gets the bounding box count.
	 * 
	 * @return int, the number of bounding box
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getBoundingBoxesCount() throws SQLiteException {
		return this.getTableRecordsCount("bounding_box");
	}

	/**
	 * Gets the bounding box count.
	 * 
	 * @param runSliceId
	 *            the run slice id
	 * @return the number of bounding box contained in the specified runSliceId
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getBoundingBoxesCount(int runSliceId) throws SQLiteException {
		String queryStr = "SELECT count(*) FROM bounding_box WHERE bounding_box.run_slice_id = ?";
		return new SQLiteQuery(connection, queryStr).bind(1, runSliceId).extractSingleInt();
	}
	
	/**
	 * Gets the cycle count.
	 * 
	 * @return the cycle count
	 * @throws SQLiteException
	 */
	public int getCyclesCount() throws SQLiteException {
		String queryStr = "SELECT cycle FROM spectrum ORDER BY id DESC LIMIT 1";
		return new SQLiteQuery(connection, queryStr).extractSingleInt();
	}

	/**
	 * Gets the data encoding count.
	 * 
	 * @return the data encoding count
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getDataEncodingsCount() throws SQLiteException {
		return this.getTableRecordsCount("data_encoding");
	}

	/**
	 * Gets the scan count.
	 * 
	 * @return int the number of scans
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getScansCount() throws SQLiteException {
		return this.getTableRecordsCount("spectrum");
	}
	
	/**
	 * Gets the scan count.
	 * 
	 * @return int the number of scans
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getScansCount(int msLevel) throws SQLiteException {
		String queryStr = "SELECT count(*) FROM spectrum WHERE ms_level = ?";
		return new SQLiteQuery(connection, queryStr).bind(1, msLevel).extractSingleInt();
	}

	/**
	 * Gets the run slice count.
	 * 
	 * @return int the number of runSlice
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getRunSlicesCount() throws SQLiteException {
		return this.getTableRecordsCount("run_slice");
	}
	
	/**
	 * Gets the table records count.
	 * 
	 * @param tableName
	 *            the table name
	 * @return the int
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getTableRecordsCount(String tableName) throws SQLiteException {
		return new SQLiteQuery(connection, "SELECT seq FROM sqlite_sequence WHERE name = ?")
			.bind(1,tableName).extractSingleInt();
	}
	
	/**
	 * ImmutablePair can not be wrapped into an array
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public IsolationWindow[] getDIAIsolationWindows() throws SQLiteException {
		
		if (this.diaIsolationWindows == null) {
			final String sqlQuery = "SELECT DISTINCT min_parent_mz, "
					+ "max_parent_mz FROM bounding_box_msn_rtree ORDER BY min_parent_mz";
			final SQLiteRecordIterator recordIt = new SQLiteQuery(connection, sqlQuery).getRecords();

			ArrayList<IsolationWindow> isolationWindowList = new ArrayList<IsolationWindow>();
			while (recordIt.hasNext()) {
				final SQLiteRecord record = recordIt.next();
				final Double minMz = record.columnDouble("min_parent_mz");
				final Double maxMz = record.columnDouble("max_parent_mz");
				isolationWindowList.add(new IsolationWindow(minMz, maxMz));
			}
			
			diaIsolationWindows = isolationWindowList.toArray(new IsolationWindow[isolationWindowList.size()]);
		}
		
		return diaIsolationWindows;
	}

	/**
	 * Gets the data encoding.
	 * 
	 * @param id
	 *            the id
	 * @return the data encoding
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public DataEncoding getDataEncoding(int id) throws SQLiteException {
		return this._dataEncodingReader.getDataEncoding(id);
	}

	/**
	 * Gets the data encoding by scan id.
	 * 
	 * @return the data encoding by scan id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Map<Long, DataEncoding> getDataEncodingByScanId() throws SQLiteException {
		return this._dataEncodingReader.getDataEncodingByScanId();
	}

	/**
	 * Gets the scan data encoding.
	 * 
	 * @param scanId
	 *            the scan id
	 * @return the scan data encoding
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public DataEncoding getScanDataEncoding(long scanId) throws SQLiteException {
		return this._dataEncodingReader.getScanDataEncoding(scanId);
	}

	/**
	 * Gets the run slices.
	 * 
	 * @return array of runSlice instance without data associated
	 * @throws SQLiteException
	 *             the SQLite exception
	 */
	public RunSliceHeader[] getRunSliceHeaders(int msLevel) throws SQLiteException {
		return this._runSliceHeaderReader.getRunSliceHeaders(msLevel);
	}

	/**
	 * Gets the run slice header by id.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return the run slice header by id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public HashMap<Integer, RunSliceHeader> getRunSliceHeaderById(int msLevel) throws SQLiteException {
		return this._runSliceHeaderReader.getRunSliceHeaderById(msLevel);
	}

	/**
	 * Gets the run slice data.
	 * 
	 * @param runSliceId
	 *            the run slice id
	 * @return the run slice data
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public RunSliceData getRunSliceData(int runSliceId) throws SQLiteException, StreamCorruptedException {

		// Retrieve the corresponding run slices
		// TODO: DBO => why a JOIN here ???
		//String queryStr = "SELECT bounding_box.* FROM bounding_box, bounding_box_rtree"
		//	+ " WHERE bounding_box.id = bounding_box_rtree.id AND bounding_box.run_slice_id = ?"
		//	+ " ORDER BY first_spectrum_id"; // number
		String queryStr = "SELECT * FROM bounding_box"
				+ " WHERE run_slice_id = ?"
				+ " ORDER BY first_spectrum_id"; // number

		// SQLiteStatement stmt =
		// connection.prepare("SELECT * FROM run_slice WHERE ms_level="+msLevel+" ORDER BY begin_mz ",
		// false);//number ASC", false);
		SQLiteRecordIterator records = new SQLiteQuery(connection, queryStr).bind(1, runSliceId).getRecords();

		List<BoundingBox> bbs = new ArrayList<BoundingBox>();
		// FIXME: getScanHeaderById
		Map<Long, ScanHeader> scanHeaderById = this.getMs1ScanHeaderById();
		Map<Long, DataEncoding> dataEncodingByScanId = this.getDataEncodingByScanId();

		while (records.hasNext()) {
			SQLiteRecord record = records.next();

			int bbId = record.columnInt(BoundingBoxTable.ID);
			byte[] data = record.columnBlob(BoundingBoxTable.DATA);
			int firstScanId = record.columnInt(BoundingBoxTable.FIRST_SPECTRUM_ID);
			int lastScanId = record.columnInt(BoundingBoxTable.LAST_SPECTRUM_ID);
			// float minTime = (float) stmt.columnDouble(3);

			BoundingBox bb = BoundingBoxBuilder.buildBB(
				bbId,
				data,
				firstScanId,
				lastScanId,
				scanHeaderById,
				dataEncodingByScanId
			);
			bb.setRunSliceId(runSliceId);
			
			bbs.add(bb);
		}

		// TODO: check if faster than order by
		// Collections.sort(bbs); //sort bbs by their rt_min

		List<ScanSlice> scanSliceList = new ArrayList<ScanSlice>();
		for (BoundingBox bb : bbs) {
			ScanSlice[] sl = bb.toScanSlices();
			for( ScanSlice ss : sl ) {
			  scanSliceList.add(ss);
			}
		}

		// rsd.buildPeakListByScanId();
		return new RunSliceData(runSliceId, scanSliceList.toArray(new ScanSlice[scanSliceList.size()]));
	}

	/**
	 * Gets the bounding box data.
	 * 
	 * @param bbId
	 *            the bb id
	 * @return the bounding box data
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public byte[] getBoundingBoxData(int bbId) throws SQLiteException {
		String sqlString = "SELECT data FROM bounding_box WHERE bounding_box.id = ?";
		return new SQLiteQuery(connection, sqlString).bind(1, bbId).extractSingleBlob();
	}

	/**
	 * Gets the bounding box first scan index.
	 * 
	 * @param scanId
	 *            the scan id
	 * @return the bounding box first scan index
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public long getBoundingBoxFirstScanId(long scanId) throws SQLiteException {
		String sqlString = "SELECT bb_first_spectrum_id FROM spectrum WHERE id = ?";
		return new SQLiteQuery(connection, sqlString).bind(1, scanId).extractSingleLong();
	}

	/**
	 * Gets the bounding box min mz.
	 * 
	 * @param bbId
	 *            the bb id
	 * @return the bounding box min mz
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public float getBoundingBoxMinMz(int bbId) throws SQLiteException {
		String sqlString = "SELECT min_mz FROM bounding_box_rtree WHERE bounding_box_rtree.id = ?";
		return (float) new SQLiteQuery(connection, sqlString).bind(1, bbId).extractSingleDouble();
	}

	/**
	 * Gets the bounding box min time.
	 * 
	 * @param bbId
	 *            the bb id
	 * @return the bounding box min time
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public float getBoundingBoxMinTime(int bbId) throws SQLiteException {
		String sqlString = "SELECT min_time FROM bounding_box_rtree WHERE bounding_box_rtree.id = ?";
		return (float) new SQLiteQuery(connection, sqlString).bind(1, bbId).extractSingleDouble();
	}

	/**
	 * Gets the bounding box ms level.
	 * 
	 * @param bbId
	 *            the bb id
	 * @return the bounding box ms level
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getBoundingBoxMsLevel(int bbId) throws SQLiteException {

		// FIXME: check that the mzDB file has the bounding_box_msn_rtree table
		String sqlString1 = "SELECT run_slice_id FROM bounding_box WHERE id = ?";
		int runSliceId = new SQLiteQuery(connection, sqlString1).bind(1, bbId).extractSingleInt();

		String sqlString2 = "SELECT ms_level FROM run_slice WHERE run_slice.id = ?";
		return new SQLiteQuery(connection, sqlString2).bind(1, runSliceId).extractSingleInt();
		
		/*
		String sqlString = "SELECT min_ms_level FROM bounding_box_msn_rtree WHERE bounding_box_msn_rtree.id = ?";
		return new SQLiteQuery(connection, sqlString).bind(1, bbId).extractSingleInt();
		*/
	}

	/**
	 * Gets the MS1 scan headers.
	 * 
	 * @return the scan headers
	 * @throws SQLiteException the SQLiteException
	 */
	public ScanHeader[] getMs1ScanHeaders() throws SQLiteException {
		return this._scanHeaderReader.getMs1ScanHeaders();
	}

	/**
	 * Gets the MS1 scan header by id.
	 * 
	 * @return the scan header by id
	 * @throws SQLiteException the SQLiteException
	 */
	public Map<Long, ScanHeader> getMs1ScanHeaderById() throws SQLiteException {
		return this._scanHeaderReader.getMs1ScanHeaderById();
	}
	
	/**
	 * Gets the MS2 scan headers.
	 * 
	 * @return the scan headers
	 * @throws SQLiteException the SQLiteException
	 */
	public ScanHeader[] getMs2ScanHeaders() throws SQLiteException {
		return this._scanHeaderReader.getMs2ScanHeaders();
	}

	/**
	 * Gets the MS2 scan header by id.
	 * 
	 * @return the scan header by id
	 * @throws SQLiteException the SQLiteException
	 */
	public Map<Long, ScanHeader> getMs2ScanHeaderById() throws SQLiteException {
		return this._scanHeaderReader.getMs2ScanHeaderById();
	}
	
	/**
	 * Gets all scan headers.
	 * 
	 * @return the scan headers
	 * @throws SQLiteException the SQLiteException
	 */
	public ScanHeader[] getScanHeaders() throws SQLiteException {
		ScanHeader[] ms1ScanHeaders = this._scanHeaderReader.getMs1ScanHeaders();
		ScanHeader[] ms2ScanHeaders = this._scanHeaderReader.getMs2ScanHeaders();
		
		ScanHeader[] scanHeaders = new ScanHeader[ ms1ScanHeaders.length + ms2ScanHeaders.length ];

	    System.arraycopy( ms1ScanHeaders, 0, scanHeaders, 0, ms1ScanHeaders.length );
	    System.arraycopy( ms2ScanHeaders, 0, scanHeaders, ms1ScanHeaders.length, ms2ScanHeaders.length );
	    
		return scanHeaders;
	}

	/**
	 * Gets each scan header mapped by its id.
	 * 
	 * @return the scan header by id
	 * @throws SQLiteException the SQLiteException
	 */
	public Map<Long, ScanHeader> getScanHeaderById() throws SQLiteException {
		
		ScanHeader[] ms1ScanHeaders = this._scanHeaderReader.getMs1ScanHeaders();
		ScanHeader[] ms2ScanHeaders = this._scanHeaderReader.getMs2ScanHeaders();

		int scansCount = ms1ScanHeaders.length + ms2ScanHeaders.length;
		Map<Long, ScanHeader> scanHeaderById = new HashMap<Long, ScanHeader>(scansCount);

		for (ScanHeader ms1ScanHeader : ms1ScanHeaders)
			scanHeaderById.put(ms1ScanHeader.getId(), ms1ScanHeader);
		
		for (ScanHeader ms2ScanHeader : ms2ScanHeaders)
			scanHeaderById.put(ms2ScanHeader.getId(), ms2ScanHeader);
		
		return scanHeaderById;
	}
	
	/**
	 * Gets the scan header.
	 * 
	 * @param id
	 *            the id
	 * @return the scan header
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public ScanHeader getScanHeader(long id) throws SQLiteException {
		return this._scanHeaderReader.getScanHeader(id);
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
		return this._scanHeaderReader.getScanHeaderForTime(time, msLevel);
	}

	/**
	 * Gets the scan data.
	 * 
	 * @param scanId
	 *            the scan id
	 * @return the scan data
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException 
	 */
	public ScanData getScanData(long scanId) throws SQLiteException, StreamCorruptedException {

		Map<Long, ScanHeader> scanHeaderById = this.getScanHeaderById();
		Map<Long, DataEncoding> dataEncodingByScanId = this.getDataEncodingByScanId();
		
		long firstScanId = scanHeaderById.get(scanId).getBBFirstSpectrumId();

		String sqlString = "SELECT * FROM bounding_box WHERE bounding_box.first_spectrum_id = ?";
		SQLiteRecordIterator records = new SQLiteQuery(connection, sqlString).bind(1, firstScanId).getRecords();

		List<BoundingBox> bbS = new ArrayList<BoundingBox>();

		while (records.hasNext()) {
			SQLiteRecord r = records.next();
			
			int lastScanId = r.columnInt(BoundingBoxTable.LAST_SPECTRUM_ID);

			BoundingBox bb = BoundingBoxBuilder.buildBB(
				r.columnInt(BoundingBoxTable.ID),
				r.columnBlob(BoundingBoxTable.DATA),
				firstScanId,
				lastScanId,
				scanHeaderById,
				dataEncodingByScanId
			);
			bb.setRunSliceId(r.columnInt(BoundingBoxTable.RUN_SLICE_ID));

			bbS.add(bb);
		}

		// Construct empty scan data
		ScanData sd = new ScanData(new double[0], new float[0]);

		// int firstScanCycle = getScanHeader(firstScanId).cycle;
		// int cycle = getScanHeader(scanId).cycle;
		// int cycleOffset = cycle - firstScanCycle;

		for (BoundingBox bb : bbS) {
			
			IBlobReader bbReader = bb.getReader();
			
			/*System.out.println("searching for " +scanId);
			ScanSlice[] ssList = bbReader.readAllScanSlices(bb.getRunSliceId());
			System.out.println("ssList.length: " +ssList.length);
			for( ScanSlice ss: ssList ) {
				System.out.println("has scan id=" +ss.getScanId());
			}*/

			// Retrieve only slices corresponding to the provided scan id
			int nbScans = bb.getScansCount();
			for (int scanIdx = 0; scanIdx < nbScans; scanIdx++) {
				if (scanId == bbReader.getScanIdAt(scanIdx)) {
					sd.addScanData(bbReader.readScanSliceDataAt(scanIdx));
					break;
				}
			}
		}

		return sd;
	}

	/**
	 * Gets the scan.
	 * 
	 * @param scanId
	 *            the scan id
	 * @return the scan
	 * @throws SQLiteException
	 *             the SQlite exception
	 * @throws StreamCorruptedException 
	 */
	public Scan getScan(long scanId) throws SQLiteException, StreamCorruptedException {
		ScanHeader sh = this.getScanHeader(scanId);
		ScanData sd = this.getScanData(scanId);
		return new Scan(sh, sd);
	}

	/**
	 * Gets the scan peaks.
	 * 
	 * @param scanId
	 *            the scan id
	 * @return the scan peaks
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException 
	 */
	public Peak[] getScanPeaks(int scanId) throws SQLiteException, StreamCorruptedException {
		return this.getScan(scanId).getPeaks();
	}

	/**
	 * Gets the scan slices. Each returned scan slice corresponds to a single scan.
	 * 
	 * @param minmz
	 *            the minMz
	 * @param maxmz
	 *            the maxMz
	 * @param minrt
	 *            the minRt
	 * @param maxrt
	 *            the maxRt
	 * @param msLevel
	 *            the ms level
	 * @return the scan slices
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException
	 * 
	 * @Deprecated Use getMsScanSlices or getMsnScanSlices methods instead
	 */
	@Deprecated
	public ScanSlice[] getScanSlices(
		double minMz,
		double maxMz,
		double minRt,
		double maxRt,
		int msLevel
	) throws SQLiteException, StreamCorruptedException {
		return _getScanSlicesInRanges(minMz, maxMz, minRt, maxRt, msLevel, 0.0);
	}
	
	/**
	 * Gets the scan slices. Each returned scan slice corresponds to a single scan.
	 * 
	 * @param minmz
	 *            the minMz
	 * @param maxmz
	 *            the maxMz
	 * @param minrt
	 *            the minRt
	 * @param maxrt
	 *            the maxRt
	 * @param msLevel
	 *            the ms level
	 * @return the scan slices
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException 
	 */
	public ScanSlice[] getMsScanSlices(
		double minMz,
		double maxMz,
		double minRt,
		double maxRt
	) throws SQLiteException, StreamCorruptedException {
		return _getScanSlicesInRanges(minMz, maxMz, minRt, maxRt, 1, 0.0);
	}
	
	// TODO: think about msLevel > 2
	public ScanSlice[] getMsnScanSlices(
		double parentMz,
		double minMz,
		double maxMz,
		double minRt,
		double maxRt
	) throws SQLiteException, StreamCorruptedException {
		return _getScanSlicesInRanges(minMz, maxMz, minRt, maxRt, 2, parentMz);
	}
	
	private ScanSlice[] _getScanSlicesInRanges(
		double minMz,
		double maxMz,
		double minRt,
		double maxRt,
		int msLevel,
		double parentMz
	) throws SQLiteException, StreamCorruptedException {
		
		BBSizes sizes = getBBSizes();
		double rtWidth = (msLevel == 1) ? sizes.BB_RT_WIDTH_MS1 : sizes.BB_RT_WIDTH_MSn;
		double mzHeight = (msLevel == 1) ? sizes.BB_MZ_HEIGHT_MS1 : sizes.BB_MZ_HEIGHT_MSn;

		double _maxRt = maxRt + rtWidth;
		double _minMz = minMz - mzHeight;
		double _minRt = minRt - rtWidth;
		double _maxMz = maxMz + mzHeight;

		// TODO: query using bounding_box_msn_rtree to use the min_ms_level information even for MS1 data ???
		SQLiteQuery sqliteQuery;
		if (msLevel == 1) {
			String sqlQuery = "SELECT * FROM bounding_box WHERE id IN "
			+ "(SELECT id FROM bounding_box_rtree WHERE min_mz >= ? AND max_mz <= ? AND min_time >= ? AND max_time <= ? )"
			+ " ORDER BY first_spectrum_id";
		
			sqliteQuery = new SQLiteQuery(connection, sqlQuery, false)
			.bind(1, _minMz)
			.bind(2, _maxMz)
			.bind(3, _minRt)
			.bind(4, _maxRt);
			
		} else {
			String sqlQuery = "SELECT * FROM bounding_box WHERE id IN "
					+ "(SELECT id FROM bounding_box_msn_rtree"
					+ " WHERE min_ms_level = " + msLevel + " AND max_ms_level = " + msLevel
					+ " AND min_parent_mz <= ? AND max_parent_mz >= ? "
					+ " AND min_mz >= ? AND max_mz <= ? AND min_time >= ? AND max_time <= ? )"
					+ " ORDER BY first_spectrum_id";
			
			sqliteQuery = new SQLiteQuery(connection, sqlQuery, false)
			.bind(1, parentMz)
			.bind(2, parentMz)
			.bind(3, _minMz)
			.bind(4, _maxMz)
			.bind(5, _minRt)
			.bind(6, _maxRt);
		}
		
		SQLiteRecordIterator recordIter = sqliteQuery.getRecords();

		Map<Long, ScanHeader> scanHeaderById = null;
		if( msLevel == 1 ) scanHeaderById = this.getMs1ScanHeaderById();
		else if( msLevel == 2 ) scanHeaderById = this.getMs2ScanHeaderById();
		else throw new IllegalArgumentException("unsupported MS level: " + msLevel);
		
		Map<Long, DataEncoding> dataEncodingByScanId = this.getDataEncodingByScanId();
		TreeMap<Long, ArrayList<ScanData>> scanDataListById = new TreeMap<Long, ArrayList<ScanData>>();
		HashMap<Long,Integer> peaksCountByScanId = new HashMap<Long, Integer>();

		// Iterate over bounding boxes
		while (recordIter.hasNext()) {
			
			SQLiteRecord record = recordIter.next();

			int bbId = record.columnInt(BoundingBoxTable.ID);

			// TODO: remove me when the query is performed using msn_rtree
			//if (getBoundingBoxMsLevel(bbId) != msLevel)
			//	continue;

			// Retrieve bounding box data
			byte[] data = record.columnBlob(BoundingBoxTable.DATA);
			long firstScanId = record.columnLong(BoundingBoxTable.FIRST_SPECTRUM_ID);
			long lastScanId = record.columnLong(BoundingBoxTable.LAST_SPECTRUM_ID);

			// Build the Bounding Box
			BoundingBox bb = BoundingBoxBuilder.buildBB(
				bbId,
				data,
				firstScanId,
				lastScanId,
				scanHeaderById,
				dataEncodingByScanId
			);
			//bb.setRunSliceId(record.columnInt(BoundingBoxTable.RUN_SLICE_ID));
			
			IBlobReader bbReader = bb.getReader();
			int bbScansCount = bbReader.getScansCount();
			long[] bbScanIds = bbReader.getAllScanIds();

			// Iterate over each scan
			for (int scanIdx = 0; scanIdx < bbScansCount; scanIdx++) {
				
				long scanId = bbScanIds[scanIdx];
				ScanHeader sh = scanHeaderById.get(scanId);
				float currentRt = sh.getElutionTime();
				
				// Filtering on time dimension
				if( currentRt >= minRt && currentRt <= maxRt ) {
					// Filtering on m/z dimension
					ScanData scanSliceData = bbReader.readFilteredScanSliceDataAt(scanIdx, minMz, maxMz);
					if (scanSliceData.isEmpty() == false) {
						if( scanDataListById.containsKey(scanId) == false ) {
							scanDataListById.put( scanId, new ArrayList<ScanData>() );
							peaksCountByScanId.put( scanId, 0 );
						}
						scanDataListById.get(scanId).add(scanSliceData);
						peaksCountByScanId.put( scanId, peaksCountByScanId.get(scanId) + scanSliceData.getPeaksCount() );
					}
				}
			}
		}
		
		ScanSlice[] finalScanSlices = new ScanSlice[scanDataListById.size()];
		
		int scanIdx = 0;
		for (Map.Entry<Long,ArrayList<ScanData>> entry : scanDataListById.entrySet() ) {
			Long scanId = entry.getKey();
			ArrayList<ScanData> scanDataList = entry.getValue();
			int peaksCount = peaksCountByScanId.get(scanId);
			
			double[] finalMzList = new double[peaksCount];
			float[] finalIntensityList = new float[peaksCount];
			float[] finalLeftHwhmList = null; 
			float[] finalRightHwhmList = null;
			
			ScanData firstScanData = scanDataList.get(0);
			if (firstScanData.getLeftHwhmList() != null && firstScanData.getRightHwhmList() != null) {
				finalLeftHwhmList = new float[peaksCount];
				finalRightHwhmList = new float[peaksCount];
			}
			
			// TODO: check that scanDataList is m/z sorted ???
			int finalPeakIdx = 0;
			for( ScanData scanData : scanDataList ) {
				double[] mzList = scanData.getMzList();
				float[] intensityList = scanData.getIntensityList();
				float[] leftHwhmList = scanData.getLeftHwhmList();
				float[] rightHwhmList = scanData.getRightHwhmList();
				
				// Add peaks of this ScanData to the final arrays
				int scanDataPeaksCount = scanData.getPeaksCount();
				for( int i = 0; i < scanDataPeaksCount; i++ ) {
					finalMzList[finalPeakIdx] = mzList[i];
					finalIntensityList[finalPeakIdx] = intensityList[i];
					
					if( finalLeftHwhmList != null && finalRightHwhmList != null ) {
						finalLeftHwhmList[finalPeakIdx] = leftHwhmList[i];
						finalRightHwhmList[finalPeakIdx] = rightHwhmList[i];
					}
				}
				
				finalPeakIdx++;
			}
			
			ScanData finalScanData = new ScanData(finalMzList,finalIntensityList, finalLeftHwhmList, finalRightHwhmList);
			finalScanSlices[scanIdx] = new ScanSlice(scanHeaderById.get(scanId), finalScanData);
			
			scanIdx++;
		}
		
		return finalScanSlices;
	}

	/**
	 * Gets the bounding box iterator.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return the bounding box iterator
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException 
	 */
	public Iterator<BoundingBox> getBoundingBoxIterator(int msLevel) throws SQLiteException, StreamCorruptedException {
		// TODO: try to use msn_rtree join instead (may be faster)
		SQLiteStatement stmt = connection.prepare(
			"SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level= ?",
			false
		);
		stmt.bind(1, msLevel);

		return new BoundingBoxIterator(this, stmt, msLevel);
	}

	/**
	 * Gets the ms scan iterator.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return the ms scan iterator
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException 
	 */
	public Iterator<Scan> getMsScanIterator(int msLevel) throws SQLiteException, StreamCorruptedException {
		return new MsScanIterator(this, msLevel);
	}
	
	/**
	 * Gets a RunSlice iterator.
	 * 
	 * @return the RunSlice iterator
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	public Iterator<RunSlice> getLcMsRunSliceIterator() throws SQLiteException, StreamCorruptedException {
		
		// First pass to load the index
		final SQLiteStatement fakeStmt = connection.prepare("SELECT * FROM bounding_box", false);
		while (fakeStmt.step()) {}
		fakeStmt.dispose();
		
		return new LcMsRunSliceIterator(this);
	}
	
	/**
	 * Gets a RunSlice iterator for a given m/z range
	 * 
	 * @param minRunSliceMz
	 * @param minRunSliceMz
	 * @return the RunSlice iterator
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	public Iterator<RunSlice> getLcMsRunSliceIterator(double minRunSliceMz, double maxRunSliceMz) throws SQLiteException, StreamCorruptedException {
		return new LcMsRunSliceIterator(this, minRunSliceMz, maxRunSliceMz);
	}

	/**
	 * Gets a DIA data RunSlice iterator
	 * 
	 * @param minParentMz
	 * @param maxParentMz
	 * @return the RunSlice iterator
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	public Iterator<RunSlice> getLcMsnRunSliceIterator(double minParentMz, double maxParentMz)
			throws SQLiteException, StreamCorruptedException {

		// First pass to load the index
		final SQLiteStatement fakeStmt = connection.prepare("SELECT * FROM bounding_box", false);
		while (fakeStmt.step()) {}
		fakeStmt.dispose();

		return new LcMsnRunSliceIterator(this, minParentMz, maxParentMz);
	}
	
	/**
	 * Gets a DIA data RunSlice iterator for a given m/z range
	 * 
	 * @param msLevel
	 * @param minParentMz
	 * @param maxParentMz
	 * @return the RunSlice iterator
	 * @throws SQLiteException
	 * @throws StreamCorruptedException 
	 */
	public Iterator<RunSlice> getLcMsnRunSliceIterator(
		double minParentMz,
		double maxParentMz,
		double minRunSliceMz,
		double maxRunSliceMz
	) throws SQLiteException, StreamCorruptedException {
		return new LcMsnRunSliceIterator(this, minParentMz, maxParentMz, minRunSliceMz, maxRunSliceMz);
	}
	
	/**
	 * Lazy loading of the acquisition mode, parameter
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public AcquisitionMode getAcquisitionMode() throws SQLiteException {

		if (this.acquisitionMode == null) {
			/*final String sqlString = "SELECT param_tree FROM run";
			final String runParamTree = new SQLiteQuery(connection, sqlString).extractSingleString();
			final ParamTree runTree = ParamTreeParser.parseParamTree(runParamTree);
			*/
			
			final ParamTree runTree = this.getRuns().get(0).getParamTree(this);

			try {
				final CVParam cvParam = runTree.getCVParam(CVEntry.ACQUISITION_PARAMETER);
				final String value = cvParam.getValue();
				this.acquisitionMode = AcquisitionMode.valueOf(value);
			} catch (Exception e) {
				this.acquisitionMode = AcquisitionMode.UNKNOWN;
			}
		}

		return this.acquisitionMode;
	}

	public List<InstrumentConfiguration> getInstrumentConfigurations() throws SQLiteException {
		if( instrumentConfigs == null ) {
			instrumentConfigs = this._instrumentConfigReader.getInstrumentConfigList();
		}
		return instrumentConfigs;
	}
	
	public List<Run> getRuns() throws SQLiteException {
		if( runs == null ) {
			runs = this._runReader.getRunList();
		}
		return runs;
	}
	
	public List<Sample> getSamples() throws SQLiteException {
		if( samples == null ) {
			samples = this._sampleReader.getSampleList();
		}
		return samples;
	}
	
	public List<Software> getSoftwareList() throws SQLiteException {
		if( softwareList == null ) {
			softwareList = this._softwareListReader.getSoftwareList();
		}
		return softwareList;
	}
	
	public List<SourceFile> getSourceFiles() throws SQLiteException {
		if( sourceFiles == null ) {
			sourceFiles = this._sourceFileReader.getSourceFileList();
		}
		return sourceFiles;
	}
	
	public String getFirstSourceFileName() throws SQLiteException {
		return this.getSourceFiles().get(0).getName();
		//String sqlString = "SELECT name FROM source_file LIMIT 1";
		//return new SQLiteQuery(connection, sqlString).extractSingleString();
	}

	public enum XicMethod {
		MAX(0), NEAREST(1), SUM(2);

		private final Integer val;

		private XicMethod(Integer val_) {
			val = val_;
		}

		public String toString() {
			return val.toString();
		}
	};

	/**
	 * Gets the xic.
	 * 
	 * @param minMz
	 *            the min mz
	 * @param maxMz
	 *            the max mz
	 * @param msLevel
	 *            the ms level
	 * @return the xic
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Peak[] getXIC(double minMz, double maxMz, int msLevel, XicMethod method) throws SQLiteException, StreamCorruptedException {
		return getXIC(minMz, maxMz, -1, -1, msLevel, method);
	}

	public Peak[] getXIC(double minMz, double maxMz, float minRt, float maxRt, int msLevel, XicMethod method)
			throws SQLiteException, StreamCorruptedException {

		final double mzCenter = (minMz + maxMz) / 2;
		final double mzTolInDa = maxMz - mzCenter;

		return getXICForMz(mzCenter, mzTolInDa, minRt, maxRt, msLevel, method);
	}
	
	public Peak[] getXICForMz(
		double mz,
		double mzTolInDa,
		float minRt,
		float maxRt,
		int msLevel,
		XicMethod method
	) throws SQLiteException, StreamCorruptedException {

		final double minMz = mz - mzTolInDa;
		final double maxMz = mz + mzTolInDa;
		final double minRtForRtree = minRt >= 0 ? minRt : 0;
		final double maxRtForRtree = maxRt > 0 ? maxRt : this.getLastTime();

		ScanSlice[] scanSlices = getMsScanSlices(minMz, maxMz, minRtForRtree, maxRtForRtree);
		
		final double mzTolPPM = MsUtils.DaToPPM(mz, mzTolInDa);
		return _scanSlicesToXIC(scanSlices, mz, mzTolPPM, method);
	}
	
	public Peak[] getMsnXIC(
		double parentMz,
		double fragmentMz,
		double fragmentMzTolInDa,
		float minRt,
		float maxRt,
		XicMethod method
	) throws SQLiteException, StreamCorruptedException {

		final double minFragMz = fragmentMz - fragmentMzTolInDa;
		final double maxFragMz = fragmentMz + fragmentMzTolInDa;
		final double minRtForRtree = minRt >= 0 ? minRt : 0;
		final double maxRtForRtree = maxRt > 0 ? maxRt : this.getLastTime();

		ScanSlice[] scanSlices = getMsnScanSlices(parentMz, minFragMz, maxFragMz, minRtForRtree, maxRtForRtree);
		
		final double fragMzTolPPM = MsUtils.DaToPPM(fragmentMz, fragmentMzTolInDa);
		return _scanSlicesToXIC(scanSlices, fragmentMz, fragMzTolPPM, method);
	}

	private Peak[] _scanSlicesToXIC(
		ScanSlice[] scanSlices,
		double searchedMz,
		double mzTolPPM,
		XicMethod method
	) throws SQLiteException, StreamCorruptedException {

		if (scanSlices == null)
			logger.warn("null detected");// throw new

		if (scanSlices.length == 0) {
			// logger.warn("Empty scanSlices, too narrow request ?");
			return new Peak[0];
		}

		int scanSlicesCount = scanSlices.length;
		List<Peak> xicPeaks = new ArrayList<Peak>(scanSlicesCount);

		switch (method) {
		case MAX: {

			for (int i = 0; i < scanSlicesCount; i++) {

				ScanSlice sl = scanSlices[i];

				Peak[] peaks = sl.getPeaks();
				int peaksCount = peaks.length;

				if (peaksCount == 0)
					continue;

				Arrays.sort(peaks, Peak.getIntensityComp());

				xicPeaks.add( peaks[peaksCount - 1] );
			}

			return xicPeaks.toArray(new Peak[xicPeaks.size()]);
		}
		case NEAREST: {
			
			for (int i = 0; i < scanSlicesCount; i++) {
				ScanSlice sl = scanSlices[i];
				ScanData slData = sl.getData();
				
				if (slData.isEmpty())
					continue;
				
				Peak nearestPeak = sl.getNearestPeak(searchedMz, mzTolPPM);
				
				if( nearestPeak == null ) {
					logger.error("nearest peak is null but should not be: searchedMz="+ searchedMz+" minMz="+slData.getMzList()[0] + " tol="+mzTolPPM);
					continue;
				}
				
				xicPeaks.add( nearestPeak );
			}
			
			return xicPeaks.toArray(new Peak[xicPeaks.size()]);
		}
		case SUM: {
			for (int i = 0; i < scanSlicesCount; i++) {

				ScanSlice sl = scanSlices[i];

				Peak[] peaks = sl.getPeaks();
				int peaksCount = peaks.length;

				if (peaksCount == 0)
					continue;

				Arrays.sort(peaks, Peak.getIntensityComp());

				float sum = 0.0f;
				for (Peak p : peaks) {
					sum += p.getIntensity();
				}

				Peak refPeak = peaks[(int) Math.floor(0.5 * peaksCount)];

				xicPeaks.add(
					new Peak(
						refPeak.getMz(),
						sum,
						refPeak.getLeftHwhm(),
						refPeak.getRightHwhm(),
						refPeak.getLcContext()
					)
				);
			}

			return xicPeaks.toArray(new Peak[xicPeaks.size()]);
		}
		default: {
			logger.error("[_scanSlicesToXIC]: method must be one of 'MAX', 'NEAREST' or 'SUM', returning null");
			return null;
		}
		}

	}

	/**
	 * Gets the peaks.
	 * 
	 * @param minmz
	 *            the minmz
	 * @param maxmz
	 *            the maxmz
	 * @param minrt
	 *            the minrt
	 * @param maxrt
	 *            the maxrt
	 * @param msLevel
	 *            the ms level
	 * @return the peaks
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException 
	 */
	// TODO: rename into getMsPeaks
	public Peak[] getPeaks(double minmz, double maxmz, double minrt, double maxrt, int msLevel)
			throws SQLiteException, StreamCorruptedException {
		/*
		 * use get ScanSlices function then return a peak array using simply the toPeaks function
		 */
		ScanSlice[] scanSlices = this.getScanSlices(minmz, maxmz, minrt, maxrt, msLevel);

		this.logger.debug("ScanSlice length : {}", scanSlices.length);

		if (scanSlices.length == 0)
			return new Peak[0];
		
		ScanSlice mergedScanSlice = scanSlices[0];
		ScanData mergedScanData = mergedScanSlice.getData();
		for (int i = 1; i < scanSlices.length; ++i) {
			mergedScanData.addScanData(scanSlices[i].getData());
		}
		
		return mergedScanSlice.getPeaks();
	}


}