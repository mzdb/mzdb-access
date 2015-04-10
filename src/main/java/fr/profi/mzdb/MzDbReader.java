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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import fr.profi.mzdb.db.model.IMzDBParamNameGetter;
import fr.profi.mzdb.db.model.MzDBParamName_0_8;
import fr.profi.mzdb.db.model.MzDBParamName_0_9;
import fr.profi.mzdb.db.model.MzDbHeader;
import fr.profi.mzdb.db.model.params.ComponentList;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.model.params.Precursor;
import fr.profi.mzdb.db.model.params.ScanList;
import fr.profi.mzdb.db.model.params.param.CVEntry;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.table.BoundingBoxTable;
import fr.profi.mzdb.io.reader.DataEncodingReader;
import fr.profi.mzdb.io.reader.MzDbEntityCache;
import fr.profi.mzdb.io.reader.MzDbHeaderReader;
import fr.profi.mzdb.io.reader.ParamTreeParser;
import fr.profi.mzdb.io.reader.RunSliceHeaderReader;
import fr.profi.mzdb.io.reader.ScanHeaderReader;
import fr.profi.mzdb.io.reader.bb.BoundingBoxBuilder;
import fr.profi.mzdb.io.reader.bb.IBlobReader;
import fr.profi.mzdb.io.reader.iterator.BoundingBoxIterator;
import fr.profi.mzdb.io.reader.iterator.LcMsRunSliceIterator;
import fr.profi.mzdb.io.reader.iterator.LcMsnRunSliceIterator;
import fr.profi.mzdb.io.reader.iterator.MsScanIterator;
import fr.profi.mzdb.model.AcquisitionMode;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.IsolationWindow;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.RunSlice;
import fr.profi.mzdb.model.RunSliceData;
import fr.profi.mzdb.model.RunSliceHeader;
import fr.profi.mzdb.model.Scan;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;
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

	final Logger logger = LoggerFactory.getLogger(MzDbReader.class);

	/** The connection. */
	protected SQLiteConnection connection = null;

	/** The entity cache. */
	protected MzDbEntityCache entityCache = null;

	/**
	 * The is no loss mode. If no loss mode is enabed, all data points will be encoded as highres, i.e. 64
	 * bits mz and 64 bits int. No peak picking and not fitting will be performed on profile data.
	 */
	protected Boolean isNoLossMode;

	/** The _mz db header reader. */
	private MzDbHeaderReader _mzDbHeaderReader = null;

	// private InstrumentConfigReader _instrumentConfigReader = null;

	/** The _data encoding reader. */
	private DataEncodingReader _dataEncodingReader = null;

	/** The scan header reader. */
	private ScanHeaderReader _scanHeaderReader = null;

	/** The _run slice header reader. */
	private RunSliceHeaderReader _runSliceHeaderReader = null;

	private BBSizes _boundingBoxSizes = null;

	/** paramname getter */
	private IMzDBParamNameGetter _paramNameGetter = null;

	/** path of the sqlite file */
	protected String dbLocation = null;

	/**
	 * Acquisition mode: TODO: find a CV param representing the information better
	 */
	protected AcquisitionMode acquisitionMode = null;

	/**
	 * If swath acquisition, the list will be computed on first use (lazy loading) Will be always null on non
	 * swath acquisition
	 */
	protected IsolationWindow[] _diaIsolationWindows = null;

	/** The xml mappers. */
	public static Unmarshaller paramTreeUnmarshaller;
	public static Unmarshaller instrumentConfigUnmarshaller;
	public static Unmarshaller scanListUnmarshaller;
	public static Unmarshaller precursorUnmarshaller;

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
	 *             the sQ lite exception
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

		/** set the marshalling */
		try {
			MzDbReader.paramTreeUnmarshaller = JAXBContext.newInstance(ParamTree.class).createUnmarshaller();
			MzDbReader.instrumentConfigUnmarshaller = JAXBContext.newInstance(ComponentList.class)
					.createUnmarshaller();
			MzDbReader.scanListUnmarshaller = JAXBContext.newInstance(ScanList.class).createUnmarshaller();
			MzDbReader.precursorUnmarshaller = JAXBContext.newInstance(Precursor.class).createUnmarshaller();

		} catch (JAXBException e) {
			e.printStackTrace();
		}

		this._mzDbHeaderReader = new MzDbHeaderReader(connection);
		// this._instrumentConfigReader = new
		// InstrumentConfigReader(connection);
		this._dataEncodingReader = new DataEncodingReader(this);
		this._scanHeaderReader = new ScanHeaderReader(this);
		this._runSliceHeaderReader = new RunSliceHeaderReader(this);
		this._boundingBoxSizes = getBBSizes();
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
		return _mzDbHeaderReader.getMzDbHeader();
	}

	/**
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public String getSoftwareVersion() throws SQLiteException {
		String sqlString = "SELECT version FROM software WHERE name='mzDB'";
		return new SQLiteQuery(connection, sqlString).extractSingleString();

	}

	/**
	 * Lazy loading of the acquisition mode, parameter
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public AcquisitionMode getAcquisitionMode() throws SQLiteException {

		if (this.acquisitionMode == null) {
			final String sqlString = "SELECT param_tree FROM run";
			final String runParamTree = new SQLiteQuery(connection, sqlString).extractSingleString();
			final ParamTree runTree = ParamTreeParser.parseParamTree(runParamTree);

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

	/**
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public boolean isNoLossMode() throws SQLiteException {

		if (this.isNoLossMode == null) {
			MzDbHeader p = this._mzDbHeaderReader.getMzDbHeader();

			if (this._paramNameGetter == null) {
				String softVersion = this.getSoftwareVersion();
				this._paramNameGetter = (softVersion.compareTo("0.9.1") > 0) ? new MzDBParamName_0_9()
						: new MzDBParamName_0_8();
			}

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

		if (_boundingBoxSizes == null) {
			_boundingBoxSizes = new BBSizes();
			MzDbHeader header = null;
			try {
				header = _mzDbHeaderReader.getMzDbHeader();
			} catch (SQLiteException e) {
				e.printStackTrace();
			}

			String softVersion = this.getSoftwareVersion();
			this._paramNameGetter = (softVersion.compareTo("0.9.1") > 0) ? new MzDBParamName_0_9()
					: new MzDBParamName_0_8();
			this._setBBSizes(_boundingBoxSizes, this._paramNameGetter, header);

			// More robust but moire verbose and ugly
			/*
			 * try { MzDbReader._setBBSizes(_boundingBoxSizes, this._paramNameGetter, header); }
			 * catch(NullPointerException e) { try { this._paramNameGetter = new MzDBParamName_0_8();
			 * MzDbReader._setBBSizes(_boundingBoxSizes, this._paramNameGetter, header); }
			 * catch(NullPointerException e_) { this.logger.error(
			 * "Can not parse sizes of BoundingBox in mzDbHeader. This a fatal error" ); throw e_; } }
			 */
		}
		return _boundingBoxSizes;
	}
	
	/**
	 * @param bbSizes
	 * @param paramNameGetter
	 * @param header
	 */
	private void _setBBSizes(BBSizes bbSizes, IMzDBParamNameGetter paramNameGetter, MzDbHeader header) {
		bbSizes.BB_MZ_HEIGHT_MS1 = Double.parseDouble(
			header.getUserParam(paramNameGetter.getMs1BBMzWidthParamName()).getValue()
		);
		bbSizes.BB_MZ_HEIGHT_MSn = Double.parseDouble(
			header.getUserParam(paramNameGetter.getMsnBBMzWidthParamName()).getValue()
		);
		bbSizes.BB_RT_WIDTH_MS1 = Double.parseDouble(
			header.getUserParam(paramNameGetter.getMs1BBTimeWidthParamName()).getValue()
		);
		bbSizes.BB_RT_WIDTH_MSn = Double.parseDouble(
			header.getUserParam(paramNameGetter.getMs1BBTimeWidthParamName()).getValue()
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
	 * ImmutablePair can not be wrapped into an array
	 * 
	 * @return
	 * @throws SQLiteException
	 */
	public IsolationWindow[] getDIAIsolationWindows() throws SQLiteException {
		
		if (this._diaIsolationWindows == null) {
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
			
			_diaIsolationWindows = isolationWindowList.toArray(new IsolationWindow[isolationWindowList.size()]);
		}
		
		return _diaIsolationWindows;
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
	public Map<Integer, DataEncoding> getDataEncodingByScanId() throws SQLiteException {
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
	public DataEncoding getScanDataEncoding(int scanId) throws SQLiteException {
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
		Map<Integer, ScanHeader> scanHeaderById = this.getMs1ScanHeaderById();
		Map<Integer, DataEncoding> dataEncodingByScanId = this.getDataEncodingByScanId();

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
			
			bb.setFirstScanId(firstScanId);
			bb.setLastScanId(lastScanId);
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
	public int getBoundingBoxFirstScanId(int scanId) throws SQLiteException {
		String sqlString = "SELECT bb_first_spectrum_id FROM spectrum WHERE id = ?";
		return new SQLiteQuery(connection, sqlString).bind(1, scanId).extractSingleInt();
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
	public Map<Integer, ScanHeader> getMs1ScanHeaderById() throws SQLiteException {
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
	public Map<Integer, ScanHeader> getMs2ScanHeaderById() throws SQLiteException {
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
	public Map<Integer, ScanHeader> getScanHeaderById() throws SQLiteException {
		
		ScanHeader[] ms1ScanHeaders = this._scanHeaderReader.getMs1ScanHeaders();
		ScanHeader[] ms2ScanHeaders = this._scanHeaderReader.getMs2ScanHeaders();

		int scansCount = ms1ScanHeaders.length + ms2ScanHeaders.length;
		Map<Integer, ScanHeader> scanHeaderById = new HashMap<Integer, ScanHeader>(scansCount);

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
	public ScanHeader getScanHeader(int id) throws SQLiteException {
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
	public ScanData getScanData(int scanId) throws SQLiteException, StreamCorruptedException {

		// FIXME: getScanHeaderById
		Map<Integer, ScanHeader> scanHeaderById = this.getMs1ScanHeaderById();
		Map<Integer, DataEncoding> dataEncodingByScanId = this.getDataEncodingByScanId();

		// retrieve first scan index of the specified scanId better
		// than doing junction in sql query
		int firstScanId = this.getBoundingBoxFirstScanId(scanId);

		String sqlString = "SELECT * FROM bounding_box WHERE bounding_box.first_spectrum_id = ?";
		SQLiteRecordIterator records = new SQLiteQuery(connection, sqlString).bind(1, firstScanId)
				.getRecords();

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

			bb.setFirstScanId(firstScanId);
			bb.setLastScanId(lastScanId);
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

			int nbScans = bb.getScansCount();
			for (int scanIdx = 0; scanIdx < nbScans; scanIdx++) {
				if (scanId == bbReader.getScanIdAt(scanIdx)) {
					sd.addScanData(bbReader.readScanSliceAt(scanIdx).getData());
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
	public Scan getScan(int scanId) throws SQLiteException, StreamCorruptedException {
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

	private ArrayList<ScanSlice> _getNeighbouringScanSlices(
		double minmz,
		double maxmz,
		double minrt,
		double maxrt,
		int msLevel
	) throws SQLiteException, StreamCorruptedException {

		BBSizes sizes = getBBSizes();
		double rtWidth = (msLevel == 1) ? sizes.BB_RT_WIDTH_MS1 : sizes.BB_RT_WIDTH_MSn;
		double mzHeight = (msLevel == 1) ? sizes.BB_MZ_HEIGHT_MS1 : sizes.BB_MZ_HEIGHT_MSn;

		double _maxrt = maxrt + rtWidth;
		double _minmz = minmz - mzHeight;
		double _minrt = minrt - rtWidth;
		double _maxmz = maxmz + mzHeight;

		// TODO: query using bounding_box_msn_rtree to use the min_ms_level information
		String sqlQuery = "SELECT * FROM bounding_box WHERE id IN "
			+ "(SELECT id FROM bounding_box_rtree WHERE min_mz >= ? AND max_mz <= ? AND min_time >= ? AND max_time <= ? );";

		SQLiteRecordIterator records = new SQLiteQuery(connection, sqlQuery, false)
			.bind(1, _minmz)
			.bind(2, _maxmz)
			.bind(3, _minrt)
			.bind(4, _maxrt)
			.getRecords();

		Map<Integer, ScanHeader> scanHeaderById = null;
		if( msLevel == 1 ) scanHeaderById = this.getMs1ScanHeaderById();
		else if( msLevel == 2 ) scanHeaderById = this.getMs2ScanHeaderById();
		else throw new IllegalArgumentException("unsupported MS level: " + msLevel);
		
		Map<Integer, DataEncoding> dataEncodingByScanId = this.getDataEncodingByScanId();
		Map<Integer, ArrayList<BoundingBox>> bbsByFirstScanId = new TreeMap<Integer, ArrayList<BoundingBox>>();

		// retrieve bounding box
		while (records.hasNext()) {
			
			SQLiteRecord record = records.next();

			int bbId = record.columnInt(BoundingBoxTable.ID);

			// TODO: remove me when the query is performed using msn_rtree
			if (getBoundingBoxMsLevel(bbId) != msLevel)
				continue;

			// Retrieve bounding box data
			byte[] data = record.columnBlob(BoundingBoxTable.DATA);
			int firstScanId = record.columnInt(BoundingBoxTable.FIRST_SPECTRUM_ID);
			int lastScanId = record.columnInt(BoundingBoxTable.LAST_SPECTRUM_ID);

			// Build the Bounding Box
			BoundingBox bb = BoundingBoxBuilder.buildBB(
				bbId,
				data,
				firstScanId,
				lastScanId,
				scanHeaderById,
				dataEncodingByScanId
			);
			bb.setFirstScanId(firstScanId);
			bb.setLastScanId(lastScanId);
			bb.setRunSliceId(record.columnInt(BoundingBoxTable.RUN_SLICE_ID));

			// Initialize map entry if it doesn't exist
			if (bbsByFirstScanId.containsKey(firstScanId) == false)
				bbsByFirstScanId.put(firstScanId, new ArrayList<BoundingBox>());

			bbsByFirstScanId.get(firstScanId).add(bb);
		}

		ArrayList<ScanSlice> partialScanSlices = new ArrayList<ScanSlice>();

		for (ArrayList<BoundingBox> bbs: bbsByFirstScanId.values()) {

			if (bbs.size() == 0)
				continue;

			BoundingBox firstbb = bbs.get(0);
			IBlobReader firstbbReader = firstbb.getReader();
			int scanCount = firstbb.getScansCount();

			for (int scanIdx = 0; scanIdx < scanCount; scanIdx++) {
				
				ScanData partialScanData = new ScanData(new double[0],new float[0], new float[0], new float[0]);
				ScanSlice partialScan = new ScanSlice(
					scanHeaderById.get(firstbbReader.getScanIdAt(scanIdx)),
					partialScanData
				);
				// TODO: remove me ??? => it has no meaning here
				partialScan.setRunSliceId(firstbb.getRunSliceId());
				
				for (BoundingBox bb : bbs) {
					IBlobReader bbReader = bb.getReader();
					ScanSlice scanSlice = bbReader.readScanSliceAt(scanIdx);
					if (scanSlice.getData().getMzList().length > 0) {
						partialScanData.addScanData(scanSlice.getData());
					}
				}
				
				partialScanSlices.add(partialScan);
			}
		}
		
		return partialScanSlices;
	}

	/**
	 * Gets the scan slices. Each returned scan slice corresponds to a single scan.
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
	 * @return the scan slices
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException 
	 */
	public ScanSlice[] getScanSlices(
		double minmz,
		double maxmz,
		double minrt,
		double maxrt,
		int msLevel
	) throws SQLiteException, StreamCorruptedException {
		
		ArrayList<ScanSlice> scanSlices = _getNeighbouringScanSlices(minmz, maxmz, minrt, maxrt, msLevel);
		
		// System.out.println(scanSlices.length);
		if (scanSlices.size() == 0) {
			logger.warn("Empty scanSlices, too narrow request ?");
			return new ScanSlice[0];
		}
		
		ArrayList<ScanSlice> finalScanSlices = new ArrayList<ScanSlice>();
		
		int i = 1;
		float curElutionTime = scanSlices.get(0).getHeader().getElutionTime();
		while (i < scanSlices.size() && curElutionTime <= minrt) {
			curElutionTime = scanSlices.get(i).getHeader().getElutionTime();
			i++;
		}
		
		while (i < scanSlices.size() && curElutionTime <= maxrt) {
			
			// Retrieve current scan slice
			ScanSlice currentScanSlice = scanSlices.get(i);
			
			// Update current eluetion time and i
			curElutionTime = currentScanSlice.getHeader().getElutionTime();
			i++;
			
			// Filter m/z values to be sure we match the minmz/maxmz range
			ScanData filteredScanData = currentScanSlice.getData().mzRangeFilter(minmz, maxmz);
			
			if (filteredScanData == null) {
				continue;
			}
			
			ScanSlice finalScanSlice = new ScanSlice(currentScanSlice.getHeader(), filteredScanData);
			
			// TODO: remove me ??? => it has no meaning here
			finalScanSlice.setRunSliceId(currentScanSlice.getRunSliceId());
			
			finalScanSlices.add(finalScanSlice);
		}
		
		return finalScanSlices.toArray(new ScanSlice[finalScanSlices.size()]);
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
	 * Gets a run slice iterator.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return the run slice data iterator
	 * @throws SQLiteException
	 *             the sQ lite exception
	 * @throws StreamCorruptedException 
	 * @Deprecated Use getLcMsRunSliceIterator or getLcMsbRunSliceIterator methods instead
	 * 
	 * 	TODO: remove me
	 */
	@Deprecated
	public Iterator<RunSlice> getRunSliceIterator(int msLevel) throws SQLiteException, StreamCorruptedException {
		
		if( msLevel > 1 )
			throw new IllegalArgumentException("can only iterate on data of ms_level = 1");
		
		return this.getLcMsRunSliceIterator();
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

	public enum XicMethod {
		MAX(0), SUM(1);

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
		
		double minRtForRtree = minRt >= 0 ? minRt : 0;
		double maxRtForRtree = maxRt > 0 ? maxRt : this.getLastTime();
		
		// System.out.println(minRt+ "," + maxRt);
		ScanSlice[] scanSlices = getScanSlices(minMz, maxMz, minRtForRtree, maxRtForRtree, msLevel);
		
		if (scanSlices == null)
			logger.warn("null detected");// throw new
		
		// Exception("Empty scanSlices, narrow request ?");
		if (scanSlices.length == 0) {
			logger.warn("Empty scanSlices, narrow request ?");
			return new Peak[0];
		}

		List<Peak> results = new ArrayList<Peak>();
		switch (method) {
			case MAX: {
				
				for (ScanSlice sl : scanSlices) {
					Peak[] peaks = sl.getPeaks();
					
					if (peaks.length == 0)
						continue;
					
					Arrays.sort(peaks, Peak.getIntensityComp());
					
					results.add(peaks[peaks.length - 1]);
				}
				
				return results.toArray(new Peak[results.size()]);
			}
			case SUM: {
				for (ScanSlice sl : scanSlices) {
					Peak[] peaks = sl.getPeaks();
					
					if (peaks.length == 0)
						continue;
					
					Arrays.sort(peaks, Peak.getIntensityComp());
					
					float sum = 0.0f;
					for (Peak p : peaks) {
						sum += p.getIntensity();
					}
					
					Peak refPeak = peaks[(int) Math.floor(0.5 * peaks.length)];
					
					Peak fp = new Peak(
						refPeak.getMz(),
						sum,
						refPeak.getLeftHwhm(),
						refPeak.getRightHwhm(),
						refPeak.getLcContext()
					);
					
					results.add(fp);
				}
				
				return results.toArray(new Peak[results.size()]);
			}
			default: {
				logger.error("[getXIC]: method must be one of 'max' or 'sum', returning null");
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