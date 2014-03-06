package fr.profi.mzdb;

import java.io.File;
import java.io.FileNotFoundException;
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
import fr.profi.mzdb.db.table.BoundingBoxTable;
import fr.profi.mzdb.io.reader.*;
import fr.profi.mzdb.io.reader.bb.BoundingBoxBuilder;
import fr.profi.mzdb.io.reader.iterator.BoundingBoxIterator;
import fr.profi.mzdb.io.reader.iterator.MsScanIterator;
import fr.profi.mzdb.io.reader.iterator.RunSliceIterator;
import fr.profi.mzdb.model.*;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;
import fr.profi.mzdb.utils.sqlite.SQLiteRecordIterator;

// TODO: Auto-generated Javadoc
/**
 * Allows to manipulates data contained in the mzDB file.
 * 
 * @author David
 */
public class MzDbReader {

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

	/** The is no loss mode. */
	protected Boolean isNoLossMode;

	/** The _mz db header reader. */
	private MzDbHeaderReader _mzDbHeaderReader = null;
	
	private InstrumentConfigReader _instrumentConfigReader = null;

	/** The _data encoding reader. */
	private DataEncodingReader _dataEncodingReader = null;

	/** The scan header reader. */
	private ScanHeaderReader _scanHeaderReader = null;

	/** The _run slice header reader. */
	private RunSliceHeaderReader _runSliceHeaderReader = null;

	private BBSizes _boundingBoxSizes = null;
	
	/**paramname getter */
	private IMzDBParamNameGetter _paramNameGetter = null;
	
	protected String dbLocation = null;

	/** The xml mapper. */
	
	public static Unmarshaller unmarshaller;
	public static Unmarshaller instrumentConfigUnmarshaller;

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
	public MzDbReader(File dbLocation, boolean cacheEntities, boolean logConnections)
			throws ClassNotFoundException, FileNotFoundException, SQLiteException {

		if (cacheEntities) {
			this.entityCache = new MzDbEntityCache();
		}

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
		connection.openReadonly();//(false);

		// SQLite optimization
		connection.exec("PRAGMA synchronous=OFF;");
		connection.exec("PRAGMA journal_mode=OFF;");
		connection.exec("PRAGMA temp_store=2;");
		connection.exec("PRAGMA cache_size=8000;");
		
		/**set the marshalling*/
    try {
      JAXBContext jaxbContext = JAXBContext.newInstance(ParamTree.class);
      MzDbReader.unmarshaller = jaxbContext.createUnmarshaller();
      JAXBContext jaxbContext_ = JAXBContext.newInstance(ComponentList.class);
      MzDbReader.instrumentConfigUnmarshaller = jaxbContext_.createUnmarshaller();

    } catch (JAXBException e) {
      e.printStackTrace();
    }
		
		this._mzDbHeaderReader = new MzDbHeaderReader(connection);
		this._instrumentConfigReader = new InstrumentConfigReader(connection);
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
		this(dbLocation, cacheEntities, false);
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
		this(new File(dbPath), cacheEntities, false);
	}
	
	public MzDbHeader getMzDbHeader() throws SQLiteException {
	  return _mzDbHeaderReader.getMzDbHeader();
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

	/**
	 * close the connection to avoid memory leaks.
	 */
	public void close() {
		connection.dispose();
	}
	
	public String getSoftwareVersion() throws SQLiteException {
	  String sqlString = "SELECT version FROM software WHERE name='mzDB'";
	  return new SQLiteQuery(connection, sqlString).extractSingleString();
	  
	}
	
	public boolean isNoLossMode() throws SQLiteException {

		if (this.isNoLossMode == null) {
			MzDbHeader p = this._mzDbHeaderReader.getMzDbHeader();
			
			//ugly workaround for the moment
			if ( this._paramNameGetter == null) {
			  String softVersion = this.getSoftwareVersion();
	      this._paramNameGetter = (softVersion.contains("0.9") ) ? new MzDBParamName_0_9() : new MzDBParamName_0_8(); 
			}
			
			if (p.getUserParam(this._paramNameGetter.getLossStateParamName()).getValue().equals("false"))
				this.isNoLossMode = false;
			else
				this.isNoLossMode = true;
		}

		return this.isNoLossMode;
	}
	
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
			this._paramNameGetter = (softVersion.contains("0.9") ) ? new MzDBParamName_0_9() : new MzDBParamName_0_8(); 
			 _boundingBoxSizes.BB_MZ_HEIGHT_MS1 = Double.parseDouble(header.getUserParam(this._paramNameGetter.getMs1BBMzWidthParamName()).getValue());
			_boundingBoxSizes.BB_MZ_HEIGHT_MSn = Double.parseDouble(header.getUserParam(this._paramNameGetter.getMsnBBMzWidthParamName()).getValue());
			_boundingBoxSizes.BB_RT_WIDTH_MS1 = Double.parseDouble(header.getUserParam(this._paramNameGetter.getMs1BBTimeWidthParamName()).getValue());
			_boundingBoxSizes.BB_RT_WIDTH_MSn = Double.parseDouble(header.getUserParam(this._paramNameGetter.getMs1BBTimeWidthParamName()).getValue());
		}
		return _boundingBoxSizes;
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

		SQLiteStatement stmt = connection
				.prepare("SELECT min(begin_mz), max(end_mz) FROM run_slice WHERE ms_level=?");
		stmt.bind(1, msLevel);
		stmt.step();

		int minMz = stmt.columnInt(0);
		int maxMz = stmt.columnInt(1);
		stmt.dispose();

		int[] mzRange = { minMz, maxMz };
		return mzRange;
	}

	/**
	 * _get table records count.
	 * 
	 * @param tableName
	 *            the table name
	 * @return the int
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public int getTableRecordsCount(String tableName) throws SQLiteException {
		return new SQLiteQuery(connection, "SELECT seq FROM sqlite_sequence WHERE name = ?").bind(1,
				tableName).extractSingleInt();
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
	public int getCyclesCount() throws SQLiteException {// SELECT MAX(cycle)
		// FROM scan
		String queryStr = "SELECT cycle FROM spectrum ORDER BY id DESC LIMIT 1";
		return new SQLiteQuery(connection, queryStr).extractSingleInt();
	}

	/**
	 * Cycle num to scan id.
	 * 
	 * @param cycleNumber
	 *            the cycle number
	 * @return the int
	 */
	public int cycleNumToScanId(int cycleNumber) {

		return 0;
	}

	/**
	 * Scan id to cycle num.
	 * 
	 * @param cycleNumber
	 *            the cycle number
	 * @return the int
	 */
	public int scanIdToCycleNum(int cycleNumber) {
		return 0;
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
	public RunSliceData getRunSliceData(int runSliceId) throws SQLiteException {

		// Retrieve the corresponding run slices
		String queryStr = "SELECT bounding_box.id, data, first_spectrum_id FROM bounding_box, bounding_box_rtree"
				+ " WHERE bounding_box.id = bounding_box_rtree.id AND bounding_box.run_slice_id = ?"
				+ " ORDER BY first_spectrum_id"; // number

		// SQLiteStatement stmt =
		// connection.prepare("SELECT * FROM run_slice WHERE ms_level="+msLevel+" ORDER BY begin_mz ",
		// false);//number ASC", false);
		SQLiteRecordIterator records = new SQLiteQuery(connection, queryStr).bind(1, runSliceId).getRecords();

		List<BoundingBox> bbs = new ArrayList<BoundingBox>();
		Map<Integer, DataEncoding> dataEncodingByScanId = this.getDataEncodingByScanId();

		while (records.hasNext()) {
			SQLiteRecord record = records.next();

			int id = record.columnInt(BoundingBoxTable.ID);
			byte[] data = record.columnBlob(BoundingBoxTable.DATA);
			int scanId = record.columnInt(BoundingBoxTable.FIRST_SPECTRUM_ID);
			// float minTime = (float) stmt.columnDouble(3);

			BoundingBox bb = BoundingBoxBuilder.buildBB(id, getScanHeaderById(), dataEncodingByScanId, data);
			bb.setFirstScanId(scanId);
			bbs.add(bb);
		}

		// TODO: check if faster than order by
		// Collections.sort(bbs); //sort bbs by their rt_min

		List<ScanSlice> scanList = new ArrayList<ScanSlice>();
		for (BoundingBox bb : bbs) {
			ScanSlice[] sl = bb.asScanSlicesArray();
			scanList.addAll(Arrays.asList(sl));
		}

		// rsd.buildPeakListByScanId();
		return new RunSliceData(runSliceId, scanList.toArray(new ScanSlice[scanList.size()]));
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

		String sqlString1 = "SELECT run_slice_id FROM bounding_box WHERE id = ?";
		int runSliceId = new SQLiteQuery(connection, sqlString1).bind(1, bbId).extractSingleInt();

		String sqlString2 = "SELECT ms_level FROM run_slice WHERE run_slice.id = ?";
		return new SQLiteQuery(connection, sqlString2).bind(1, runSliceId).extractSingleInt();
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
	 */
	public Peak[] getPeaks(double minmz, double maxmz, double minrt, double maxrt, int msLevel)
			throws SQLiteException {
		/*
		 * use get ScanSlices function then return a peak array using simply the toPeaks function
		 */
		ScanSlice[] r = this.getScanSlices(minmz, maxmz, minrt, maxrt, msLevel);

		this.logger.debug("ScanSliceLen : {}", r.length);

		if (r.length == 0)
			return new Peak[0];
		for (int i = 1; i < r.length; ++i) {
			r[0].getData().addScanData(r[i].getData());
		}
		return r[0].getPeaks();
	}

	/**
	 * Gets the scan headers.
	 * 
	 * @return the scan headers
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public ScanHeader[] getScanHeaders() throws SQLiteException {
		return this._scanHeaderReader.getScanHeaders();
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
	 * Gets the scan header by id.
	 * 
	 * @return the scan header by id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Map<Integer, ScanHeader> getScanHeaderById() throws SQLiteException {
		return this._scanHeaderReader.getScanHeaderById();
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
	 * Gets the scan id by time.
	 * 
	 * @return the scan id by time
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	// TODO: remove this method when mzDb is updated
	public Map<Float, Integer> getScanIdByTime() throws SQLiteException {
		return this._scanHeaderReader.getScanIdByTime();
	}

	/**
	 * Gets the scan data.
	 * 
	 * @param scanId
	 *            the scan id
	 * @return the scan data
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public ScanData getScanData(int scanId) throws SQLiteException {

		Map<Integer, DataEncoding> dataEnc = this.getDataEncodingByScanId();

		// retrieve first scan index of the specified scanId better
		// than doing junction in sql query
		int firstScanId = this.getBoundingBoxFirstScanId(scanId);

		String sqlString = "SELECT id, data, run_slice_id FROM bounding_box WHERE bounding_box.first_spectrum_id = ?";
		SQLiteRecordIterator records = new SQLiteQuery(connection, sqlString).bind(1, firstScanId)
				.getRecords();

		List<BoundingBox> bbS = new ArrayList<BoundingBox>();

		while (records.hasNext()) {
			SQLiteRecord r = records.next();

			BoundingBox bb = BoundingBoxBuilder.buildBB(r.columnInt(BoundingBoxTable.ID),
					getScanHeaderById(), dataEnc, r.columnBlob(BoundingBoxTable.DATA));

			bb.setRunSliceId(r.columnInt(BoundingBoxTable.RUN_SLICE_ID));
			bb.setFirstScanId(firstScanId);

			bbS.add(bb);
		}
		this.logger.debug("BBs count: {}", bbS.size());

		// Construct empty scan data
		ScanData sd = new ScanData(new double[0], new float[0]);

		// int firstScanCycle = getScanHeader(firstScanId).cycle;
		// int cycle = getScanHeader(scanId).cycle;
		// int cycleOffset = cycle - firstScanCycle;

		for (BoundingBox bb : bbS) {

			int nbScans = bb.nbScans();
			for (int j = 1; j <= nbScans; j++) {
				if (scanId == bb.idOfScanAt(j)) {
					sd.addScanData(bb.scanSliceOfScanAt(j).getData());
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
	 */
	public Scan getScan(int scanId) throws SQLiteException {
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
	 */
	public Peak[] getScanPeaks(int scanId) throws SQLiteException {
		return this.getScan(scanId).getPeaks();
	}

	private ScanSlice[] _getNeighbouringScanSlices(double minmz, double maxmz, double minrt, double maxrt,
			int msLevel) throws SQLiteException {
	   
		BBSizes sizes = getBBSizes();
		double rtWidth = (msLevel == 1) ? sizes.BB_RT_WIDTH_MS1 : sizes.BB_RT_WIDTH_MSn;
		double mzHeight = (msLevel == 1) ? sizes.BB_MZ_HEIGHT_MS1 : sizes.BB_MZ_HEIGHT_MSn;

		double _maxrt = maxrt + rtWidth;
		double _minmz = minmz - mzHeight;
		double _minrt = minrt - rtWidth;
		double _maxmz = maxmz + mzHeight;

		String sqlQuery = "SELECT bounding_box.id, data, run_slice_id, first_spectrum_id "
				+ "FROM bounding_box WHERE bounding_box.id " + "IN (SELECT id FROM bounding_box_rtree "
				+ "WHERE min_mz >= ? AND max_mz <= ? AND min_time >= ? AND max_time <= ? );";

		SQLiteStatement stmt = connection.prepare(sqlQuery, false);

		stmt.bind(1, _minmz);
		stmt.bind(2, _maxmz);
		stmt.bind(3, _minrt);
		stmt.bind(4, _maxrt);

		Map<Integer, ArrayList<BoundingBox>> BBs = new TreeMap<Integer, ArrayList<BoundingBox>>();

		// retrieve bounding box
		while (stmt.step()) {
			int id = stmt.columnInt(0);

			if (getBoundingBoxMsLevel(id) != msLevel)
				continue;

			// Retrieve bounding box data
			byte[] data = stmt.columnBlob(1);
			int runSliceId = stmt.columnInt(2);
			int firstScanId = stmt.columnInt(3);

			// Build the Bounding Box
			BoundingBox bb = BoundingBoxBuilder.buildBB(id, getScanHeaderById(), getDataEncodingByScanId(),
					data);
			bb.setFirstScanId(firstScanId);
			bb.setRunSliceId(runSliceId);

			// Initialize map entry if it doesn't exist
			if (BBs.containsKey(firstScanId) == false)
				BBs.put(firstScanId, new ArrayList<BoundingBox>());

			BBs.get(firstScanId).add(bb);
		}
		stmt.dispose();

		ArrayList<ScanSlice> sl = new ArrayList<ScanSlice>();

		for (ArrayList<BoundingBox> bbs : BBs.values()) {

			if (bbs.size() == 0)
				continue;

			BoundingBox firstbb = bbs.get(0);
			int scanCount = firstbb.nbScans();

			for (int i = 1; i <= scanCount; i++) {
				ScanSlice s = new ScanSlice(getScanHeader(firstbb.idOfScanAt(i)), new ScanData(new double[0],
						new float[0], new float[0], new float[0]));
				s.setRunSliceId(firstbb.getRunSliceId());
				for (BoundingBox bb : bbs) {
					ScanSlice _s = bb.scanSliceOfScanAt(i);
					if (_s.getData().getMzList().length > 0) {
						s.getData().addScanData(_s.getData());
					}
				}
				sl.add(s);
			}
		}
		return sl.toArray(new ScanSlice[sl.size()]);
	}

	/**
	 * Gets the scan slices.
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
	 */
	public ScanSlice[] getScanSlices(double minmz, double maxmz, double minrt, double maxrt, int msLevel)
			throws SQLiteException {
		ScanSlice[] scanSlices = _getNeighbouringScanSlices(minmz, maxmz, minrt, maxrt, msLevel);
		//System.out.println(scanSlices.length);
		if (scanSlices.length == 0) {
			logger.warn("Empty scanSlices, narrow request ?");
			return scanSlices;
		}
		ArrayList<ScanSlice> finalScanSlices = new ArrayList<ScanSlice>();
		Map<Integer, ScanHeader> headers = getScanHeaderById();
		int i = 1;
		float elt = headers.get(scanSlices[0].getScanId()).getElutionTime();
		while (i < scanSlices.length && elt < minrt) {
      elt = headers.get(scanSlices[i].getScanId()).getElutionTime();
			i++;
		}
		while (i < scanSlices.length && elt < maxrt) {
			// filter mz !
			ScanSlice currentScanSlice = scanSlices[i];
			int scanID = currentScanSlice.getScanId();
			ScanData d = currentScanSlice.getData().mzRangeFilter(minmz, maxmz);
			if (d == null) {
	      elt = headers.get(scanSlices[i].getScanId()).getElutionTime();
	      i++;
	      //if (i < scanSlices.length)
				continue;
			}
			ScanSlice f = new ScanSlice(getScanHeader(scanID), d);
			f.setRunSliceId(currentScanSlice.getRunSliceId());
			finalScanSlices.add(f);
			// update !
	    elt = headers.get(scanSlices[i].getScanId()).getElutionTime();
			i++;
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
	 */
	public Iterator<BoundingBox> getBoundingBoxIterator(int msLevel) throws SQLiteException {
		SQLiteStatement stmt = connection
				.prepare(
						"SELECT bounding_box.* FROM bounding_box, spectrum WHERE spectrum.id = bounding_box.first_spectrum_id AND spectrum.ms_level= ?",
						false);
		stmt.bind(1, msLevel);

		return new BoundingBoxIterator(this, stmt, this.getDataEncodingByScanId(), msLevel);
	}

	/**
	 * Gets the ms scan iterator.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return the ms scan iterator
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Iterator<Scan> getMsScanIterator(int msLevel) throws SQLiteException {
		return new MsScanIterator(this, msLevel);
	}

	/**
	 * Gets the run slice data iterator.
	 * 
	 * @param msLevel
	 *            the ms level
	 * @return the run slice data iterator
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Iterator<RunSlice> getRunSliceIterator(int msLevel) throws SQLiteException {

		// First pass to index data
		SQLiteStatement fakeStmt = connection.prepare("SELECT * FROM bounding_box", false);
		while (fakeStmt.step()) {
		}
		fakeStmt.dispose();

		return new RunSliceIterator(this, msLevel);
	}

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

	public Peak[] getXIC(double minMz, double maxMz, int msLevel, XicMethod method) throws SQLiteException {

		ScanHeader[] headers = getScanHeaders();
		if (headers == null || headers.length == 0) {
			logger.error("[getXIC]: Can not retrieve headers, returning null");
			return null;
		}
		double minRt = headers[0].getElutionTime();
		double maxRt = headers[headers.length - 1].getElutionTime();
		// System.out.println(minRt+ "," + maxRt);
		ScanSlice[] scanSlices = getScanSlices(minMz, maxMz, minRt, maxRt, msLevel);
		if (scanSlices == null)
		  logger.warn("null detected");//throw new Exception("Empty scanSlices, narrow request ?");
		if (scanSlices.length == 0) {
			logger.warn("Empty scanSlices, narrow request ?");
			return new Peak[0];
		}

		List<Peak> results = new ArrayList<Peak>();
		if (method == XicMethod.MAX) {
			for (ScanSlice sl : scanSlices) {
				Peak[] peaks = sl.getPeaks();
				if (peaks.length == 0)
					continue;
				Arrays.sort(peaks, Peak.getIntensityComp());
				results.add(peaks[peaks.length - 1]);
			}
			return results.toArray(new Peak[results.size()]);
		} else if (method == XicMethod.SUM) {
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
				Peak fp = new Peak(refPeak.getMz(), sum, refPeak.getLeftHwhm(), refPeak.getRightHwhm(),
						refPeak.getLcContext());
				results.add(fp);
			}
			return results.toArray(new Peak[results.size()]);
		} else {
			logger.error("[getXIC]: method must be one of 'max' or 'sum', returning null");
			return null;
		}

	}

}