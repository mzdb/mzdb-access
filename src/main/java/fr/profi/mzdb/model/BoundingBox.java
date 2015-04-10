package fr.profi.mzdb.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.io.reader.bb.IBlobReader;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.ScanSlice;

/**
 * The Class BoundingBox.
 * 
 * @author Marc Dubois
 */
public class BoundingBox implements Comparable<BoundingBox> {

	/** The _id. */
	private int _id;

	/** The _first scan id. */
	protected int _firstScanId;

	protected int _lastScanId;

	/** The _run slice id. */
	protected int _runSliceId;

	/** The _ms level. */
	protected int _msLevel;

	/** The _data mode. */
	protected DataMode _dataMode;

	/** The _reader. */
	protected IBlobReader _reader;

	/**
	 * Instantiates a new bounding box.
	 * 
	 * @param id the BoundingBox id
	 * @param _reader a IBlobReader instance
	 */
	public BoundingBox(int id, IBlobReader _reader) {
		_id = id;
		this._reader = _reader;
	}

	/**
	 * Gets the id.
	 * 
	 * @return the id
	 */
	public int getId() {
		return _id;
	}

	/**
	 * Sets the id.
	 * 
	 * @param _id the new id
	 */
	public void setId(int _id) {
		this._id = _id;
	}

	/**
	 * Gets the reader.
	 * 
	 * @return the reader
	 */
	public IBlobReader getReader() {
		return _reader;
	}

	/**
	 * Gets the first scan id.
	 * 
	 * @return the first scan id
	 */
	public int getFirstScanId() {
		return _firstScanId;
	}

	/**
	 * Sets the first scan id.
	 * 
	 * @param scanid the new first scan id
	 */
	public void setFirstScanId(int scanid) {
		_firstScanId = scanid;
	}

	public int getLastScanId() {
		return _lastScanId;
	}

	public void setLastScanId(int i) {
		_lastScanId = i;
	}

	/**
	 * Gets the run slice id.
	 * 
	 * @return the run slice id
	 */
	public int getRunSliceId() {
		return _runSliceId;
	}

	/**
	 * Sets the run slice id.
	 * 
	 * @param _runSliceId the new run slice id
	 */
	public void setRunSliceId(int _runSliceId) {
		this._runSliceId = _runSliceId;
	}

	/**
	 * Scans count.
	 * 
	 * @return the int
	 */
	public int getScansCount() {
		return _reader.getScansCount();
	}

	/**
	 * Min id.
	 * 
	 * @return the float
	 */
	public float getMinScanId() throws SQLiteException {
		return this._reader.getScanIdAt(0);
	}

	/**
	 * Max id.
	 * 
	 * @return the float
	 */
	public float getMaxScanId() {
		return this._reader.getScanIdAt(this.getScansCount() - 1);
	}

	/**
	 * As scan slices array.
	 * 
	 * @param firstScanID the first scan id
	 * @param runSliceID the run slice id
	 * @return the scan slice[]
	 */
	public ScanSlice[] toScanSlices() {
	  
		// FIXME: remove this workaround when raw2mzDB has been fixed
		// raw2mzDB is inserting multiple empty spectrum slices pointing to the same spectrum id
		// Workaround added the 22/01/2015 by DBO
		HashSet<Integer> scanIdSet = new HashSet<Integer>();
		
		List<ScanSlice> scanSliceList = new ArrayList<ScanSlice>();
		for (ScanSlice scanSlice : _reader.readAllScanSlices(this._runSliceId)) {

			int scanId = scanSlice.getHeader().getId();

			// if( scanSlice.getData().getMzList().length > 0 ) {
			if (scanIdSet.contains(scanId) == false) {
				scanSliceList.add(scanSlice);
				scanIdSet.add(scanId);
			}
		}

		return scanSliceList.toArray(new ScanSlice[scanSliceList.size()]);
		// return _reader.readAllScanSlices( this._runSliceId );
	}

	/**
	 * Scan slice of scan at.
	 * 
	 * @param idx the idx
	 * @return the scan slice
	 */
	/*
	 * public ScanSlice scanSliceOfScanAt(int idx) { return _reader.scanSliceOfScanAt(idx); }
	 */

	/*
	 * public ByteBuffer getByteBuffer() { return this._reader.getByteBuffer(); }
	 */

	/*
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(BoundingBox bb) {

		try {
			if (this.getMinScanId() < bb.getMinScanId()) {
				return -1;
			} else if (Math.abs(this.getMinScanId() - bb.getMinScanId()) == 0) {
				return 0;
			}
		} catch (SQLiteException e) {
			e.printStackTrace();
		}

		return 1;
	}

}
