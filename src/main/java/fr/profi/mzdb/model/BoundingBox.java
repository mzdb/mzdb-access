package fr.profi.mzdb.model;

import java.nio.ByteBuffer;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.io.reader.bb.IBlobReader;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.ScanSlice;

// TODO: Auto-generated Javadoc
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
     * @param id
     *            the id
     * @param _reader
     *            the _reader
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
     * @param _id
     *            the new id
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
     * @param scanid
     *            the new first scan id
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
     * @param _runSliceId
     *            the new run slice id
     */
    public void setRunSliceId(int _runSliceId) {
	this._runSliceId = _runSliceId;
    }

    // wraps _reader method;
    /**
     * Blob size.
     * 
     * @return the int
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public int blobSize() throws SQLiteException {
	return _reader.blobSize();
    }

    /**
     * Nb scans.
     * 
     * @return the int
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public int nbScans() {
	return _reader.nbScans();
    }

    /**
     * Min id.
     * 
     * @return the float
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public float minId() throws SQLiteException {
	return this.idOfScanAt(1);
    }

    /**
     * Max id.
     * 
     * @return the float
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public float maxId() throws SQLiteException {
	return this.idOfScanAt(this.nbScans());
    }

    /**
     * Id of scan at.
     * 
     * @param idx
     *            the idx
     * @return the float
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public int idOfScanAt(int idx) throws SQLiteException {
	return _reader.idOfScanAt(idx);
    }

    /**
     * Nb peaks of scan at.
     * 
     * @param idx
     *            the idx
     * @return the int
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public int nbPeaksOfScanAt(int idx) throws SQLiteException {
	return _reader.nbPeaksOfScanAt(idx);
    }

    /**
     * Peaks of scan at.
     * 
     * @param idx
     *            the idx
     * @return the peak[]
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public Peak[] peaksOfScanAt(int idx) throws SQLiteException {
	return _reader.peaksOfScanAt(idx);
    }

    /**
     * As scan slices array.
     * 
     * @param firstScanID
     *            the first scan id
     * @param runSliceID
     *            the run slice id
     * @return the scan slice[]
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public ScanSlice[] asScanSlicesArray() {
	return _reader.asScanSlicesArray(this._firstScanId, this._runSliceId);
    }

    /**
     * Scan slice of scan at.
     * 
     * @param idx
     *            the idx
     * @return the scan slice
     * @throws SQLiteException
     *             the sQ lite exception
     */
    public ScanSlice scanSliceOfScanAt(int idx) throws SQLiteException {
	return _reader.scanSliceOfScanAt(idx);
    }

    public ByteBuffer getByteBuffer() {
	return this._reader.getByteBuffer();
    }

    // @Override
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(BoundingBox bb) {

	try {
	    if (this.minId() < bb.minId()) {
		return -1;
	    } else if (Math.abs(this.minId() - bb.minId()) == 0) {
		return 0;
	    }
	} catch (SQLiteException e) {
	    e.printStackTrace();
	}

	return 1;
    }

}
