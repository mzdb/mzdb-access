/**
 * This file is part of the mzDB project
 */
package fr.profi.mzdb.io.reader.bb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.PeakEncoding;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;

/**
 * @author marco This implementation is mainly used is mzDbReader
 *         <p>
 *         Use a ByteBuffer to store the blob's bytes This class extends AbstractBlobReader
 *         </p>
 */
public class BytesReader extends AbstractBlobReader {

    /** the data */
    protected ByteBuffer _data;

    /** size of the data */
    protected int _dataSize;

    /**
     * Constructor
     * 
     * @param dataEncodings
     *            , DataEncoding object for each scan, usually given by a mzDbReaderInstance
     * @param data
     *            , array of byte of the blob
     * @see MzDbReader
     * @see DataEncoding
     */
    public BytesReader(Map<Integer, ScanHeader> headers, Map<Integer, DataEncoding> dataEncodings, byte[] data) {
	super(headers, dataEncodings);
	_data = ByteBuffer.wrap(data);
	_data.order(ByteOrder.LITTLE_ENDIAN);
	_dataSize = data.length;
	this._buildMapPositions();
    }

    /**
     * @see AbstractBlobReader
     * @see AbstractBlobReader._buildMpaPositions()
     */
    @Override
    public void _buildMapPositions() {
	int count = 1;
	int i = 0;
	_startPositions = new HashMap<Integer, Integer>();
	_nbPeaks = new HashMap<Integer, Integer>();
	while (i < _dataSize) {
	    _startPositions.put(count, i);
	    int id = _data.getInt(i);

	    i += 4; // skip id;

	    int nbPeaks = _data.getInt(i); // nbPeaks
	    _nbPeaks.put(count, nbPeaks);

	    i += 4; // skip nbPeaks

	    DataEncoding de = this._dataEncodings.get(id);
	    if (de == null) {
		System.out.println("Scared that the mzdb file is corrupted, id while reading: " + id);
		System.exit(0);
	    }
	    
	    int structSize = de.getPeakEncoding().getValue();
	    if (de.getMode() == DataMode.FITTED)
		structSize += 8; // add 2 float lwhm, rwhm
	    i += nbPeaks * structSize; // skip nbPeaks * size of one peak
	    count++;
	}
	_nbScans = count - 1; // removing the last count++ better than doing a if
	// statement inside a while loop
    }

    /**
     * @see IBlobReader#disposeBlob()
     */
    public void disposeBlob() {
    }

    /**
     * @see IBlobReader#blobSize()
     */
    public int blobSize() {
	return _dataSize;
    }

    /**
     * @see IBlobReader#nbScans()
     */
    public int nbScans() {
	return _nbScans;
    }

    /**
     * @see IBlobReader#idOfScanAt(int)
     */
    public int idOfScanAt(int i) {
	int j = _startPositions.get(i);
	return _data.getInt(j);
    }

    /**
     * @see IBlobReader#nbPeaksOfScanAt(int)
     */
    public int nbPeaksOfScanAt(int i) {
	if (i > _nbScans || i < 1) {
	    throw new IndexOutOfBoundsException("peaksOfScanAt Out of bounds, index counting starts at 1");
	}
	return _nbPeaks.get(i);
    }

    /**
     * peaksBytes corresponds here to the entire blob
     */
    public BlobData readBlob(int peaksBytesLength, int structSize, int pos, DataEncoding de) {
	DataMode dataMode = de.getMode();
	PeakEncoding pe = de.getPeakEncoding();
	int nbPeaks = peaksBytesLength / structSize;

	double[] mz = new double[nbPeaks];
	float[] intensity = new float[nbPeaks];
	float[] lwhm = new float[nbPeaks];
	float[] rwhm = new float[nbPeaks];

	int count = 0;

	switch (pe) {
	case HIGH_RES_PEAK:
	    for (int j = 0; j < peaksBytesLength; j += structSize) {
		int startPos = pos + j;
		mz[count] = _data.getDouble(startPos);
		intensity[count] = _data.getFloat(startPos + 8);
		if (de.getMode() == DataMode.FITTED) {
		    lwhm[count] = _data.getFloat(startPos + 12);
		    rwhm[count] = _data.getFloat(startPos + 16);
		}
		count++;
	    }
	    break;
	case LOW_RES_PEAK:
	    for (int j = 0; j < peaksBytesLength; j += structSize) {
		int startPos = pos + j;
		mz[count] = (double) _data.getFloat(startPos);
		intensity[count] = _data.getFloat(startPos + 4);
		if (dataMode == DataMode.FITTED) {
		    lwhm[count] = _data.getFloat(startPos + 8);
		    rwhm[count] = _data.getFloat(startPos + 12);
		}
		count++;
	    }
	    break;
	case NO_LOSS_PEAK:
	    for (int j = 0; j < peaksBytesLength; j += structSize) {
		int startPos = pos + j;
		mz[count] = _data.getDouble(startPos);
		intensity[count] = (float) _data.getDouble(startPos + 8);
		if (dataMode == DataMode.FITTED) {
		    lwhm[count] = _data.getFloat(startPos + 16);
		    rwhm[count] = _data.getFloat(startPos + 20);
		}
		count++;
	    }
	    break;
	}
	// return the newly formed blobData
	return new BlobData(mz, intensity, lwhm, rwhm);
    }

    /**
     * @see IBlobReader#peakAt(int, int)
     */
    public Peak peakAt(int idx, int pos) {
	if (idx > _nbScans || idx < 1) {
	    throw new IndexOutOfBoundsException("peakAt: Index out of bound start counting at 1");
	}
	int nbPeaks = this.nbPeaksOfScanAt(idx);
	if (pos > nbPeaks) {
	    throw new IndexOutOfBoundsException(
		    "peakAt: Index out of bound, peak wanted index superior at scan slice length");
	}
	Peak[] peaks = peaksOfScanAt(idx);
	return peaks[pos];
    }

    /**
     * @see IBlobReader#scanSliceOfScanAt(int)
     */
    public ScanSlice scanSliceOfScanAt(int idx) {
	if (idx > _nbScans || idx < 1)
	    throw new IndexOutOfBoundsException("scanSliceOfScanAt: Index out of bound start counting at 1");

	int pos = _startPositions.get(idx);
	int id = _data.getInt(pos);
	DataEncoding de = this._dataEncodings.get(id);

	int structSize = de.getPeakEncoding().getValue();
	if (de.getMode() == DataMode.FITTED)
	    structSize += 8;

	int length = _nbPeaks.get(idx) * structSize;
	pos += 8;

	BlobData blobData = readBlob(length, structSize, pos, de);

	ScanSlice s = new ScanSlice(_scanHeaders.get(id), new ScanData(blobData.mz, blobData.intensity,
		blobData.lwhm, blobData.rwhm));
	return s;
    }

    /**
     * @see IBlobReader#asScanSlicesArray(int, int)
     */
    public ScanSlice[] asScanSlicesArray(int firstScanId, int runSliceId) {

	ScanSlice[] sl = new ScanSlice[_nbScans];
	for (int i = 1; i <= _nbScans; i++) {
	    ScanSlice s = this.scanSliceOfScanAt(i);
	    s.setRunSliceId(runSliceId);
	    sl[i - 1] = s;
	}
	return sl;
    }

    @Override
    public ByteBuffer getByteBuffer() {
	return this._data;
    }
}
