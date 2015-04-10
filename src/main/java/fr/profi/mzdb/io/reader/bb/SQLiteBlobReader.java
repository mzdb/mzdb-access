package fr.profi.mzdb.io.reader.bb;

import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteBlob;
import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.primitives.BytesUtils;

/**
 * Class for manipulating Blob in SQLite datafile using sqlite4java.SQLiteBlob
 * 
 * @author Marc Dubois
 * @see AbstractBlobReader
 * 
 */
public class SQLiteBlobReader extends AbstractBlobReader {

	/** SQLiteBlob Object */
	protected SQLiteBlob _blob;

	/**
	 * Constructor
	 * @throws StreamCorruptedException 
	 * 
	 * @see SQLiteBlob
	 * @see DataEncoding
	 */
	public SQLiteBlobReader(
		SQLiteBlob blob,
		int firstScanId,
		int lastScanId,
		Map<Integer, ScanHeader> scanHeaderById,
		Map<Integer, DataEncoding> dataEncodingByScanId
	) throws StreamCorruptedException {
		super(firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId);
		
		this._blob = blob;
		this._indexScanSlices();
	}

	/**
	 * @see IBlobReader#disposeBlob()
	 */
	public void disposeBlob() {
		_blob.dispose();
	}

	/**
	 * @see IBlobReader#getBlobSize()
	 */
	public int getBlobSize() {
		try {
			return _blob.getSize();
		} catch (SQLiteException e) {
			logger.error("can't get SQLiteBlob size",e);
			return 0;
		}
	}

	/**
	 * @see IBlobReader#getScansCount()
	 */
	public int getScansCount() {
		return _scansCount;
	}

	/**
	 * @throws StreamCorruptedException 
	 * @see AbstractBlobReader
	 * @see AbstractBlobReader#_buildMapPositions()
	 */
	// TODO: factorize this code with the one from BytesReader
	protected void _indexScanSlices() throws StreamCorruptedException {

		int size = getBlobSize();
		int scanSliceIdx = 1;
		int byteIdx = 0;
		
		this._scanSliceStartPositions = new int[_scansCount];
		this._peaksCounts = new int[_scansCount];
		
		while (byteIdx < size) {
			
			// Retrieve the scan id
			int scanId = _getIntFromBlob(_blob, byteIdx);
			_scanSliceStartPositions[scanSliceIdx] = byteIdx;
			
			// Skip the scan id bytes
			byteIdx += 4;

			// Retrieve the number of peaks
			int peaksCount = _getIntFromBlob(_blob, byteIdx);
			_peaksCounts[scanSliceIdx] = peaksCount;

			// Skip the peaksCount bytes
			byteIdx += 4;

			// Retrieve the DataEncoding corresponding to this scan
			DataEncoding de = this._dataEncodingByScanId.get(scanId);
			this.checkDataEncodingIsNotNull(de, scanId);
			
			byteIdx += peaksCount * de.getPeakStructSize(); // skip nbPeaks * size of one peak
			
			scanSliceIdx++;
		}
		
		// statement inside a while loop
	}

	/**
	 * @see IBlobReader#idOfScanAt(int)
	 */
	public int getScanIdAt(int idx) {
		this.checkScanIndexRange(idx);
		
		return _getIntFromBlob(_blob, idx);
	}
	
	private int _getIntFromBlob( SQLiteBlob blob, int idx ) {
		
		byte[] byteBuffer = new byte[4];

		try {
			blob.read(idx, byteBuffer, 0, 4);
		} catch (SQLiteException e) {
			logger.error("can't read bytes from the SQLiteBlob",e);
		} // read 4 bytes

		return BytesUtils.bytesToInt(byteBuffer, 0);
	}

	/**
	 * @see IBlobReader#nbPeaksOfScanAt(int)
	 */
	/*public int nbPeaksOfScanAt(int i) {
		if (i > _nbScans || i < 1) {
			throw new IndexOutOfBoundsException("nbPeaksOfScanAt: Index out of bound, starting counting at 1");
		}
		return _nbPeaks.get(i);
	}*/

	/**
	 * @see IBlobReader#peakAt(int, int)
	 */
	/*public Peak peakAt(int idx, int pos) {
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
	}*/

	/**
	 * @see IBlobReader#scanSliceOfScanAt(int)
	 */
	// TODO: factorize this code with the one from BytesReader
	public ScanSlice readScanSliceAt(int idx) {
		this.checkScanIndexRange(idx);

		int scanSliceStartPos = _scanSliceStartPositions[idx];
		int scanId = this._getIntFromBlob(_blob, scanSliceStartPos);
		
		// Determine peak size in bytes
		DataEncoding de = this._dataEncodingByScanId.get(scanId);

		// Determine peaks bytes length
		int peaksBytesSize = _peaksCounts[idx] * de.getPeakStructSize();
		
		// Skip scan id and peaks count (two integers)
		scanSliceStartPos += 8;

		byte[] peaksBytes = new byte[peaksBytesSize];

		try {
			_blob.read(scanSliceStartPos, peaksBytes, 0, peaksBytesSize);
		} catch (SQLiteException e) {
			logger.error("can't read bytes from the SQLiteBlob",e);
		}

		// Instantiate a new ScanData for the corresponding scan slice
		ScanData scanSliceData = this.readScanSliceData(
			ByteBuffer.wrap(peaksBytes), scanSliceStartPos, peaksBytesSize, de
		);

		// Instantiate a new ScanData
		return new ScanSlice(
			_scanHeaderById.get(scanId),
			scanSliceData
		);		
	}

	/**
	 * @see IBlobReader#asScanSlicesArray(int, int)
	 */
	/*public ScanSlice[] asScanSlicesArray(int firstScanId, int runSliceId) {
		ScanSlice[] sl = new ScanSlice[_scansCount];
		for (int i = 1; i <= _scansCount; i++) {
			ScanSlice s = this.scanSliceOfScanAt(i);
			s.setRunSliceId(runSliceId);
			sl[i - 1] = s;
		}
		return sl;
	}*/
}
