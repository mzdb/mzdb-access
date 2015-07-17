package fr.profi.mzdb.io.reader.bb;

import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
		final SQLiteBlob blob,
		final long firstScanId,
		final long lastScanId,
		final Map<Long, ScanHeader> scanHeaderById,
		final Map<Long, DataEncoding> dataEncodingByScanId
	) throws StreamCorruptedException {
		super(firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId);
		
		this._blob = blob;
		this._indexScanSlices((int) (1 + lastScanId - firstScanId) );
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
	protected void _indexScanSlices(final int estimatedScansCount) throws StreamCorruptedException {

		final int[] scanSliceStartPositions = new int[estimatedScansCount];
		final int[] peaksCounts = new int[estimatedScansCount];
		
		final int size = getBlobSize();
		int scanSliceIdx = 0;
		int byteIdx = 0;

		while (byteIdx < size) {

			// Retrieve the scan id
			final long scanId = (long) _getIntFromBlob(_blob, byteIdx);
			_scanSliceStartPositions[scanSliceIdx] = byteIdx;
			// scanSliceStartPositions.add(byteIdx);

			// Skip the scan id bytes
			byteIdx += 4;

			// Retrieve the number of peaks
			final int peaksCount = _getIntFromBlob(_blob, byteIdx);
			_peaksCounts[scanSliceIdx] = peaksCount;
			// peaksCounts.add(byteIdx);

			// Skip the peaksCount bytes
			byteIdx += 4;

			// Retrieve the DataEncoding corresponding to this scan
			final DataEncoding de = this._dataEncodingByScanId.get(scanId);
			this.checkDataEncodingIsNotNull(de, scanId);

			byteIdx += peaksCount * de.getPeakStructSize(); // skip nbPeaks * size of one peak

			scanSliceIdx++;
		} // statement inside a while loop

		this._scansCount = scanSliceIdx;
		this._scanSliceStartPositions = Arrays.copyOf(scanSliceStartPositions, _scansCount);
		this._peaksCounts = Arrays.copyOf(peaksCounts, _scansCount);

		// this._scansCount = scanSliceStartPositions.size();
		// this._scanSliceStartPositions = intListToInts(scanSliceStartPositions, _scansCount);
		// this._peaksCounts = intListToInts(peaksCounts, _scansCount);
	}

	/**
	 * @see IBlobReader#idOfScanAt(int)
	 */
	public long getScanIdAt(final int idx) {
		this.checkScanIndexRange(idx);
		
		return _getScanIdAt(idx);
	}
	
	private long _getScanIdAt(final int idx) {
		return (long) _getIntFromBlob(_blob, idx);
	}
	
	private int _getIntFromBlob( final SQLiteBlob blob, final int idx ) {
		
		final byte[] byteBuffer = new byte[4];

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
	 * @see IBlobReader#readScanSliceAt(int)
	 */
	// TODO: factorize this code with the one from BytesReader
	public ScanSlice readScanSliceAt(final int idx) {
		final long scanId = _getScanIdAt(idx);
		final ScanData scanSliceData = this._readFilteredScanSliceDataAt(idx, scanId, -1.0, -1.0);
		final ScanHeader sh = _scanHeaderById.get( scanId );
		
		// Instantiate a new ScanSlice
		return new ScanSlice(sh, scanSliceData);
	}
	
	/**
	 * @see IBlobReader#readScanSliceAt(int)
	 */
	// TODO: factorize this code with the one from BytesReader
	public ScanData readScanSliceDataAt(final int idx) {
		return this._readFilteredScanSliceDataAt(idx, _getScanIdAt(idx), -1.0, -1.0 );
	}
	
	public ScanData readFilteredScanSliceDataAt(final int idx, final double minMz, final double maxMz) {
		return this._readFilteredScanSliceDataAt(idx, _getScanIdAt(idx), minMz, maxMz );		
	}

	/**
	 * @see IBlobReader#scanSliceOfScanAt(int)
	 */
	// TODO: factorize this code with the one from BytesReader
	private ScanData _readFilteredScanSliceDataAt(final int idx, final long scanId, final double minMz, final double maxMz) {
		
		// Determine peak size in bytes
		final DataEncoding de = this._dataEncodingByScanId.get(scanId);

		// Determine peaks bytes length
		final int peaksBytesSize = _peaksCounts[idx] * de.getPeakStructSize();
		
		// Skip scan id and peaks count (two integers)
		final int scanSliceStartPos = _scanSliceStartPositions[idx] + 8;

		final byte[] peaksBytes = new byte[peaksBytesSize];

		try {
			_blob.read(scanSliceStartPos, peaksBytes, 0, peaksBytesSize);
		} catch (SQLiteException e) {
			logger.error("can't read bytes from the SQLiteBlob",e);
		}

		// Instantiate a new ScanData for the corresponding scan slice
		return this.readScanSliceData(
			ByteBuffer.wrap(peaksBytes), scanSliceStartPos, peaksBytesSize, de, minMz, maxMz
		);
	}

}
