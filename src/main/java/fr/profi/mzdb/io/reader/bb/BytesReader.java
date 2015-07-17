package fr.profi.mzdb.io.reader.bb;

import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;

/**
 * @author marco This implementation is mainly used is mzDbReader
 * <p>
 *   Use a ByteBuffer to store the blob's bytes This class extends AbstractBlobReader.
 * </p>
 */
public class BytesReader extends AbstractBlobReader {

	/** the data */
	protected ByteBuffer _bbByteBuffer;

	/** size of the Blob */
	protected int _blobSize;
	protected DataEncoding _firstDataEncondig;

	/**
	 * Constructor
	 * 
	 * @param dataEncodings, DataEncoding object for each scan, usually given by a mzDbReaderInstance
	 * @param data, array of byte of the blob
	 * @throws StreamCorruptedException 
	 * @see MzDbReader
	 * @see DataEncoding
	 */
	public BytesReader(
		final byte[] bytes,
		final long firstScanId,
		final long lastScanId,
		final Map<Long, ScanHeader> scanHeaderById,
		final Map<Long, DataEncoding> dataEncodingByScanId
	) throws StreamCorruptedException {
		super(firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId);
		
		this._bbByteBuffer = ByteBuffer.wrap(bytes);
		this._firstDataEncondig = dataEncodingByScanId.values().iterator().next();
		this._bbByteBuffer.order(_firstDataEncondig.getByteOrder());
		this._blobSize = bytes.length;
		
		//logger.debug("BytesReader: blobSize="+ _blobSize);
		
		this._indexScanSlices((int) (1 + lastScanId - firstScanId) );
	}

	/**
	 * Do a first parse of the blob to determine beginning index of each scan slice
	 * @throws StreamCorruptedException 
	 * 
	 * @see AbstractBlobReader
	 * @see AbstractBlobReader._buildMpaPositions()
	 */
	protected void _indexScanSlices(final int estimatedScansCount) throws StreamCorruptedException {
		
		final int[] scanSliceStartPositions = new int[estimatedScansCount];
		final int[] peaksCounts = new int[estimatedScansCount];
		
		int scanSliceIdx = 0;
		int byteIdx = 0;
		
		while (byteIdx < _blobSize) {
			
			// Set the new position to access the byte buffer
			_bbByteBuffer.position(byteIdx);
			
			// Retrieve the scan id
			final long scanId = (long) _bbByteBuffer.getInt();
			scanSliceStartPositions[scanSliceIdx] = byteIdx;
			//System.out.println("scan id is: "+scanId);
			
			// Retrieve the number of peaks
			final int peaksCount = _bbByteBuffer.getInt(); 
			peaksCounts[scanSliceIdx] = peaksCount;

			// Retrieve the DataEncoding corresponding to this scan
			final DataEncoding de = this._dataEncodingByScanId.get(scanId);
			this.checkDataEncodingIsNotNull(de, scanId);
			
			// Skip the scan id, peaksCount and peaks (peaksCount * size of one peak)
			byteIdx += 8 + (peaksCount * de.getPeakStructSize());
			
			scanSliceIdx++;
			
		} // statement inside a while loop
		
		this._scansCount = scanSliceIdx;
		this._scanSliceStartPositions = Arrays.copyOf(scanSliceStartPositions, _scansCount);
		this._peaksCounts = Arrays.copyOf(peaksCounts, _scansCount);
	}

	/**
	 * @see IBlobReader#disposeBlob()
	 */
	public void disposeBlob() {}

	/**
	 * @see IBlobReader#getBlobSize()
	 */
	public int getBlobSize() {
		return _blobSize;
	}

	/**
	 * @see IBlobReader#getScansCount()
	 */
	public int getScansCount() {
		return _scansCount;
	}

	/**
	 * @see IBlobReader#idOfScanAt(int)
	 */
	public long getScanIdAt(final int idx) {
		this.checkScanIndexRange(idx);
		return _getScanIdAt(idx);
	}
	
	private long _getScanIdAt(final int idx) {
		return (long) _bbByteBuffer.getInt(_scanSliceStartPositions[idx]);
	}

	/**
	 * @see IBlobReader#nbPeaksOfScanAt(int)
	 */
	/*public int nbPeaksOfScanAt(int idx) {
		if (idx < 0 || idx >= _scansCount) {
			throw new IndexOutOfBoundsException("nbPeaksOfScanAt: index out of bounds (i="+idx+"), index counting starts at 0");
		}
		
		return _peaksCounts[idx];
	}*/

	/**
	 * @see IBlobReader#readScanSliceAt(int)
	 */
	public ScanSlice readScanSliceAt(final int idx) {
		long scanId = _getScanIdAt(idx);
		ScanData scanSliceData = this._readFilteredScanSliceDataAt(idx, scanId, -1.0, -1.0 );
		ScanHeader sh = _scanHeaderById.get( scanId );
		
		// Instantiate a new ScanSlice
		return new ScanSlice(sh, scanSliceData);
	}
	
	/**
	 * @see IBlobReader#readScanSliceAt(int)
	 */
	public ScanData readScanSliceDataAt(final int idx) {
		return this._readFilteredScanSliceDataAt(idx, _getScanIdAt(idx), -1.0, -1.0 );		
	}
	
	public ScanData readFilteredScanSliceDataAt(final int idx, final double minMz, final double maxMz) {
		return this._readFilteredScanSliceDataAt(idx, _getScanIdAt(idx), minMz, maxMz );		
	}
	
	private ScanData _readFilteredScanSliceDataAt(final int idx, final long scanId, final double minMz, final double maxMz) {
		
		// Determine peak size in bytes
		final DataEncoding de = this._dataEncodingByScanId.get(scanId);

		// Determine peaks bytes length
		final int peaksBytesSize = _peaksCounts[idx] * de.getPeakStructSize();
		
		// Skip scan id and peaks count (two integers)
		final int scanSliceStartPos = _scanSliceStartPositions[idx] + 8;

		// Instantiate a new ScanData for the corresponding scan slice
		return this.readScanSliceData(_bbByteBuffer, scanSliceStartPos, peaksBytesSize, de, minMz, maxMz);	
	}

}
