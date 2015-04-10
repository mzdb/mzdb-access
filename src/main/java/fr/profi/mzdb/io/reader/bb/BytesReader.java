package fr.profi.mzdb.io.reader.bb;

import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
		byte[] bytes,
		int firstScanId,
		int lastScanId,
		Map<Integer, ScanHeader> scanHeaderById,
		Map<Integer, DataEncoding> dataEncodingByScanId
	) throws StreamCorruptedException {
		super(firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId);
		
		this._bbByteBuffer = ByteBuffer.wrap(bytes);
		this._bbByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
		this._blobSize = bytes.length;
		
		//logger.debug("BytesReader: blobSize="+ _blobSize);
		
		this._indexScanSlices();
	}

	/**
	 * Do a first parse of the blob to determine beginning index of each scan slice
	 * @throws StreamCorruptedException 
	 * 
	 * @see AbstractBlobReader
	 * @see AbstractBlobReader._buildMpaPositions()
	 */
	public void _indexScanSlices() throws StreamCorruptedException {
		
		int scanSliceIdx = 0;
		int byteIdx = 0;
		
		this._scanSliceStartPositions = new int[_scansCount];
		this._peaksCounts = new int[_scansCount];
		
		while (byteIdx < _blobSize) {
			
			// Retrieve the scan id
			int scanId = _bbByteBuffer.getInt(byteIdx);	
			_scanSliceStartPositions[scanSliceIdx] = byteIdx;

			// Skip the scan id bytes
			byteIdx += 4;

			// Retrieve the number of peaks
			int peaksCount = _bbByteBuffer.getInt(byteIdx); 
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
	public int getScanIdAt(int idx) {
		this.checkScanIndexRange(idx);
		
		return _bbByteBuffer.getInt(_scanSliceStartPositions[idx]);
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
	public ScanSlice readScanSliceAt(int idx) {
		this.checkScanIndexRange(idx);

		int scanSliceStartPos = _scanSliceStartPositions[idx];
		int scanId = _bbByteBuffer.getInt(scanSliceStartPos);
		
		// Determine peak size in bytes
		DataEncoding de = this._dataEncodingByScanId.get(scanId);

		// Determine peaks bytes length
		int peaksBytesSize = _peaksCounts[idx] * de.getPeakStructSize();
		
		// Skip scan id and peaks count (two integers)
		scanSliceStartPos += 8;

		// Instantiate a new ScanData for the corresponding scan slice
		ScanData scanSliceData = this.readScanSliceData(_bbByteBuffer, scanSliceStartPos, peaksBytesSize, de);

		// Instantiate a new ScanData
		return new ScanSlice(
			_scanHeaderById.get(scanId),
			scanSliceData
		);
	}

	/*
	@Override
	public ByteBuffer getByteBuffer() {
		return this._data;
	}*/
}
