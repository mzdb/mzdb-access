package fr.profi.mzdb.io.reader.bb;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.primitives.BytesUtils;

/**
 * This class aloow to read a SQLite blob using a stream reader. We process data only in one direction in a
 * sequential way The goal is to request only one time the blob
 * 
 * @author marco
 * 
 */
public class StreamReader extends AbstractBlobReader {

	/** Stream to read */
	private InputStream _stream;

	/**
	 * @param dataEnc
	 *            ScanID the key, dataEncoding the value
	 * @param s
	 *            inputStream
	 * @see AbstractBlobReader
	 * @see AbstractBlobReader#_dataEncodingByScanId
	 */
	public StreamReader(
		final InputStream inputStream,
		final long firstScanId,
		final long lastScanId,
		final Map<Long, ScanHeader> scanHeaderById,
		final Map<Long, DataEncoding> dataEncodingByScanId
	) {
		super(firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId);
		
		this._stream = inputStream;
	}

	/**
	 * @see IBlobReader#disposeBlob()
	 */
	public void disposeBlob() {
		try {
			_stream.close();
		} catch (IOException e) {
			logger.error("IOException has been catched while closing stream", e);
		}
	}

	/**
	 * @see IBlobReader#getScansCount()
	 */
	public int getScansCount() {
		// FIXME: this information should be added to the BB to optimize performances
		return -1;
	}

	/**
	 * @see IBlobReader#getBlobSize()
	 */
	//public int getBlobSize() {
	//	throw new UnsupportedOperationException("can't compute the size of a stream");
		
		/*int c = 0;
		try {
			while (_stream.read() != 0)
				c++;
		} catch (IOException e) {
			logger.error("IOException catched while calculating the size of the stream", e);
			e.printStackTrace();
		}
		return c;*/
	//}

	/**
	 * @see IBlobReader#idOfScanAt(int)
	 */
	public long getScanIdAt(final int idx) {
		
		long lastScanId = 0;
		try {
			for (int j = 0; j <= idx; j++) {
				
				final byte[] scanIdBytes = new byte[4];
				_stream.read(scanIdBytes);
				lastScanId = (long) BytesUtils.bytesToInt(scanIdBytes, 0);

				final byte[] peaksCountBytes = new byte[4];
				_stream.read(peaksCountBytes);
				int peaksCount = BytesUtils.bytesToInt(peaksCountBytes, 0);
				
				final DataEncoding de = this._dataEncodingByScanId.get( lastScanId);
				this.checkDataEncodingIsNotNull(de, lastScanId);
				
				_stream.skip(peaksCount * de.getPeakStructSize());
			}
			_stream.close();
		} catch (IOException e) {
			logger.error("IOException has been catched while closing stream", e);
		}
		
		return lastScanId;
	}

	/**
	 * @see IBlobReader#nbPeaksOfScanAt(int)
	 */
	/*public int nbPeaksOfScanAt(int i) {
		int lastNbPeaks = 0;
		try {
			for (int j = 1; j <= i; j++) {
				byte[] b = new byte[4];
				_stream.read(b);
				int id = BytesUtils.bytesToInt(b, 0);
				byte[] bytes = new byte[4];
				_stream.read(bytes);
				int nbPeaks = BytesUtils.bytesToInt(bytes, 0);
				lastNbPeaks = nbPeaks;
				DataEncoding de = this._dataEncodingByScanId.get(id);
				int structSize = de.getPeakEncoding().getValue();
				if (de.getMode() == DataMode.FITTED)
					structSize += 8;
				_stream.skip(nbPeaks * structSize);
			}
			_stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lastNbPeaks;
	}*/
	
	/**
	 * @see IBlobReader#readScanSliceAt(int)
	 */
	public ScanSlice readScanSliceAt(final int idx) {
		return this._readScanSliceAt(idx, -1.0, -1.0);
	}

	/**
	 * @see IBlobReader#readScanSliceAt(int)
	 */
	private ScanSlice _readScanSliceAt(final int idx, final double minMz, final double maxMz) {
		
		byte[] peaksBytes = null;
		long scanId = 0;
		int peaksCount = 0;
		DataEncoding de = null;
		
		try {
			for (int j = 0; j <= idx; j++) {
				
				final byte[] scanIdBytes = new byte[4];
				_stream.read(scanIdBytes);
				scanId = (long) BytesUtils.bytesToInt(scanIdBytes, 0);
				de = this._dataEncodingByScanId.get(scanId);
				
				final byte[] peaksCountBytes = new byte[4];
				_stream.read(peaksCountBytes);
				peaksCount = BytesUtils.bytesToInt(peaksCountBytes, 0);
				
				final int peaksBytesSize = peaksCount * de.getPeakStructSize();
				
				// If not on specified index
				if( j < idx ) {
					// skip the peaks
					_stream.skip(peaksBytesSize);
				} else {
					// read peaks
					final byte[] pb = new byte[peaksBytesSize];
					_stream.read(pb);
					peaksBytes = pb;
				}

			}
			_stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (peaksBytes == null) {
			return null;
		}

		final ScanData scanSliceData = this.readScanSliceData(ByteBuffer.wrap(peaksBytes), 0, peaksBytes.length, de, minMz, maxMz);
		
		return new ScanSlice(_scanHeaderById.get(scanId), scanSliceData);
	}
	
	// TODO: call this method from readScanSliceAt instead of calling readScanSliceAt from this methods
	public ScanData readScanSliceDataAt(final int idx) {
		return readScanSliceAt(idx).getData();
	}
	
	public ScanData readFilteredScanSliceDataAt(final int idx, final double minMz, final double maxMz) {
		return this._readScanSliceAt(idx, minMz, maxMz).getData();
	}

}
