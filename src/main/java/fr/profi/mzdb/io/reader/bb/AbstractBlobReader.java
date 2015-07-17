package fr.profi.mzdb.io.reader.bb;

import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.PeakEncoding;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;

/**
 * Abstract Class containing commons objects and attributes through the implementations
 * 
 * @author marco
 * @author David Bouyssie
 * @see IBlobReader
 */
public abstract class AbstractBlobReader implements IBlobReader {
	
	/*
	 * Size of structure depending on dataMode selected
	 */
	final static int FITTED = 20;
	final static int CENTROID = 12;
	
	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	protected int _scansCount; // number of scan slices in the blob
	protected int[] _scanSliceStartPositions; // list of scan slice starting positions in the blob
	protected int[] _peaksCounts; // number of peaks in each scan slice of the blob

	protected Map<Long, ScanHeader> _scanHeaderById;
	protected Map<Long, DataEncoding> _dataEncodingByScanId; // DataEncoding (32-64 bit, centroid/profile)

	/**
	 * Abstract constructor
	 * 
	 * @param scanHeaderById ScanHeader by scan id
	 * @param dataEncById DataEncoding by scan id
	 * @see DataEncoding
	 */
	protected AbstractBlobReader(
		final long firstScanId,
		final long lastScanId,
		final Map<Long, ScanHeader> scanHeaderById,
		final Map<Long, DataEncoding> dataEncodingByScanId
	) {

		if( firstScanId > lastScanId ) {
			throw new IllegalArgumentException("lastScanId must be greater or the same than firstScanId");
		}
		
		this._scanHeaderById = scanHeaderById;
		this._dataEncodingByScanId = dataEncodingByScanId;
	}
	
	public long[] getAllScanIds() {
		final int scansCount = this.getScansCount();
		final long[] scanIds = new long[scansCount];
		
		for (int i = 0; i < scansCount; i++) {
			scanIds[i] = this.getScanIdAt(i);
		}
		
		return scanIds;
	}
	
	/**
	 * Read scan slice data by using a ByteBuffer as input
	 * 
	 * @param bbByteBuffer array of bytes containing the ScanSlices of interest
	 * @param scanSliceStartPos, the starting position
	 * @param peaksBytesLength, length of bytes used by peaks
	 * @param structSize, size of the struct for a given peak
	 * @param de, the corresponding DataEncoding
	 * @param minMz, the minimum m/z value
	 * @param maxMz, the maximum m/z value
	 * @return
	 */
	protected ScanData readScanSliceData(
		final ByteBuffer bbByteBuffer,
		final int scanSliceStartPos,
		final int peaksBytesLength,
		final DataEncoding de,
		final double minMz,
		final double maxMz
	) {
		
		final DataMode dataMode = de.getMode();
		final PeakEncoding pe = de.getPeakEncoding();
		final int structSize = de.getPeakStructSize();

		int peaksCount = 0;
		int peaksStartIdx = 0;
		
		// If no m/z range is provided
		if( minMz < 0 && maxMz < 0) {
			// Compute the peaks count for the whole scan slice
			peaksCount = peaksBytesLength / structSize;
			// Set peaksStartIdx value to scanSliceStartPos
			peaksStartIdx = scanSliceStartPos;
		// Else determine the peaksStartIdx and peaksCount corresponding to provided m/z filters
		} else {
			
			// Determine the max m/z threshold to use
			double maxMzThreshold = maxMz;
			if( maxMz < 0 ) {
				maxMzThreshold = Double.MAX_VALUE;
			}
			
			for (int i = 0; i < peaksBytesLength; i += structSize) {
				final int peakStartPos = scanSliceStartPos + i;
				
				double mz = 0.0;
				switch (pe) {
				case HIGH_RES_PEAK:
					mz = bbByteBuffer.getDouble(peakStartPos);
					break;
				case LOW_RES_PEAK:
					mz = (double) bbByteBuffer.getFloat(peakStartPos);
					break;
				case NO_LOSS_PEAK:
					mz = bbByteBuffer.getDouble(peakStartPos);
					break;
				}
				
				// Check if we are in the desired m/z range
				if( mz >= minMz && mz <= maxMzThreshold) {
					
					// Increment the number of peaks to read
					peaksCount++;
					
					// Determine the peaksStartIdx
					if( mz >= minMz && peaksStartIdx == 0 ) {
						peaksStartIdx = peakStartPos;
					}
				}
			}
		}
		
		// Set the position of the byte buffer
		bbByteBuffer.position(peaksStartIdx);

		// Create new arrays of primitives
		final double[] mzArray = new double[peaksCount];
		final float[] intensityArray = new float[peaksCount];
		final float[] lwhmArray = new float[peaksCount];
		final float[] rwhmArray = new float[peaksCount];
		
		for (int peakIdx = 0; peakIdx < peaksCount; peakIdx++ ) {
			
			switch (pe) {
			case HIGH_RES_PEAK:
				mzArray[peakIdx] = bbByteBuffer.getDouble();
				intensityArray[peakIdx] = bbByteBuffer.getFloat();
				break;
			case LOW_RES_PEAK:
				mzArray[peakIdx] = (double) bbByteBuffer.getFloat();
				intensityArray[peakIdx] = bbByteBuffer.getFloat();
				break;
			case NO_LOSS_PEAK:
				mzArray[peakIdx] = bbByteBuffer.getDouble();
				intensityArray[peakIdx] = (float) bbByteBuffer.getDouble();
				break;
			}
			
			if (dataMode == DataMode.FITTED) {
				lwhmArray[peakIdx] = bbByteBuffer.getFloat();
				rwhmArray[peakIdx] = bbByteBuffer.getFloat();
			}
			
		}
		
		// return the newly formed ScanData
		return new ScanData(mzArray, intensityArray, lwhmArray, rwhmArray);
	}
	
	protected void checkScanIndexRange(int idx) {
		if (idx < 0 || idx >= this.getScansCount() ) {
			throw new IndexOutOfBoundsException("scan index out of bounds (idx="+idx+"), index counting starts at 0");
		}
	}
	
	protected void checkDataEncodingIsNotNull(final DataEncoding de, final long scanId) throws StreamCorruptedException {
		if (de == null) {
			throw new StreamCorruptedException("Scared that the mzdb file is corrupted, scan id is: " + scanId);
			//logger.error("Scared that the mzdb file is corrupted, scan id is: " + scanId);
			//System.exit(0);
		}
	}
	
	/**
	 * @see IBlobReader#readAllScanSlices(int)
	 */
	public ScanSlice[] readAllScanSlices(final int runSliceId) {
		
		int scansCount = this.getScansCount();
		ScanSlice[] sl = new ScanSlice[scansCount];
		
		for (int i = 0; i < scansCount; i++) {
			ScanSlice s = this.readScanSliceAt(i);
			s.setRunSliceId(runSliceId);
			sl[i] = s;
		}
		
		return sl;
	}
	
	// TODO: temp workaround (remove me when each BB is annotated with the number of spectra it contains)
	/*protected int[] intListToInts(List<Integer> integers, int size) {
		int[] ret = new int[size];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = integers.get(i).intValue();
		}
		return ret;
	}*/

}
