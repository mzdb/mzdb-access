/**
 * This file is part of the mzDB project
 */
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
//import fr.profi.mzdb.utils.primitives.BytesUtils;
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

	protected Map<Integer, ScanHeader> _scanHeaderById;
	protected Map<Integer, DataEncoding> _dataEncodingByScanId; // DataEncoding (32-64 bit, centroid/profile)

	/**
	 * Abstract constructor
	 * 
	 * @param scanHeaderById ScanHeader by scan id
	 * @param dataEncById DataEncoding by scan id
	 * @see DataEncoding
	 */
	protected AbstractBlobReader(
		int firstScanId,
		int lastScanId,
		Map<Integer, ScanHeader> scanHeaderById,
		Map<Integer, DataEncoding> dataEncodingByScanId
	) {
		this._scansCount = (lastScanId - firstScanId) + 1;

		if( this._scansCount < 1 ) {
			throw new IllegalArgumentException("lastScanId must be greater than firstScanId");
		}
		
		this._scanHeaderById = scanHeaderById;
		this._dataEncodingByScanId = dataEncodingByScanId;		
	}
	
	/**
	 * @see IBlobReader#peaksOfScanAt(int)
	 */
	/*public Peak[] peaksOfScanAt(int idx) {
		return null;// return scanSliceOfScanAt(idx).toPeaks(_scanHeader.get());
		// //TODO how to pass a lcContext here ?
	}*/

	/**
	 * Class holding mz, intensity, lwhm, rwhm
	 * 
	 * @author Marc Dubois
	 * 
	 */
	/*protected class BlobData {

		public double[] mz;
		public float[] intensity;
		public float[] lwhm;
		public float[] rwhm;

		public BlobData(double[] mz_, float[] intensity_, float[] lwhm_, float[] rwhm_) {
			mz = mz_;
			intensity = intensity_;
			lwhm = lwhm_;
			rwhm = rwhm_;
		}
	};*/
	
	/**
	 * Read scan slice data by using a ByteBuffer as input
	 * 
	 * @param bbByteBuffer array of bytes containing the ScanSlices of interest
	 * @param scanSliceStartPos, the starting position
	 * @param peaksBytesLength, length of bytes used by peaks
	 * @param structSize, size of the struct for a given peak	 
	 * @param de, the corresponding DataEncoding
	 * @return
	 */
	protected ScanData readScanSliceData(
		ByteBuffer bbByteBuffer,
		int scanSliceStartPos,
		int peaksBytesLength,
		DataEncoding de
	) {
		
		DataMode dataMode = de.getMode();
		PeakEncoding pe = de.getPeakEncoding();
		int structSize = de.getPeakStructSize();
		int nbPeaks = peaksBytesLength / structSize;

		double[] mzArray = new double[nbPeaks];
		float[] intensityArray = new float[nbPeaks];
		float[] lwhmArray = new float[nbPeaks];
		float[] rwhmArray = new float[nbPeaks];

		int count = 0;

		switch (pe) {
		case HIGH_RES_PEAK:
			for (int j = 0; j < peaksBytesLength; j += structSize) {
				int startPos = scanSliceStartPos + j;
				mzArray[count] = bbByteBuffer.getDouble(startPos);
				intensityArray[count] = bbByteBuffer.getFloat(startPos + 8);
				if (de.getMode() == DataMode.FITTED) {
					lwhmArray[count] = bbByteBuffer.getFloat(startPos + 12);
					rwhmArray[count] = bbByteBuffer.getFloat(startPos + 16);
				}
				count++;
			}
			break;
		case LOW_RES_PEAK:
			for (int j = 0; j < peaksBytesLength; j += structSize) {
				int startPos = scanSliceStartPos + j;
				mzArray[count] = (double) bbByteBuffer.getFloat(startPos);
				intensityArray[count] = bbByteBuffer.getFloat(startPos + 4);
				if (dataMode == DataMode.FITTED) {
					lwhmArray[count] = bbByteBuffer.getFloat(startPos + 8);
					rwhmArray[count] = bbByteBuffer.getFloat(startPos + 12);
				}
				count++;
			}
			break;
		case NO_LOSS_PEAK:
			for (int j = 0; j < peaksBytesLength; j += structSize) {
				int startPos = scanSliceStartPos + j;
				mzArray[count] = bbByteBuffer.getDouble(startPos);
				intensityArray[count] = (float) bbByteBuffer.getDouble(startPos + 8);
				if (dataMode == DataMode.FITTED) {
					lwhmArray[count] = bbByteBuffer.getFloat(startPos + 16);
					rwhmArray[count] = bbByteBuffer.getFloat(startPos + 20);
				}
				count++;
			}
			break;
		}
		
		// return the newly formed ScanData
		return new ScanData(mzArray, intensityArray, lwhmArray, rwhmArray);
	}

	/**
	 * By default use byteUtils but will be overridden in BytesReader
	 * 
	 * @param peaksBytes
	 *            array of bytes containing the ScanSlice of interest
	 * @param length
	 *            of peaksBytes
	 * @param structSize
	 * @param de
	 * @return
	 */
	/*protected BlobData readBlob(byte[] peaksBytes, int peaksBytesLength, int structSize, DataEncoding de) {
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
				mz[count] = BytesUtils.bytesToDouble(Arrays.copyOfRange(peaksBytes, j, j + 8), 0);
				intensity[count] = BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j + 8, j + 12), 0);
				if (de.getMode() == DataMode.FITTED) {
					lwhm[count] = BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j + 12, j + 16), 0);
					rwhm[count] = BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j + 16, j + 20), 0);
				}
				count++;
			}
			break;
		case LOW_RES_PEAK:
			for (int j = 0; j < peaksBytesLength; j += structSize) {
				mz[count] = (double) BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j, j + 4), 0);
				intensity[count] = BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j + 4, j + 8), 0);
				if (dataMode == DataMode.FITTED) {
					lwhm[count] = BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j + 8, j + 12), 0);
					rwhm[count] = BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j + 12, j + 16), 0);
				}
				count++;
			}
			break;
		case NO_LOSS_PEAK:
			for (int j = 0; j < peaksBytesLength; j += structSize) {
				mz[count] = BytesUtils.bytesToDouble(Arrays.copyOfRange(peaksBytes, j, j + 8), 0);
				intensity[count] = (float) BytesUtils.bytesToDouble(
						Arrays.copyOfRange(peaksBytes, j + 8, j + 16), 0);
				if (dataMode == DataMode.FITTED) {
					lwhm[count] = BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j + 16, j + 20), 0);
					rwhm[count] = BytesUtils.bytesToFloat(Arrays.copyOfRange(peaksBytes, j + 20, j + 24), 0);
				}
				count++;
			}
			break;
		}
		
		// return the newly formed blobData
		return new BlobData(mz, intensity, lwhm, rwhm);
	}*/

	/*public ByteBuffer getByteBuffer() {
		return null;
	}*/
	
	protected void checkScanIndexRange(int idx) {
		if (idx < 0 || idx >= _scansCount) {
			throw new IndexOutOfBoundsException("scan index out of bounds (idx="+idx+"), index counting starts at 0");
		}
	}
	
	protected void checkDataEncodingIsNotNull(DataEncoding de, int scanId) throws StreamCorruptedException {
		if (de == null) {
			throw new StreamCorruptedException("Scared that the mzdb file is corrupted, scan id is: " + scanId);
			//logger.error("Scared that the mzdb file is corrupted, scan id is: " + scanId);
			//System.exit(0);
		}
	}
	
	/**
	 * @see IBlobReader#asScanSlicesArray(int, int)
	 */
	public ScanSlice[] readAllScanSlices(int runSliceId) {

		ScanSlice[] sl = new ScanSlice[_scansCount];
		for (int i = 0; i < _scansCount; i++) {
			ScanSlice s = this.readScanSliceAt(i);
			s.setRunSliceId(runSliceId);
			sl[i] = s;
		}
		
		return sl;
	}

}
