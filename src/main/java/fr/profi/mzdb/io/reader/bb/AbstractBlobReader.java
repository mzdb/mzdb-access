/**
 * This file is part of the mzDB project
 */
package fr.profi.mzdb.io.reader.bb;

import java.util.Arrays;
import java.util.Map;

import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.PeakEncoding;
import fr.profi.mzdb.model.ScanHeader;
//import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.utils.primitives.BytesUtils;

/**
 * Abstract Class containing commons objects and attributes through the implementations
 * 
 * @author marco
 * @see IBlobReader
 */
public abstract class AbstractBlobReader implements IBlobReader {

	protected Map<Integer, Integer> _startPositions; // startPositions of each
	// scan
	protected Map<Integer, Integer> _nbPeaks; // number of peaks of each scan
	protected int _nbScans; // number of scans
	// protected Map<Integer, ScanHeader> _scanHeaders;
	protected Map<Integer, DataEncoding> _dataEncodings; // DataEncoding (32-64
	// bit, centroid
	// profile) by
	// scanId
	protected Map<Integer, ScanHeader> _scanHeaders;

	// protected DataMode _dataMode;

	/**
	 * constructor
	 * 
	 * @param dataEnc
	 *            Id by DataEncoding
	 * @see DataEncoding
	 */
	public AbstractBlobReader(Map<Integer, ScanHeader> headers, Map<Integer, DataEncoding> dataEnc) {
		this._dataEncodings = dataEnc;
		this._scanHeaders = headers;
	}

	/**
	 * do a first parse of the blob to determine beginning index of each scan
	 */
	protected abstract void _buildMapPositions();

	/**
	 * @see IBlobReader#peaksOfScanAt(int)
	 */
	public Peak[] peaksOfScanAt(int idx) {
		return null;// return scanSliceOfScanAt(idx).toPeaks(_scanHeader.get());
		// //TODO how to pass a lcContext here ?
	}

	/**
	 * Class holding mz, intensity, lwhm, rwhm
	 * 
	 * @author Marc Dubois
	 * 
	 */
	protected class BlobData {

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
	};

	/**
	 * By default use byteUtils but will be overrided in BytesReader
	 * 
	 * @param peaksBytes
	 *            array of bytes containing the ScanSlice of interest
	 * @param length
	 *            of peaksBytes
	 * @param structSize
	 * @param de
	 * @return
	 */
	protected BlobData readBlob(byte[] peaksBytes, int peaksBytesLength, int structSize, DataEncoding de) {
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
	}

}
