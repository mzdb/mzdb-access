package fr.profi.mzdb.io.reader.bb;

import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanSlice;

//import fr.profi.mzdb.io.reader.bb.AbstractBlobReader.BlobData;
//import fr.profi.mzdb.model.DataEncoding;

/**
 * Interface for reading SQLite Blob
 * 
 * @author marco
 * @author David Bouyssie
 * 
 */
public interface IBlobReader {
	
	/**
	 * Cleanup the blob if necessary
	 */
	void disposeBlob();
	
	/**
	 * @return the scans count in the blob.
	 */
	int getScansCount();

	/**
	 * 
	 * @param i index of scan starting at 1
	 * @return long, the ID of the scan at the specified index in the blob
	 */
	long getScanIdAt(int i);
	
	long[] getAllScanIds();

	/**
	 * 
	 * @param i index of the wanted scan
	 * @return int, the number of peaks of the scan specified by the index
	 */
	//int nbPeaksOfScanAt(int i);

	/**
	 * 
	 * @param runSliceId needed to correctly annotate the ScanSlice
	 * @return array of scanSlice representing the bounding box
	 */
	ScanSlice[] readAllScanSlices(int runSliceId);

	/**
	 * 
	 * @param idx
	 *            index of specified scan
	 * @return ScanSlice of the specified scan
	 */
	ScanSlice readScanSliceAt(int idx);
	
	/**
	 * 
	 * @param idx
	 *            index of specified scan
	 * @return ScanData of the specified scan
	 */
	ScanData readScanSliceDataAt(int idx);
	
	/**
	 * 
	 * @param idx
	 *            index of specified scan
	 * @return ScanData of the specified scan
	 */
	ScanData readFilteredScanSliceDataAt(int idx, double minMz, double maxMz);



}
