package fr.profi.mzdb.io.reader.bb;

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
	 * @return int, the ID of the scan at the specified index in the blob
	 */
	int getScanIdAt(int i);

	/**
	 * 
	 * @param i index of the wanted scan
	 * @return int, the number of peaks of the scan specified by the index
	 */
	//int nbPeaksOfScanAt(int i);

	/**
	 * 
	 * @param i index of the wanted scan
	 * @return array of peak of the scan specified by the index
	 */
	//TODO how to pass a lcContext here ?
	//Peak[] peaksOfScanAt(int i);

	/**
	 * 
	 * @param i index of the wanted scan
	 * @param pos position of the peak to extract
	 * @return peak of the specified scan at specified position
	 */
	//TODO how to pass a lcContext here ?
	//Peak peakAt(int i, int pos);

	/**
	 * 
	 * @param idx
	 *            index of specified scan
	 * @return ScanSlice of the specified scan
	 */
	ScanSlice readScanSliceAt(int idx);

	/**
	 * 
	 * @param runSliceId needed to correctly annotate the ScanSlice
	 * @return array of scanSlice representing the bounding box
	 */
	ScanSlice[] readAllScanSlices(int runSliceId);

}
