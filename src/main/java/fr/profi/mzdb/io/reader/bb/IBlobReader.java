/**
 * 
 */
package fr.profi.mzdb.io.reader.bb;

import java.nio.ByteBuffer;

import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.ScanSlice;

//import fr.profi.mzdb.io.reader.bb.AbstractBlobReader.BlobData;
//import fr.profi.mzdb.model.DataEncoding;

/**
 * Interface for reading sqlite Blob
 * 
 * @author marco
 * 
 */
public interface IBlobReader {
    /*
     * Size of structure depending on dataMode selected
     */
    int FITTED = 20;
    int CENTROID = 12;

    /**
     * @return int the number of scan contained in the bounding box
     */
    int nbScans();

    /**
     * 
     * @return int the size of the blob in bytes
     */
    int blobSize();

    /**
     * 
     * @param i
     *            index of scan starting at 1
     * @return int, the ID of the scan at the specified index in the blob
     */
    int idOfScanAt(int i);

    /**
     * 
     * @param i
     *            , index of the wanted scan
     * @return int, the number of peaks of the scan specified by the index
     */
    int nbPeaksOfScanAt(int i);

    /**
     * 
     * @param i
     *            , index of the wanted scan
     * @return array of peak of the scan specified by the index
     */
    Peak[] peaksOfScanAt(int i);

    /**
     * 
     * @param i
     *            , index of the wanted scan
     * @param pos
     *            , position of the peak to extract
     * @return peak of the specified scan at specified position
     */
    Peak peakAt(int i, int pos);

    /**
     * 
     * @param idx
     *            index of specified scan
     * @return ScanSlice of the specified scan
     */
    ScanSlice scanSliceOfScanAt(int idx);

    /**
     * 
     * @param firstScanId
     *            needed in the constructor of the ScanSlice
     * @param runSliceId
     *            needed in the constructor of the ScanSlice
     * @return array of scanSlice respresenting the bounding box
     */
    ScanSlice[] asScanSlicesArray(int firstScanId, int runSliceId);

    /**
     * cleanup the blob if necessary
     */
    void disposeBlob();

    ByteBuffer getByteBuffer();
}
