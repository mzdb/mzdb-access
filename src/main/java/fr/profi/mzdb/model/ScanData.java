/*
 * Package fr.profi.mzdb.model
 * @author David Bouyssie
 */
package fr.profi.mzdb.model;

import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;

import fr.profi.mzdb.utils.ms.MsUtils;

// TODO: Auto-generated Javadoc
/**
 * The Class ScanData.
 * 
 * @author David Bouyssie
 */
public class ScanData {

    /** The mz list. */
    protected double[] mzList;

    /** The intensity list. */
    protected float[] intensityList;

    /** The left hwhm list. */
    protected float[] leftHwhmList;

    /** The right hwhm list. */
    protected float[] rightHwhmList;

    // private int currLength;

    /**
     * Instantiates a new scan data.
     * 
     * @param mzList
     *            the mz list
     * @param intensityList
     *            the intensity list
     * @param lHwhmList
     *            the l hwhm list
     * @param rHwhmList
     *            the r hwhm list
     */
    public ScanData(double[] mzList, float[] intensityList, float[] lHwhmList, float[] rHwhmList) {
	super();
	this.mzList = mzList;
	this.intensityList = intensityList;
	this.leftHwhmList = lHwhmList;
	this.rightHwhmList = rHwhmList;
    }

    /**
     * Instantiates a new scan data.
     * 
     * @param mzList
     *            the mz list
     * @param intensityList
     *            the intensity list
     */
    public ScanData(double[] mzList, float[] intensityList) {
	this(mzList, intensityList, null, null);
    }

    /**
     * Gets the mz list.
     * 
     * @return the mz list
     */
    public double[] getMzList() {
	return mzList;
    }

    /**
     * Gets the intensity list.
     * 
     * @return the intensity list
     */
    public float[] getIntensityList() {
	return intensityList;
    }

    /**
     * Gets the left hwhm list.
     * 
     * @return the left hwhm list
     */
    public float[] getLeftHwhmList() {
	return leftHwhmList;
    }

    /**
     * Gets the right hwhm list.
     * 
     * @return the right hwhm list
     */
    public float[] getRightHwhmList() {
	return rightHwhmList;
    }

    /**
     * To peaks.
     * 
     * @return the peak[]
     */
    public Peak[] toPeaks(ILcContext lcContext) {
	int peaksCount = mzList.length;
	Peak[] peaks = new Peak[peaksCount];

	for (int i = 0; i < peaksCount; i++) {

	    float leftHwhm = 0, rightHwhm = 0;
	    if (leftHwhmList != null && rightHwhmList != null) {
		leftHwhm = leftHwhmList[i];
		rightHwhm = rightHwhmList[i];
	    }

	    peaks[i] = new Peak(mzList[i], intensityList[i], leftHwhm, rightHwhm, lcContext);
	}
	return peaks;
    }

    /**
     * Adds the scan data.
     * 
     * @param scanData
     *            the scan data
     */
    public void addScanData(ScanData scanData) {
	if (scanData != null) {
	    this.mzList = ArrayUtils.addAll(this.mzList, scanData.mzList);
	    this.intensityList = ArrayUtils.addAll(this.intensityList, scanData.intensityList);
	    if (scanData.leftHwhmList != null && scanData.rightHwhmList != null) {
		this.leftHwhmList = ArrayUtils.addAll(this.leftHwhmList, scanData.leftHwhmList);
		this.rightHwhmList = ArrayUtils.addAll(this.rightHwhmList, scanData.rightHwhmList);
	    }
	}
    }

    /**
     * Gets the max mz.
     * 
     * @return the max mz
     */
    public double getMaxMz() {
	// supposed and i hope it will always be true that mzList is sorted
	// do not do any verification
	if (mzList.length == 0) {
	    return 0;
	}
	return mzList[mzList.length - 1];
    }

    /**
     * Checks if is empty.
     * 
     * @return true, if is empty
     */
    public boolean isEmpty() {
	return mzList.length == 0; // supposing intensityList and others have the
	// same size;
    }

    /**
     * _bin search index to nearest index.
     * 
     * @param binSearchIndex
     *            the bin search index
     * @param length
     *            the length
     * @return the int
     */
    private int _binSearchIndexToNearestIndex(int binSearchIndex) {
	if (binSearchIndex >= 0)
	    return binSearchIndex;
	else {
	    int idx = -binSearchIndex - 1;
	    if (idx == 0)
		return -1;
	    else
		return idx;
	}
    }

    /** assuming mzList is sorted */
    public Peak getNearestPeak(double mz, double mzTolPPM) {

	if (this.mzList.length == 0)
	    return null;

	final double mzDa = MsUtils.ppmToDa(mz, mzTolPPM);
	final int binSearchIndex = Arrays.binarySearch(this.mzList, mz);
	if (binSearchIndex >= 0) {
	    System.out.println("data found");
	}
	int idx = binSearchIndex >= 0 ? binSearchIndex : -binSearchIndex - 1;
	double prevVal = 0.0, nextVal = 0.0;
	int newIdx = 0;

	if (idx == this.mzList.length) {
	    prevVal = this.mzList[this.mzList.length - 1];
	    if (Math.abs(mz - prevVal) > mzDa)
		return null;
	    newIdx = idx - 1;
	} else if (idx == 0) {
	    // System.out.println("idx == zero");
	    nextVal = this.mzList[idx];
	    if (Math.abs(mz - nextVal) > mzDa)
		return null;
	    newIdx = idx;
	    // System.out.println(""+ this.mzList[idx] +", "+ mz);

	} else {
	    nextVal = this.mzList[idx];
	    prevVal = this.mzList[idx - 1];

	    final double diffNextVal = Math.abs(mz - nextVal);
	    final double diffPrevVal = Math.abs(mz - prevVal);

	    if (diffNextVal < diffPrevVal) {
		if (diffNextVal > mzDa)
		    return null;
		newIdx = idx;
	    } else {
		if (diffPrevVal > mzDa)
		    return null;
		newIdx = idx - 1;
	    }
	}
	// System.out.println("" + this.mzList.length + ", " + newIdx + ", " + idx);
	return new Peak(this.mzList[newIdx], this.intensityList[newIdx], this.leftHwhmList[newIdx],
		this.rightHwhmList[newIdx], null);

    }

    /**
     * Mz range filter.
     * 
     * @param mzMin
     *            the mz min
     * @param mzMax
     *            the mz max
     * @return the scan data
     */
    public ScanData mzRangeFilter(double mzMin, double mzMax) {
	if (mzMin > mzMax) {
	    double tmp = mzMax;
	    mzMax = mzMin;
	    mzMin = tmp;
	}
	int nbPoints = this.mzList.length;

	// Retrieve the index of nearest minimum value if it exists
	int minBinSearchIndex = Arrays.binarySearch(this.mzList, mzMin);
	int firstIdx = this._binSearchIndexToNearestIndex(minBinSearchIndex);
	// If out of bounds => return empty scan data
	if (firstIdx == nbPoints)
	    return null;
	// If first index => set the first value index as the array first index
	if (firstIdx == -1)
	    firstIdx = 0;

	// Retrieve the index of nearest maximum value if it exists
	int maxBinSearchIndex = Arrays.binarySearch(this.mzList, firstIdx, nbPoints, mzMax);
	int lastIdx = this._binSearchIndexToNearestIndex(maxBinSearchIndex);
	// If first index => return empty scan data
	if (lastIdx == -1)
	    return null;
	// If out of bounds => set the last value index as the array last index
	if (lastIdx == nbPoints)
	    lastIdx -= 1;

	// Increment the last index to have an exclusive index (needed by
	// copyOfRange)
	int exclusiveLastidx = lastIdx + 1;

	// System.out.println("range: "+firstIdx + " " + lastIdx + " " +
	// mzList.length);

	ScanData filteredScanData = new ScanData(Arrays.copyOfRange(mzList, firstIdx, lastIdx), // exclusiveLastidx),
		Arrays.copyOfRange(intensityList, firstIdx, lastIdx) // exclusiveLastidx)
	);

	if (this.leftHwhmList != null) {
	    filteredScanData.leftHwhmList = Arrays.copyOfRange(this.leftHwhmList, firstIdx, exclusiveLastidx);
	    filteredScanData.rightHwhmList = Arrays.copyOfRange(this.rightHwhmList, firstIdx,
		    exclusiveLastidx);
	}

	return filteredScanData;
    }

    /**
     * Resize data arrays.
     * 
     * @param newLength
     *            the new length
     */
    public void resizeDataArrays(int newLength) {
	this.mzList = Arrays.copyOf(this.mzList, newLength);
	this.intensityList = Arrays.copyOf(this.intensityList, newLength);

	if (this.leftHwhmList != null && this.rightHwhmList != null) {
	    this.leftHwhmList = Arrays.copyOf(this.leftHwhmList, newLength);
	    this.rightHwhmList = Arrays.copyOf(this.rightHwhmList, newLength);
	}
    }

}
