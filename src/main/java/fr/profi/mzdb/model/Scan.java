/*
 * Package fr.profi.mzdb.model
 * @author David Bouyssie
 */
package fr.profi.mzdb.model;

// TODO: Auto-generated Javadoc
/**
 * The Class Scan.
 * 
 * @author David Bouyssie
 */
public class Scan {

	/** The header. */
	protected final ScanHeader header;

	/** The scan data. */
	protected ScanData scanData;

	/** The peaks. */
	// protected Peak[] peaks;

	/**
	 * Instantiates a new scan.
	 * 
	 * @param header
	 *            the header
	 * @param peaks
	 *            the peaks
	 */
	/*
	 * public Scan(ScanHeader header, Peak[] peaks) { super(); this.header = header; this.peaks = peaks; }
	 */

	/**
	 * Instantiates a new scan.
	 * 
	 * @param header
	 *            the header
	 * @param scanData
	 *            the scan data
	 */
	public Scan(ScanHeader header, ScanData scanData) {
		super();
		this.header = header;
		this.scanData = scanData;
	}

	/**
	 * Gets the header.
	 * 
	 * @return the header
	 */
	public ScanHeader getHeader() {
		return this.header;
	}

	/**
	 * Gets the data.
	 * 
	 * @return the data
	 */
	public ScanData getData() {

		if (this.scanData == null) {
			// TODO: build scan data from peaks
		}

		return this.scanData;
	}

	/**
	 * Gets the peaks.
	 * 
	 * @return the peaks
	 */
	public Peak[] getPeaks() {

		// if( this.peaks == null )
		// this.peaks = scanData.toPeaks(this.header);
		// return this.peaks;
		return scanData.toPeaks(this.header);
	}

}
