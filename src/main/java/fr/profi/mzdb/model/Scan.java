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
		return this.scanData;
	}

	/**
	 * Gets the peaks.
	 * 
	 * @return the peaks
	 */
	public Peak[] getPeaks() {
		return scanData.toPeaks(this.header);
	}
	
	public Peak getNearestPeak(double mz, double mzTolPPM) {
		return this.scanData.getNearestPeak(mz, mzTolPPM, header);
	}

}
