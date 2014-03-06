/*
 * Package fr.profi.mzdb.model
 * @author David Bouyssie
 */
package fr.profi.mzdb.model;

// TODO: Auto-generated Javadoc
/**
 * The Class RunSlice.
 * 
 * @author David Bouyssie
 */
public class RunSlice {

	/** The header. */
	protected final RunSliceHeader header;

	/** The data. */
	protected final RunSliceData data;

	/**
	 * Instantiates a new run slice.
	 * 
	 * @param header
	 *            the header
	 * @param scanData
	 *            the scan data
	 */
	public RunSlice(RunSliceHeader header, RunSliceData scanData) {
		super();
		this.header = header;
		this.data = scanData;
	}

	/**
	 * Gets the header.
	 * 
	 * @return the header
	 */
	public RunSliceHeader getHeader() {
		return header;
	}

	/**
	 * Gets the data.
	 * 
	 * @return the data
	 */
	public RunSliceData getData() {
		return data;
	}

}
