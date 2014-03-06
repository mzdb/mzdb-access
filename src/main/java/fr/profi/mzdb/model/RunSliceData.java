/*
 * Package fr.profi.mzdb.model
 * @author David Bouyssie
 */
package fr.profi.mzdb.model;

import fr.profi.mzdb.utils.misc.AbstractInMemoryIdGen;

// TODO: Auto-generated Javadoc
/**
 * The Class RunSliceData.
 * 
 * @author David Bouyssie
 */
public class RunSliceData extends AbstractInMemoryIdGen {

	/** The id. */
	protected final int id;

	/** The scan slice list. */
	protected final ScanSlice[] scanSliceList;

	/**
	 * Instantiates a new run slice data.
	 * 
	 * @param runSliceId
	 *            the run slice id
	 * @param scanSliceList
	 *            the scan slice list
	 */
	public RunSliceData(int runSliceId, ScanSlice[] scanSliceList) {
		super();
		this.id = runSliceId;
		this.scanSliceList = scanSliceList;
	}

	/**
	 * Gets the id.
	 * 
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * Gets the scan slice list.
	 * 
	 * @return the scan slice list
	 */
	public ScanSlice[] getScanSliceList() {
		return scanSliceList;
	}

}
