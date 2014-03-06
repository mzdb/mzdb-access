package fr.profi.mzdb.io.reader;

import java.util.Map;

import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.RunSliceHeader;
import fr.profi.mzdb.model.ScanHeader;

/**
 * @author David Bouyssie
 * 
 */
public class MzDbEntityCache {

	protected ScanHeader[] scanHeaders = null;

	protected Map<Integer, ScanHeader> scanHeaderById = null;

	protected Map<Float, Integer> scanIdByTime = null;

	// protected HashMap<Integer, ArrayList<Integer>> _scanIdsByTimeIndex = null;

	protected Map<Integer, DataEncoding> dataEncodingById = null;

	protected Map<Integer, DataEncoding> dataEncodingByScanId = null;

	protected RunSliceHeader[] runSliceHeaders = null;

	protected Map<Integer, RunSliceHeader> runSliceHeaderById = null;

	public ScanHeader[] getScanHeaders() {
		return scanHeaders;
	}

	public Map<Integer, ScanHeader> getScanHeaderById() {
		return scanHeaderById;
	}

	public Map<Float, Integer> getScanIdByTime() {
		return scanIdByTime;
	}

	public Map<Integer, DataEncoding> getDataEncodingByScanId() {
		return dataEncodingByScanId;
	}

	public RunSliceHeader[] getRunSliceHeaders() {
		return runSliceHeaders;
	}

	public Map<Integer, RunSliceHeader> getRunSliceHeaderById() {
		return runSliceHeaderById;
	}

}
