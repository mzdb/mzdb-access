package fr.profi.mzdb.io.reader;

import java.util.ArrayList;
import java.util.Map;

import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.RunSliceHeader;
import fr.profi.mzdb.model.ScanHeader;

/**
 * @author David Bouyssie
 * 
 */
public class MzDbEntityCache {

	protected ScanHeader[] ms1ScanHeaders = null;

	protected Map<Integer, ScanHeader> ms1ScanHeaderById = null;
	
	protected ScanHeader[] ms2ScanHeaders = null;

	protected Map<Integer, ScanHeader> ms2ScanHeaderById = null;
	
	protected Map<Integer, Float> scanTimeById = null;
	
	protected Map<Integer, ArrayList<Integer>> ms1ScanIdsByTimeIndex = null;
	
	protected Map<Integer, ArrayList<Integer>> ms2ScanIdsByTimeIndex = null;

	protected Map<Integer, DataEncoding> dataEncodingById = null;

	protected Map<Integer, DataEncoding> dataEncodingByScanId = null;

	protected RunSliceHeader[] runSliceHeaders = null;

	protected Map<Integer, RunSliceHeader> runSliceHeaderById = null;

	public ScanHeader[] getMs1ScanHeaders() {
		return ms1ScanHeaders;
	}

	public Map<Integer, ScanHeader> getMs1ScanHeaderById() {
		return ms1ScanHeaderById;
	}
	
	public ScanHeader[] getMs2ScanHeaders() {
		return ms2ScanHeaders;
	}

	public Map<Integer, ScanHeader> getMs2ScanHeaderById() {
		return ms2ScanHeaderById;
	}

	public Map<Integer, Float> getScanTimeById() {
		return scanTimeById;
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
