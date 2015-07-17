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

	protected Map<Long, ScanHeader> ms1ScanHeaderById = null;
	
	protected ScanHeader[] ms2ScanHeaders = null;

	protected Map<Long, ScanHeader> ms2ScanHeaderById = null;
	
	protected Map<Long, Float> scanTimeById = null;
	
	protected Map<Integer, ArrayList<Long>> ms1ScanIdsByTimeIndex = null;
	
	protected Map<Integer, ArrayList<Long>> ms2ScanIdsByTimeIndex = null;

	protected Map<Integer, DataEncoding> dataEncodingById = null;

	protected Map<Long, DataEncoding> dataEncodingByScanId = null;

	protected RunSliceHeader[] runSliceHeaders = null;

	protected Map<Integer, RunSliceHeader> runSliceHeaderById = null;

	public ScanHeader[] getMs1ScanHeaders() {
		return ms1ScanHeaders;
	}

	public Map<Long, ScanHeader> getMs1ScanHeaderById() {
		return ms1ScanHeaderById;
	}
	
	public ScanHeader[] getMs2ScanHeaders() {
		return ms2ScanHeaders;
	}

	public Map<Long, ScanHeader> getMs2ScanHeaderById() {
		return ms2ScanHeaderById;
	}

	public Map<Long, Float> getScanTimeById() {
		return scanTimeById;
	}

	public Map<Long, DataEncoding> getDataEncodingByScanId() {
		return dataEncodingByScanId;
	}

	public RunSliceHeader[] getRunSliceHeaders() {
		return runSliceHeaders;
	}

	public Map<Integer, RunSliceHeader> getRunSliceHeaderById() {
		return runSliceHeaderById;
	}

}
