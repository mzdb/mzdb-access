package fr.profi.mzdb.io.reader.bb;

import java.util.HashMap;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteBlob;
import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.primitives.BytesUtils;

/**
 * Class for manipulating Blob in SQLite datafile using sqlite4java.SQLiteBlob
 * 
 * @author Marc Dubois
 * @see AbstractBlobReader
 * 
 */
public class SQLiteBlobReader extends AbstractBlobReader {

	/** SQLiteBlob Object */
	protected SQLiteBlob _blob;

	/**
	 * Constructor
	 * 
	 * @param dataEnc
	 *            map <ScanId, DataEncoding>
	 * @param r
	 *            the SQliteBlob object
	 * @see SQLiteBlob
	 * @see DataEncoding
	 */
	public SQLiteBlobReader(Map<Integer, ScanHeader> headers, Map<Integer, DataEncoding> dataEnc, SQLiteBlob r) {
		super(headers, dataEnc);
		_blob = r;
		_buildMapPositions();
	}

	/**
	 * @see IBlobReader#disposeBlob()
	 */
	public void disposeBlob() {
		_blob.dispose();
	}

	/**
	 * @see IBlobReader#blobSize()
	 */
	public int blobSize() {
		try {
			return _blob.getSize();
		} catch (SQLiteException e) {
			e.printStackTrace();
			return 0;
		}
	}

	/**
	 * @see IBlobReader#nbScans()
	 */
	public int nbScans() {
		return _nbScans;
	}

	/**
	 * @see AbstractBlobReader
	 * @see AbstractBlobReader#_buildMapPositions()
	 */
	protected void _buildMapPositions() {

		int size = blobSize();
		int count = 1;
		int i = 0;
		_startPositions = new HashMap<Integer, Integer>();
		_nbPeaks = new HashMap<Integer, Integer>();
		while (i < size) {
			_startPositions.put(count, i);
			byte[] buffer_ = new byte[4];
			try {
				_blob.read(i, buffer_, 0, 4);
			} catch (SQLiteException e) {
				e.printStackTrace();
			} // read 4 bytes

			i += 4; // pass rt
			int id = BytesUtils.bytesToInt(buffer_, 0);

			byte[] buffer = new byte[4];

			try {
				_blob.read(i, buffer, 0, 4);
			} catch (SQLiteException e) {
				e.printStackTrace();
			} // read 4 bytes

			int nbPeaks = BytesUtils.bytesToInt(buffer, 0); // nbPeaks
			_nbPeaks.put(count, nbPeaks);
			i += 4; // skip nbPeaks

			DataEncoding de = this._dataEncodings.get(id);
			int structSize = de.getPeakEncoding().getValue();
			if (de.getMode() == DataMode.FITTED)
				structSize += 8; // add 2 float lwhm, rwhm

			i += nbPeaks * structSize; // skip nbPeaks * size of one peak
			count++;
		}
		_nbScans = count - 1; // removing the last count++ better than doing a if
		// statement inside a while loop
	}

	/**
	 * @see IBlobReader#idOfScanAt(int)
	 */
	public int idOfScanAt(int i) {
		if (i > _nbScans || i < 1) {
			throw new IndexOutOfBoundsException("idOfScanAt: Index out of bound exception");
		}

		byte[] b = new byte[4];

		try {
			_blob.read(_startPositions.get(i), b, 0, 4);
		} catch (SQLiteException e) {
			e.printStackTrace();
		}
		return BytesUtils.bytesToInt(b, 0);
	}

	/**
	 * @see IBlobReader#nbPeaksOfScanAt(int)
	 */
	public int nbPeaksOfScanAt(int i) {
		if (i > _nbScans || i < 1) {
			throw new IndexOutOfBoundsException("nbPeaksOfScanAt: Index out of bound, starting counting at 1");
		}
		return _nbPeaks.get(i);
	}

	/**
	 * @see IBlobReader#peakAt(int, int)
	 */
	public Peak peakAt(int idx, int pos) {
		if (idx > _nbScans || idx < 1) {
			throw new IndexOutOfBoundsException("peakAt: Index out of bound start counting at 1");
		}
		int nbPeaks = this.nbPeaksOfScanAt(idx);
		if (pos > nbPeaks) {
			throw new IndexOutOfBoundsException(
					"peakAt: Index out of bound, peak wanted index superior at scan slice length");
		}
		Peak[] peaks = peaksOfScanAt(idx);
		return peaks[pos];
	}

	/**
	 * @see IBlobReader#scanSliceOfScanAt(int)
	 */
	public ScanSlice scanSliceOfScanAt(int idx) {

		if (idx > _nbScans || idx < 1)
			throw new IndexOutOfBoundsException("scanSliceOfScanAt: Index out of bound, start counting at 1");

		int pos = _startPositions.get(idx);
		int nbPeaks = _nbPeaks.get(idx);

		byte[] buffer_ = new byte[4];
		try {
			_blob.read(pos, buffer_, 0, 4);
		} catch (SQLiteException e) {
			e.printStackTrace();
		} // read 4 bytes

		int id = BytesUtils.bytesToInt(buffer_, 0);

		pos += 8;

		DataEncoding de = this._dataEncodings.get(id);
		DataMode dataMode = de.getMode();
		int structSize = de.getPeakEncoding().getValue();
		if (dataMode == DataMode.FITTED)
			structSize += 8; // add 2 float lwhm, rwhm

		int nbBytes = nbPeaks * structSize;

		byte[] peaksBytes = new byte[nbBytes];

		try {
			_blob.read(pos, peaksBytes, 0, nbBytes);
		} catch (SQLiteException e) {
			e.printStackTrace();
		}

		BlobData blobData = readBlob(peaksBytes, peaksBytes.length, structSize, de);
		ScanSlice s = new ScanSlice(null, new ScanData(blobData.mz, blobData.intensity, blobData.lwhm,
				blobData.rwhm));
		return s;
	}

	/**
	 * @see IBlobReader#asScanSlicesArray(int, int)
	 */
	public ScanSlice[] asScanSlicesArray(int firstScanId, int runSliceId) {
		ScanSlice[] sl = new ScanSlice[_nbScans];
		for (int i = 1; i <= _nbScans; i++) {
			ScanSlice s = this.scanSliceOfScanAt(i);
			s.setRunSliceId(runSliceId);
			sl[i - 1] = s;
		}
		return sl;
	}
}
