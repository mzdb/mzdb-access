/**
 * This file is part of the mzDB project
 */
package fr.profi.mzdb.io.reader.bb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.Peak;
import fr.profi.mzdb.model.ScanData;
import fr.profi.mzdb.model.ScanHeader;
import fr.profi.mzdb.model.ScanSlice;
import fr.profi.mzdb.utils.primitives.BytesUtils;

/**
 * This class aloow to read a SQLite blob using a stream reader. We process data only in one direction in a
 * sequential way The goal is to request only one time the blob
 * 
 * @author marco
 * 
 */
public class StreamReader extends AbstractBlobReader {

	/** Stream to read */
	private InputStream _stream;

	/**
	 * @param dataEnc
	 *            ScanID the key, dataEncoding the value
	 * @param s
	 *            inputStream
	 * @see AbstractBlobReader
	 * @see AbstractBlobReader#_dataEncodings
	 */
	public StreamReader(Map<Integer, ScanHeader> headers, Map<Integer, DataEncoding> dataEnc, InputStream s) {
		super(headers, dataEnc);
		_stream = s;
	}

	/**
	 * @see IBlobReader#disposeBlob()
	 */
	public void disposeBlob() {
		try {
			_stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @see IBlobReader#nbScans()
	 */
	public int nbScans() {
		throw new UnsupportedOperationException("Unsupported Operation");
	}

	/**
	 * @see IBlobReader#blobSize()
	 */
	public int blobSize() {
		int c = 0;
		try {
			while (_stream.read() != 0)
				c++;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return c;
	}

	/**
	 * @see IBlobReader#idOfScanAt(int)
	 */
	public int idOfScanAt(int i) {
		int lastId = 0;
		try {
			for (int j = 1; j <= i; j++) {
				byte[] b = new byte[4];
				_stream.read(b);
				lastId = BytesUtils.bytesToInt(b, 0);

				byte[] bytes = new byte[4];
				_stream.read(bytes);
				int nbPeaks = BytesUtils.bytesToInt(bytes, 0);
				DataEncoding de = this._dataEncodings.get(lastId);
				int structSize = de.getPeakEncoding().getValue();
				if (de.getMode() == DataMode.FITTED)
					structSize += 8;
				_stream.skip(nbPeaks * structSize);
			}
			_stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lastId;
	}

	/**
	 * @see IBlobReader#nbPeaksOfScanAt(int)
	 */
	public int nbPeaksOfScanAt(int i) {
		int lastNbPeaks = 0;
		try {
			for (int j = 1; j <= i; j++) {
				byte[] b = new byte[4];
				_stream.read(b);
				int id = BytesUtils.bytesToInt(b, 0);
				byte[] bytes = new byte[4];
				_stream.read(bytes);
				int nbPeaks = BytesUtils.bytesToInt(bytes, 0);
				lastNbPeaks = nbPeaks;
				DataEncoding de = this._dataEncodings.get(id);
				int structSize = de.getPeakEncoding().getValue();
				if (de.getMode() == DataMode.FITTED)
					structSize += 8;
				_stream.skip(nbPeaks * structSize);
			}
			_stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lastNbPeaks;
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
		byte[] peaksBytes = null;
		int id = 0, structSize = 0, nbPeaks = 0;
		DataEncoding de = null;
		try {
			for (int j = 1; j <= idx; j++) {
				byte[] b = new byte[4];
				_stream.read(b);
				id = BytesUtils.bytesToInt(b, 0);

				byte[] bytes = new byte[4];
				_stream.read(bytes);
				nbPeaks = BytesUtils.bytesToInt(bytes, 0);

				de = this._dataEncodings.get(id);
				structSize = de.getPeakEncoding().getValue();
				if (de.getMode() == DataMode.FITTED)
					structSize += 8;

				byte[] pb = new byte[nbPeaks * structSize];
				_stream.read(pb);
				peaksBytes = pb;
			}
			_stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (peaksBytes == null) {
			return null;
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
		List<ScanSlice> sl = new ArrayList<ScanSlice>();
		int i = 1;
		while (true) {
			ScanSlice s = this.scanSliceOfScanAt(i);
			if (s == null) {
				break;
			}
			s.setRunSliceId(runSliceId);
			sl.add(s);
			i++;
		}
		try {
			_stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sl.toArray(new ScanSlice[sl.size()]);
	}

	protected void _buildMapPositions() {

	}

}
