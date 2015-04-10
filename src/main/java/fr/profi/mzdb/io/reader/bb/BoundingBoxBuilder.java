package fr.profi.mzdb.io.reader.bb;

import java.io.InputStream;
import java.io.StreamCorruptedException;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteBlob;

import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.ScanHeader;

/**
 * The Class BoundingBoxBuilder.
 * <p>
 * Contains static methods to build BoundingBox objects Use a different reader depending of provided data in
 * the constructor
 * </p>
 * 
 * @author David Bouyssie
 */
public class BoundingBoxBuilder {

	public static BoundingBox buildBB(
		int bbId,
		byte[] bytes,
		int firstScanId,
		int lastScanId,
		Map<Integer, ScanHeader> scanHeaderById,
		Map<Integer, DataEncoding> dataEncodingByScanId
	) throws StreamCorruptedException {
		return new BoundingBox(bbId, new BytesReader(bytes, firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId));
	}

	public static BoundingBox buildBB(
		int bbId,
		SQLiteBlob blob,
		int firstScanId,
		int lastScanId,
		Map<Integer, ScanHeader> scanHeaderById,
		Map<Integer, DataEncoding> dataEncodingByScanId
	) throws StreamCorruptedException {
		return new BoundingBox(bbId, new SQLiteBlobReader(blob, firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId) );
	}

	public static BoundingBox buildBB(
		int bbId,
		InputStream stream,
		int firstScanId,
		int lastScanId,
		Map<Integer, ScanHeader> scanHeaderById,
		Map<Integer, DataEncoding> dataEncodingByScanId
	) {
		return new BoundingBox(bbId, new StreamReader(stream, firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId) );
	}

}
