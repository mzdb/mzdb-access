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
		long firstScanId,
		long lastScanId,
		Map<Long, ScanHeader> scanHeaderById,
		Map<Long, DataEncoding> dataEncodingByScanId
	) throws StreamCorruptedException {
		
		BoundingBox bb = new BoundingBox(bbId, new BytesReader(bytes, firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId));
		bb.setFirstScanId(firstScanId);
		bb.setLastScanId(lastScanId);
		
		return bb;
	}

	public static BoundingBox buildBB(
		int bbId,
		SQLiteBlob blob,
		long firstScanId,
		long lastScanId,
		Map<Long, ScanHeader> scanHeaderById,
		Map<Long, DataEncoding> dataEncodingByScanId
	) throws StreamCorruptedException {
		
		BoundingBox bb =  new BoundingBox(bbId, new SQLiteBlobReader(blob, firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId) );
		bb.setFirstScanId(firstScanId);
		bb.setLastScanId(lastScanId);
		
		return bb;
	}

	public static BoundingBox buildBB(
		int bbId,
		InputStream stream,
		long firstScanId,
		long lastScanId,
		Map<Long, ScanHeader> scanHeaderById,
		Map<Long, DataEncoding> dataEncodingByScanId
	) {
		
		BoundingBox bb = new BoundingBox(bbId, new StreamReader(stream, firstScanId, lastScanId, scanHeaderById, dataEncodingByScanId) );
		bb.setFirstScanId(firstScanId);
		bb.setLastScanId(lastScanId);
		
		return bb;
	}

}
