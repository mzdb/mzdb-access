/*
 * Package fr.profi.mzdb.io.reader
 * @author David Bouyssie
 */
package fr.profi.mzdb.io.reader.bb;

import java.io.InputStream;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteBlob;
import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.ScanHeader;

// TODO: Auto-generated Javadoc
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

	/**
	 * Builds the bb.
	 * 
	 * @param id
	 *            the id
	 * @param dm
	 *            the dm
	 * @param bytes
	 *            the bytes
	 * @return the bounding box
	 */
	public static BoundingBox buildBB(int id, Map<Integer, ScanHeader> headers,
			Map<Integer, DataEncoding> dataEnc, byte[] bytes) {
		return new BoundingBox(id, new BytesReader(headers, dataEnc, bytes));
	}

	/**
	 * Builds the bb.
	 * 
	 * @param id
	 *            the id
	 * @param dm
	 *            the dm
	 * @param blob
	 *            the blob
	 * @return the bounding box
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public static BoundingBox buildBB(int id, Map<Integer, ScanHeader> headers,
			Map<Integer, DataEncoding> dataEnc, SQLiteBlob blob) throws SQLiteException {
		return new BoundingBox(id, new SQLiteBlobReader(headers, dataEnc, blob));
	}

	/**
	 * Builds the bb.
	 * 
	 * @param id
	 *            the id
	 * @param dm
	 *            the dm
	 * @param stream
	 *            the stream
	 * @return the bounding box
	 */
	public static BoundingBox buildBB(int id, Map<Integer, ScanHeader> headers,
			Map<Integer, DataEncoding> dataEnc, InputStream stream) {
		return new BoundingBox(id, new StreamReader(headers, dataEnc, stream));
	}

}
