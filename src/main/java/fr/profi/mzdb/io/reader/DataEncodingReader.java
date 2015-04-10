package fr.profi.mzdb.io.reader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.almworks.sqlite4java.SQLiteException;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.db.model.params.ParamTree;
import fr.profi.mzdb.db.model.params.param.CVParam;
import fr.profi.mzdb.db.table.DataEncodingTable;
import fr.profi.mzdb.db.table.SpectrumTable;
import fr.profi.mzdb.model.ByteOrder;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.DataMode;
import fr.profi.mzdb.model.PeakEncoding;
import fr.profi.mzdb.utils.sqlite.ISQLiteRecordExtraction;
import fr.profi.mzdb.utils.sqlite.SQLiteQuery;
import fr.profi.mzdb.utils.sqlite.SQLiteRecord;
import fr.profi.mzdb.utils.sqlite.SQLiteRecordIterator;

// TODO: Auto-generated Javadoc
/**
 * The Class DataEncodingReader.
 * 
 * @author David Bouyssie
 */
public class DataEncodingReader extends AbstractMzDbReaderHelper {

	/**
	 * Instantiates a new data encoding reader.
	 * 
	 * @param mzDbReader
	 *            the mz db reader
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public DataEncodingReader(MzDbReader mzDbReader) throws SQLiteException {
		super(mzDbReader);
	}

	// Define some variable for scan header extraction
	/** The _data encoding query str. */
	private static String _dataEncodingQueryStr = "SELECT * FROM data_encoding";

	/** The _data encoding extractor. */
	private ISQLiteRecordExtraction<DataEncoding> _dataEncodingExtractor = new ISQLiteRecordExtraction<DataEncoding>() {

		public DataEncoding extract(SQLiteRecord record) throws SQLiteException {

			// Extract record values
			int id = record.columnInt(DataEncodingTable.ID);
			String dmAsStr = record.columnString(DataEncodingTable.MODE);
			String compression = record.columnString(DataEncodingTable.COMPRESSION);
			String byteOrderAsStr = record.columnString(DataEncodingTable.BYTE_ORDER);

			// Parse record values
			DataMode dm;
			if (dmAsStr.equalsIgnoreCase("FITTED"))
				dm = DataMode.FITTED;
			else
				dm = DataMode.CENTROID;

			ByteOrder bo;
			if (byteOrderAsStr.equalsIgnoreCase("big_endian"))
				bo = ByteOrder.BIG_ENDIAN;
			else
				bo = ByteOrder.LITTLE_ENDIAN;
			
			// Parse param tree
			String paramTreeAsStr = record.columnString(SpectrumTable.PARAM_TREE);
			ParamTree paramTree = ParamTreeParser.parseParamTree(paramTreeAsStr);		
			
			// FIXME: the two CV params may have the same AC => it could be conflicting...
			List<CVParam> cvParams = paramTree.getCVParams();
			CVParam mzEncoding = cvParams.get(0);
			CVParam intEncoding = cvParams.get(1);
			
			PeakEncoding peakEnc = null;
			if( mzEncoding.getValue().equals("32") ) {
				peakEnc = PeakEncoding.LOW_RES_PEAK;
			} else {
				if( intEncoding.getValue().equals("32") ) {
					peakEnc = PeakEncoding.HIGH_RES_PEAK;
				} else {
					peakEnc = PeakEncoding.NO_LOSS_PEAK;
				}
			}

			// Return data encoding object
			return new DataEncoding(id, dm, peakEnc, compression, bo);
		}

	};

	/**
	 * Gets the data encoding.
	 * 
	 * @param dataEncodingId
	 *            the data encoding id
	 * @return the data encoding
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public DataEncoding getDataEncoding(int dataEncodingId) throws SQLiteException {

		if (this.entityCache != null) {
			return this.getDataEncodingById().get(dataEncodingId);
		} else {
			// Retrieve data encoding record
			String queryStr = _dataEncodingQueryStr + " WHERE id = ?";
			return new SQLiteQuery(connection, queryStr).bind(1, dataEncodingId).extractRecord(
					this._dataEncodingExtractor);
		}

	}

	/**
	 * Gets the data encodings.
	 * 
	 * @return the data encodings
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public DataEncoding[] getDataEncodings() throws SQLiteException {
		DataEncoding[] dataEncodings = new DataEncoding[this.mzDbReader.getDataEncodingsCount()];
		return new SQLiteQuery(connection, _dataEncodingQueryStr).extractRecords(this._dataEncodingExtractor,
				dataEncodings);
	}

	/**
	 * Gets the data encoding by id.
	 * 
	 * @return the data encoding by id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Map<Integer, DataEncoding> getDataEncodingById() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.dataEncodingById != null) {
			return this.entityCache.dataEncodingById;
		} else {
			DataEncoding[] dataEncodings = this.getDataEncodings();
			HashMap<Integer, DataEncoding> dataEncodingById = new HashMap<Integer, DataEncoding>(
					dataEncodings.length);

			for (DataEncoding dataEncoding : dataEncodings)
				dataEncodingById.put(dataEncoding.getId(), dataEncoding);

			if (this.entityCache != null)
				this.entityCache.dataEncodingById = dataEncodingById;

			return dataEncodingById;
		}
	}

	/**
	 * Gets the data encoding by scan id.
	 * 
	 * @return the data encoding by scan id
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public Map<Integer, DataEncoding> getDataEncodingByScanId() throws SQLiteException {

		if (this.entityCache != null && this.entityCache.dataEncodingByScanId != null) {
			return this.entityCache.dataEncodingByScanId;
		} else {

			Map<Integer, DataEncoding> dataEncodingById = this.getDataEncodingById();

			// Retrieve encoding PK for the given scan id
			String queryStr = "SELECT id, data_encoding_id FROM spectrum";
			SQLiteRecordIterator records = new SQLiteQuery(connection, queryStr).getRecords();

			HashMap<Integer, DataEncoding> dataEncodingByScanId = new HashMap<Integer, DataEncoding>();
			while (records.hasNext()) {
				SQLiteRecord record = records.next();

				int scanId = record.columnInt(SpectrumTable.ID);
				int scanDataEncodingId = record.columnInt(SpectrumTable.DATA_ENCODING_ID);
				
				DataEncoding dataEnc = dataEncodingById.get(scanDataEncodingId);

				/*
				// Looking for the appropriate peak encoding
				// FIXME: retrieve the resolution from the data encoding param tree
				PeakEncoding pe = (h.isHighResolution()) ? PeakEncoding.HIGH_RES_PEAK
						: PeakEncoding.LOW_RES_PEAK;
				if (mzDbReader.isNoLossMode())
					pe = PeakEncoding.NO_LOSS_PEAK;

				// Setting new peak encoding was set to null before
				dataEnc.setPeakEncoding(pe);*/

				dataEncodingByScanId.put(scanId, dataEnc);
			}

			if (this.entityCache != null) {
				this.entityCache.dataEncodingByScanId = dataEncodingByScanId;
			}

			return dataEncodingByScanId;
		}

	}

	/**
	 * Gets the scan data encoding.
	 * 
	 * @param scanId
	 *            the scan id
	 * @return the scan data encoding
	 * @throws SQLiteException
	 *             the sQ lite exception
	 */
	public DataEncoding getScanDataEncoding(int scanId) throws SQLiteException {

		if (this.entityCache != null) {
			return this.getDataEncodingByScanId().get(scanId);
		} else {
			// Retrieve encoding PK for the given scan id
			String queryStr = "SELECT data_encoding_id FROM spectrum WHERE id = " + scanId;
			return this.getDataEncoding(new SQLiteQuery(connection, queryStr).extractSingleInt());
		}

	}

}
