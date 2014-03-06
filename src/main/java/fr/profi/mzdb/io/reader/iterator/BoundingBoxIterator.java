package fr.profi.mzdb.io.reader.iterator;

import java.util.Map;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.reader.bb.BoundingBoxBuilder;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.DataMode;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class BoundingBoxIterator extends StatementIterator<BoundingBox> {

	protected final DataMode dataMode = null;
	protected final Map<Integer, DataEncoding> dataEnc;
	protected final int msLevel;

	public BoundingBoxIterator(MzDbReader mzdb, SQLiteStatement stmt, Map<Integer, DataEncoding> dataEnc,
			int msLevel) throws SQLiteException {
		super(mzdb, stmt);
		this.dataEnc = dataEnc;
		// this.dataMode = dataEnc.getMode();
		this.msLevel = msLevel;
	}

	public BoundingBox extractObject(SQLiteStatement stmt) throws SQLiteException { // process
		// only
		// one
		// boundingbox

		int bbId = stmt.columnInt(0);
		byte[] data = stmt.columnBlob(1);
		int runSliceId = stmt.columnInt(2);
		int firstScanId = stmt.columnInt(3);
		int lastScanId = stmt.columnInt(4);

		BoundingBox bb = BoundingBoxBuilder.buildBB(bbId, this.mzDbReader.getScanHeaderById(), this.dataEnc,
				data);
		bb.setRunSliceId(runSliceId);
		bb.setFirstScanId(firstScanId);
		bb.setLastScanId(lastScanId);
		return bb;
		// ScanSlice[] sl = bb.asScanSlicesArray(scanId, runSliceId);//ScanId,
		// RunSliceId

		// return sl;
	}

}
