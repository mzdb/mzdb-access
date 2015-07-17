package fr.profi.mzdb.io.reader.iterator;

import java.io.StreamCorruptedException;
import java.util.Map;

import fr.profi.mzdb.MzDbReader;
import fr.profi.mzdb.io.reader.bb.BoundingBoxBuilder;
import fr.profi.mzdb.model.BoundingBox;
import fr.profi.mzdb.model.DataEncoding;
import fr.profi.mzdb.model.ScanHeader;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

public class BoundingBoxIterator extends StatementIterator<BoundingBox> {

	protected final int msLevel;
	protected final Map<Long, ScanHeader> scanHeaderById;
	protected final Map<Long, DataEncoding> dataEncodingByScanId;

	public BoundingBoxIterator(
		MzDbReader mzdb,
		SQLiteStatement stmt,
		int msLevel
	) throws SQLiteException, StreamCorruptedException {
		super(mzdb, stmt);
		
		this.msLevel = msLevel;
		
		if( msLevel == 1 ) this.scanHeaderById = this.mzDbReader.getMs1ScanHeaderById();
		else if( msLevel == 2 ) this.scanHeaderById = this.mzDbReader.getMs2ScanHeaderById();
		else throw new IllegalArgumentException("unsupported MS level: " + msLevel);
			
		this.dataEncodingByScanId = this.mzDbReader.getDataEncodingByScanId();
		
	}

	public BoundingBox extractObject(SQLiteStatement stmt) throws SQLiteException, StreamCorruptedException {

		int bbId = stmt.columnInt(0);
		byte[] bbBytes = stmt.columnBlob(1);
		int runSliceId = stmt.columnInt(2);
		int firstScanId = stmt.columnInt(3);
		int lastScanId = stmt.columnInt(4);

		BoundingBox bb = BoundingBoxBuilder.buildBB(
			bbId,
			bbBytes,
			firstScanId,
			lastScanId,
			this.scanHeaderById,
			this.dataEncodingByScanId
		);		
		bb.setRunSliceId(runSliceId);
		
		return bb;
	}

}
